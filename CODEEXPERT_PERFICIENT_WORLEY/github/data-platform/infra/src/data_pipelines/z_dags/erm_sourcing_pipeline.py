import datetime
import boto3
from boto3.dynamodb.conditions import Key
from airflow.models.dag import DAG
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
import logging


## Setup Global Env Variables
region = "ap-southeast-2"
source_name = "erm"
source_system_id = "erm"
dag_config = Variable.get("env", deserialize_json=True)
# Datasets for Downstream Supply CHain
supply_chain_dataset = Dataset("//erm/supply_chain/domain_integrated_model")
environment = dag_config["environment"]

# DynamoDB table configuration
metadata_table_name = f"worley-mf-sydney-{environment}-metadata-table"
metadata_type = "erm#tables#list"

# Function to get metadata from DynamoDB
def get_metadata_from_ddb(source_system_id: str, metadata_type: str, region: str, metadata_table_name: str) -> dict:
    dynamo_resource = boto3.resource("dynamodb", region_name=region)
    table = dynamo_resource.Table(metadata_table_name)

    try:
        # Query DynamoDB table to retrieve metadata based on SourceSystemId and MetadataType
        response = table.query(
            KeyConditionExpression=Key("SourceSystemId").eq(source_system_id) & Key("MetadataType").eq(metadata_type)
        )
        items = response["Items"]
        if items:
            return items[0]  # Return the first item in the response
        else:
            return {}  # Return an empty dictionary if no items found
    except Exception as e:
        print(f"Error fetching metadata from DynamoDB: {e}")
        return {}

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs" : 1,
    "catchup" : False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-erm-api-sourcing",
    "erm_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
}

# Fetch metadata from DynamoDB for the given environment
metadata = get_metadata_from_ddb(
    source_system_id=source_system_id,
    metadata_type=metadata_type,
    region=region,
    metadata_table_name=metadata_table_name
)

# Extract table names, query IDs from the 'datasets' list in DynamoDB response
datasets = metadata.get("datasets", [])
table_names = [dataset["publish_id"] for dataset in datasets]
query_Ids = [dataset["query_id"] for dataset in datasets]
table_lower = [table_name.lower() for table_name in table_names]

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"erm_sourcing_{environment}_data_pipeline"
          })

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)


def invoke_lambda_function(lambda_function_name,payload):
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=payload            
        )
        print('Response--->', response)
        logging.error("SNS notification got triggered as one of the task failed.Hence mark main dag as failied")
        raise AirflowFailException("A task has failed, marking the DAG as failed.")
    except Exception as e:
        raise

# Create DAG
with DAG(
    dag_id=f"erm_sourcing_{environment}_data_pipeline",
    schedule="0 0 * * 1-5",
    tags=["erm"],
    start_date=datetime.datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 # This ensures only one active DAG run at a time
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task_group(group_id="erm_initial_sourcing")
    def erm_initial_sourcing():
        previous_batch_group = None
        for i in range(0, len(table_names), 3):  # Batch size of 3
            batch = table_names[i:i+3]
            queries = query_Ids[i:i+3]
            batch_group_id = f"batch_{i//3}"
            
            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table_name, queryID in zip(batch, queries):
                    task = GlueJobOperator(
                        task_id=f"{source_name}_initial_sourcing_{table_name.lower()}",
                        job_name=glue_job_names["api_sourcing_job"],
                        region_name=region,
                        max_active_tis_per_dagrun=3,  # Ensuring only 3 tasks run concurrently
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_name": source_name,
                            "--table_name": table_name,
                            "--queryID": queryID
                        },
                    )
            
            # Set up dependency to ensure sequential execution
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group

    raw_crawler = GlueCrawlerOperator(
        task_id=f"erm_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-supply-chain-{source_system_id}-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    # Raw crawler sensor
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_erm_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-supply-chain-{source_system_id}-raw",
    )

    # Detect Schema Change Task Group with batching
    @task_group(group_id="detect_schema_change")
    def detect_schema_change():
        previous_batch_group = None
        for i in range(0, len(table_names), 3):  # Batch size of 3
            batch = table_names[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table.lower()}_schema_change",
                        job_name=glue_job_names["schema_change_detection"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        max_active_tis_per_dagrun=3,
                        script_args={
                            "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_supply_chain_{source_system_id}_raw",
                            "--table_name": f"raw_{table.lower()}"
                        },
                    )

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group

    # Raw to Curated Task Group with batching
    @task_group(group_id="curation")
    def raw_to_curated():
        previous_batch_group = None
        for i in range(0, len(table_names), 3):  # Batch size of 3
            batch = table_names[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table.lower()}_curation",
                        job_name=glue_job_names["erm_raw_curated"],
                        region_name=region,
                        max_active_tis_per_dagrun=3,
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_system_id": f"{source_system_id}_curated",
                            "--metadata_type": f"curated#erm#{table}#job#iceberg",
                            "--start_date": execution_date,
                            "--end_date": execution_date,
                        },
                    )                

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group

    run_supply_chain_models_dag = BashOperator(
        task_id='run_supply_chain_models_dag',
        bash_command='echo "run supply_chain models dag"',
        outlets=[supply_chain_dataset]
    )

    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )
    erm_initial_sourcing_group = erm_initial_sourcing()

    ## Set up the pipeline execution order
    (
        erm_initial_sourcing_group >> raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> run_supply_chain_models_dag >> sns_notification_for_failure
    )