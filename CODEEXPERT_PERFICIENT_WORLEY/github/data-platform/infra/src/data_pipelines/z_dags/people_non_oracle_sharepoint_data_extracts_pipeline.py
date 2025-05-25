import datetime
import boto3
import pytz
from boto3.dynamodb.conditions import Key
from airflow.models.dag import DAG
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.datasync import DataSyncOperator
from airflow.models import Variable
from airflow.decorators import task_group, task
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
source_name = "people"
source_system_id = "people"
function_name = "hcm_extracts"
dag_config = Variable.get("env", deserialize_json=True)
# Datasets for Downstream Supply Chain
people_non_oracle_data_extracts_dataset = Dataset("//people/non_oracle/domain_integrated_model")
environment = dag_config["environment"]

TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# DynamoDB table configuration
metadata_table_name = f"worley-mf-sydney-{environment}-metadata-table"
masking_metadata_table_name = f"worley-mf-rbac-sydney-{environment}-database-permissions-metadata"

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
    "sharepoint_api_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-sharepoint-document-api-sourcing-job",
    "csx_excel_conversion_job": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "curation_job": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
}


table_names = ["demographics_norway", "mobilization_norway", "demobilization_norway", "demographics_jesa_jv", "mobilization_jesa_jv", "demobilization_jesa_jv"]

print(f"{table_names=}")

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"people_non_oracle_sharepoint_sourcing_{environment}_data_pipeline"
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
    dag_id=f"people_non_oracle_sharepoint_sourcing_{environment}_data_pipeline",
    schedule=None,
    tags=["people_link"],
    start_date=datetime.datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 # This ensures only one active DAG run at a time
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task
    def format_execution_date():
        # Get the current date
        today = datetime.datetime.now(pytz.UTC)
        batch_run_start_time_str = today.strftime(TIMESTAMP_FORMAT)
        return batch_run_start_time_str

    batch_run_start_time_str = format_execution_date()


    norway_sharepoint_sourcing_task = GlueJobOperator(
        task_id=f"{source_name}_norway_sharepoint_sourcing_task",
        job_name=glue_job_names["sharepoint_api_sourcing_job"],
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--batch_run_start_time": batch_run_start_time_str,
            "--source_name": "SharePointPeopleNorway",
            "--metadata_table_name": metadata_table_name
        },
    )

    jesa_jv_sharepoint_sourcing_task = GlueJobOperator(
        task_id=f"{source_name}_jesa_jv_sharepoint_sourcing_task",
        job_name=glue_job_names["sharepoint_api_sourcing_job"],
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--batch_run_start_time": batch_run_start_time_str,
            "--source_name": "SharePointPeopleJesaJV",
            "--metadata_table_name": metadata_table_name
        },
    )


    csv_xlsx_conversion_sourcing_task = GlueJobOperator(
        task_id=f"{source_name}_csv_xlsx_conversion_sourcing_task",
        job_name=glue_job_names["csx_excel_conversion_job"],
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--source_name": "non_oracle",
            "--metadata_type": f"csv_xlsx#{source_system_id}#non_oracle",
            "--function_name": f"csv_xlsx#{source_system_id}",
            "--connector_file_path" : f"people/non_oracle/data_extracts/raw/{batch_run_start_time_str}_UTC/",
            "--start_date": execution_date,
            "--end_date": execution_date,
            "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
        },
    )

    raw_crawler = GlueCrawlerOperator(
        task_id=f"people_non_oracle_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-{source_system_id}-nonoracle-data-extracts-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    # Raw crawler sensor
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_people_non_oracle_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-{source_system_id}-nonoracle-data-extracts-raw",
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
                    print(f"Running Detect Schema for table - {table}")
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table.lower()}_schema_change",
                        job_name=glue_job_names["schema_change_detection"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        max_active_tis_per_dagrun=3,
                        script_args={
                            "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_{source_system_id}_nonoracle_data_extracts_raw",
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
                    print(f"Running Curation for table - {table}")
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table.lower()}_curation",
                        job_name=glue_job_names["curation_job"],
                        region_name=region,
                        max_active_tis_per_dagrun=3,
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_system_id": f"{source_system_id}_curated",
                            "--metadata_type": f"curated#people#{table}#job#iceberg",
                            "--start_date": execution_date,
                            "--end_date": execution_date,
                        },
                    )                

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group

    run_supply_chain_models_dag = BashOperator(
        task_id='create_people_non_oracle_data_extracts_dataset',
        bash_command='echo "run people non oracle data extracts models dag"',
        outlets=[people_non_oracle_data_extracts_dataset]
    )

    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )


    ## Set up the pipeline execution order
    (
        [norway_sharepoint_sourcing_task, jesa_jv_sharepoint_sourcing_task]  >> csv_xlsx_conversion_sourcing_task >> raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> run_supply_chain_models_dag >> sns_notification_for_failure
    )