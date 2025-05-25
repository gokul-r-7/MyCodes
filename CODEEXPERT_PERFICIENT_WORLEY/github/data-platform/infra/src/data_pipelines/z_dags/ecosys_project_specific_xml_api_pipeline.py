import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
import requests
import pytz

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
import logging

# Retrieve variables from airflow env variables
dag_config = Variable.get("env", deserialize_json=True)
dag_s3_config = Variable.get("s3_bucket_name", deserialize_json=True)

# Setup Global Environment Variables
environment = dag_config["environment"]
region = "ap-southeast-2"
source_system_id = "ecosys"


metadata_tablename = f"worley-mf-sydney-{environment}-metadata-table"
max_project_concurrency = 1

# Datasets for Downstream project_control
project_control_dataset = Dataset("//ecosys/project_control/domain_integrated_model")

def get_metadata_from_ddb(source_system_id: str, metadata_type: str, region: str, metadata_table_name: str) -> dict:
    dynamo_resource = boto3.resource("dynamodb", region_name=region)
    table = dynamo_resource.Table(metadata_table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key("SourceSystemId").eq(
                source_system_id) & Key("MetadataType").eq(metadata_type)
        )
        items = response["Items"]
        if items:
            return items[0]  # Assuming you want the first item in the response
        else:
            return {}  # Return an empty dictionary if no items found
    except Exception as e:
        print(f"Error fetching metadata from DynamoDB: {e}")
        return {}

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api_xml#" + source_system_id + "#" + "project_api"
metadata = get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=input_keys, region="ap-southeast-2", metadata_table_name=metadata_tablename
)
region = metadata[environment]['region']
job_data = [metadata[environment]['Project_VG']['job_data']['project']]
secorg_ids = [i['secorg'] for i in job_data]
secorg_proj_comb = [
    {"secorg_id": d["secorg"], "project_id": project_id}
    for d in job_data for project_id in d["id"]
]

# secorg_ids = ["HOUS-B2d90","EUCA-P1b0201","EUCA-P1b0301"]
# datasets = ["project_list", "TimePhased","dcsMapping","gateData", "controlAccount", "controlAccount_Snapshot"]
datasets = ["dcsMapping","gateData"]
datasets_lower = [table_name.lower() for table_name in datasets]

# entity_sourcing = ["TimePhased","dcsMapping","gateData"]
entity_sourcing = ["dcsMapping","gateData"]
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-ecosys-workflow-api-xml-sourcing",
    "ecosys_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
 
}
default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "catchup": False,
    "max_active_runs":1
}

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"ecosys_project_specific_xml_{environment}_data_pipeline"
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
    dag_id=f"ecosys_project_specific_xml_{environment}_data_pipeline",
    schedule="0 5 * * *",
    tags=["ecosys"],
    start_date=datetime.datetime(2025, 4, 23),
    default_args=default_args,
    catchup=False,
    max_active_runs = 1
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"
    source_name="ecosys"
    
    @task_group(group_id="ecosys_project_specific_sourcing_entities")
    def ecosys_entity_sourcing(secorg_id: str, project_id: str):
        for entity in entity_sourcing:
            src_raw_entity = GlueJobOperator(
            task_id=f"{source_name}_{entity}_sourcing",
            job_name=glue_job_names["api_sourcing_job"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_name,
                "--function_name": entity,
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--secorg_id" : secorg_id,
                "--project_id" : project_id
            },
        )


    raw_crawler = GlueCrawlerOperator(
        task_id=f"ecosys_project_specific_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-project-control-ecosys-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_ecosys_project_specific_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-project-control-ecosys-raw"
    )
    
    @task_group(group_id=f"project_specific_schema_detection")
    def detect_schema_change():
        for table in datasets_lower:
            schema_change = GlueJobOperator(
                task_id=f"schema_detect_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_project_control_ecosys_raw",
                    "--table_name": f"raw_{table}"
                },
            )


    @task_group(group_id=f"project_specific_curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["ecosys_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#ecosys#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    
    run_project_control_models_dag = BashOperator(
        task_id='run_project_control_models_dag',
        bash_command='echo "run project_control models dag"',
        outlets=[project_control_dataset]
    )
    
    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )
    
    (
        ecosys_entity_sourcing.expand_kwargs(secorg_proj_comb)
        >> raw_crawler >> raw_crawler_sensor 
        >> detect_schema_change()
        >> raw_to_curated()
        >> run_project_control_models_dag  >> sns_notification_for_failure
    )
