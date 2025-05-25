import datetime
import pendulum
import os
import boto3
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import json
from pytz import timezone
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
import logging



# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]


# Setup Global Env Variables
source_system_id = "adt"


# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs" : 1,
    "catchup" : False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=3),
}

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"adt_api_sourcing_{environment}_data_pipeline"
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

datasets = ["adt_compliance_progress"]
cur_table_name = "adt_compliance_progress"


glue_job_names = {
    "adt_api_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-adt-api-sourcing",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "adt_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}


# Create DAG
with DAG(
    dag_id=f"adt_api_sourcing_{environment}_pipeline",
    schedule = "0 19 * * 0",  # Every Sunday at 7:00 PM UTC (12:30 AM IST Monday)
    tags=["adt_sourcing"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 # This ensures only one active DAG run at a time
) as dag:

    
    # Gets current execution date
    execution_date = "{{logical_date}}"
    today = datetime.datetime.now()



    @task_group(group_id="adt_api_sourcing")
    def adt_api_sourcing():
        """Task Group that runs the required steps to source adt"""

        sourcing_task = GlueJobOperator(
            task_id="adt_api_sourcing_job",
            job_name=glue_job_names["adt_api_sourcing_job"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--table_name": cur_table_name,
                "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table"
            },
        )
        
    sourcing_tasks = adt_api_sourcing()

    raw_crawler = GlueCrawlerOperator(
        task_id=f"adt_sourcing_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-health-safety-environment-{source_system_id}-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    # Raw crawler sensor
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_erm_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-health-safety-environment-{source_system_id}-raw",
    )

    # Detect Schema Change Task Group with batching
    @task_group(group_id="detect_schema_change")
    def detect_schema_change():
        previous_batch_group = None
        for i in range(0, len(datasets), 3):  # Batch size of 3
            batch = datasets[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"raw_{source_system_id}_{table.lower()}_schema_change",
                        job_name=glue_job_names["schema_change_detection"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        max_active_tis_per_dagrun=3,
                        script_args={
                            "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_health_safety_environment_{source_system_id}_raw",
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
        for i in range(0, len(datasets), 3):  # Batch size of 3
            batch = datasets[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table}",
                        job_name=glue_job_names["adt_raw_curated"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_system_id": f"{source_system_id}",
                            "--metadata_type": f"curated#adt#{table}#job#iceberg",
                            "--start_date": execution_date,
                            "--end_date": execution_date,
                        },
                    )                

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group



    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )
    
    
    (
      sourcing_tasks >> raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> sns_notification_for_failure
    )
    
