import datetime
import json
import pendulum
import os
import boto3
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from botocore.config import Config

#Setup Global Env Variables.
dag_spi_config = Variable.get("spi_env", deserialize_json=True)
dag_config = Variable.get("env", deserialize_json=True)
spi_environment = dag_spi_config["environment"]
environment = dag_config["environment"]
region = "ap-southeast-2"
source_system_id = "spi"
domain_name = "engineering"

table_names = ["COMPONENT", "LINE", "DRAWING", "COMPONENT_FUNCTION_TYPE", "UDF_COMPONENT", "COMPONENT_SYS_IO_TYPE", "TAG_CATEGORY", "COMPONENT_HANDLE", "PLANT", "COMPONENT_MFR", "COMPONENT_LOCATION","LOOP"]
table_names_lower = [table_name.lower() for table_name in table_names]

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs" : 1,
    "catchup" : False,    
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "db_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-db-sourcing",
    "spi_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
}

payload = json.dumps({
          "dag_name": f"spi_{environment}_data_pipeline"
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
    except Exception as e:
        raise

# Create DAG
with DAG(
    dag_id=f"spi_{environment}_data_pipeline",
    schedule="15 9 * * 1-5",
    tags=["spi"],
    start_date=datetime.datetime(2024, 5, 1),
    default_args=default_args,
    catchup=False
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task_group(group_id=f"raw")
    def source_to_raw():
        for table in table_names:
            source_raw = GlueJobOperator(
                task_id=f"raw_{source_system_id}_{table}",
                job_name=glue_job_names["db_sourcing_job"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                "--source_name": source_system_id,
                "--domain_name": domain_name,
                "--environment": spi_environment,
                "--table_name": table,
                "--start_date": execution_date,
                "--end_date": execution_date
                },
            )


    raw_crawler = GlueCrawlerOperator(
        task_id=f"spi_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-engineering-spi-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in table_names_lower:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_engineering_{source_system_id}_raw",
                    "--table_name": f"raw_{table}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in table_names:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["spi_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"database_{source_system_id}_curated",
                    "--metadata_type": f"curated#spi#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )

    (
        source_to_raw() >> raw_crawler >> detect_schema_change() >> raw_to_curated() >> sns_notification_for_failure
    )
