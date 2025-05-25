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


# Setup Global Env Variables
dag_config = Variable.get("env", deserialize_json=True)


environment = dag_config["environment"]
source_system_id = "e3d"

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"

datasets = ["pipecompwbsrep", "vg_e3d_vglpipes_sheet"]

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "e3d_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "e3d_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

# Create DAG
with DAG(
    dag_id=f"e3d_csv_xlsx_{environment}_data_pipeline",
    schedule="@once",
    tags=["e3d_csv_xlsx"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    p6_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    @task_group(group_id="e3d_csv_xlsx_sourcing")
    def e3d_csv_xlsx_sourcing():
        """Task Group that runs the required steps to source E3D"""

        csv_xlsx_task = GlueJobOperator(
            task_id="e3d_csv_xlsx_parsing",
            job_name=glue_job_names["e3d_csv_xlsx_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"csv_xlsx#vg#{source_system_id}",
                "--function_name": "csv_xlsx#vg",
                "--start_date": execution_date,
                "--end_date": execution_date,
            },
        )

        csv_xlsx_task
        
    sourcing_tasks = e3d_csv_xlsx_sourcing()

    raw_crawler = GlueCrawlerOperator(
        task_id=f"e3d_csv_xlsx_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-vg-e3d",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in datasets:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_vg_e3d",
                    "--table_name": f"raw_{table}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["e3d_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#vg_E3D#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )        


    sourcing_tasks >> raw_crawler >> detect_schema_change() >> raw_to_curated()
