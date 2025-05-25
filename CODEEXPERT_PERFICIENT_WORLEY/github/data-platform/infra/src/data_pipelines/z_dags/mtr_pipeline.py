import datetime
import pendulum
import os

import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable


# Setup Global Env Variables
dag_mtr_config = Variable.get("mtr_env", deserialize_json=True)
dag_config = Variable.get("env", deserialize_json=True)
mtr_environment = dag_mtr_config["environment"]
environment = dag_config["environment"]
region = "ap-southeast-2"
source_system_id = "mtr"
table_names = ["W_MTR_AllTags"]
table_names_lower = [table_name.lower() for table_name in table_names]

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "db_sourcing_job": "worley-datalake-sydney-dev-glue-job-db-sourcing",
    "mtr_raw_curated": "worley-datalake-sydney-dev-glue-job-raw-to-curated-generic",
    "schema_change_detection": "worley-datalake-sydney-dev-glue-job-schema-change-detection-generic"
}

# Create DAG
with DAG(
    dag_id=f"mtr_{environment}_data_pipeline",
    schedule="0 0 * * *",
    tags=["mtr"],
    start_date=datetime.datetime(2024, 5, 1),
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
                "--environment": mtr_environment,
                "--table_name": table,
                "--start_date": execution_date,
                "--end_date": execution_date
                },
            )


    raw_crawler = GlueCrawlerOperator(
        task_id=f"mtr_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-mtr",
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
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_{source_system_id}",
                    "--table_name": f"raw_{table}"
                },
            )


    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in table_names:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["mtr_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"database_{source_system_id}_curated",
                    "--metadata_type": f"curated#mtr#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    (
        source_to_raw() >> raw_crawler >> detect_schema_change() >> raw_to_curated()
    )
