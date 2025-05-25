import datetime
import pendulum
import os

import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator


# Setup Global Env Variables
environment = "dev"
region = "ap-southeast-2"
source_system_id = "ecosys"
secorg_ids = ["HOUS-B2d90"]
datasets = ["project_list", "TimePhased","dcsMapping","gateData"]
datasets_lower = [table_name.lower() for table_name in datasets]
initial_sourcing = ["project_list"]
entity_sourcing = ["TimePhased","dcsMapping","gateData"]
glue_job_names = {
    "api_sourcing_job": "worley-datalake-sydney-dev-glue-job-ecosys-workflow-api-sourcing",
    "ecosys_raw_curated": "worley-datalake-sydney-dev-glue-job-raw-to-curated-generic",
    "schema_change_detection": "worley-datalake-sydney-dev-glue-job-schema-change-detection-generic"
 
}
default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

# Create DAG
with DAG(
    dag_id=f"ecosys_{environment}_data_pipeline",
    schedule="@once",
    tags=["ecosys"],
    start_date=datetime.datetime(2024, 5, 1),
    default_args=default_args,
    catchup=False
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task_group(group_id="ecosys_project_sourcing")
    def ecosys_intial_sourcing(metadata: str,source_name: str = "ecosys" ):
        for secorg_id in secorg_ids:
            src_raw_initial = GlueJobOperator(
              task_id=f"{source_name}_initial_sourcing",
              job_name=glue_job_names["api_sourcing_job"],
              region_name=region,
              verbose=True,
              wait_for_completion=True,
              script_args={
                   "--source_name": source_name,
                   "--function_name": metadata,
                   "--start_date": execution_date,
                   "--end_date": execution_date,
                   "--secorg_id" : secorg_id
            },
        )

    @task_group(group_id="ecosys_sourcing_entities")
    def ecosys_entity_sourcing(metadata: str,source_name: str = "ecosys" ):
        for secorg_id in secorg_ids:
                src_raw_entity = GlueJobOperator(
                task_id=f"{source_name}_entity_sourcing",
                job_name=glue_job_names["api_sourcing_job"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_name,
                    "--function_name": metadata,
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--secorg_id" : secorg_id

            },
        )


    raw_crawler = GlueCrawlerOperator(
        task_id=f"ecosys_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-ecosys",
        },
        poll_interval=5,
        wait_for_completion=False,
    )


    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in datasets_lower:
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

    (
        ecosys_intial_sourcing.expand(metadata=initial_sourcing)
        >> ecosys_entity_sourcing.expand(metadata=entity_sourcing)
        >> raw_crawler
        >> detect_schema_change()
        >> raw_to_curated()
    )
