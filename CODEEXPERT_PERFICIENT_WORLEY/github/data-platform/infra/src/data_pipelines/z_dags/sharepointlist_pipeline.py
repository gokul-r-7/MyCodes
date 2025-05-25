import datetime
#import pendulum
import os

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
source_system_id = "SharePointList_curated"
region = "ap-southeast-2"

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

datasets=[
    "Assumptions_Index",
    "Holds_Index",
    "Seal_Decisions",
    "TD_Index"
]

# Create DAG
with DAG(
    dag_id=f"SharePointList_{environment}_data_pipeline", 
    schedule="0 0 * * *",
    tags=["SharePointList"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    aconex_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    SharePointList_source_extraction = GlueJobOperator(
        task_id="SharePointList_source_extraction",
        job_name="worley-datalake-sydney-dev-glue-job-SharePointList-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
        },
    )
   
    
    raw_crawler = GlueCrawlerOperator(
        task_id=f"SharePointList_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-SharePointList"
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"{table}",
                job_name="worley-datalake-sydney-dev-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": source_system_id,
                    "--metadata_type": f"curated#SharePointList#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    SharePointList_source_extraction >> raw_crawler >> raw_to_curated()
