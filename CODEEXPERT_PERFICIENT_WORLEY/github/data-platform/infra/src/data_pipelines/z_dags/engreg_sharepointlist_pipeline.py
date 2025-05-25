import datetime
import os

import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.models import Variable


#Setup Global Env Variables
dag_config = Variable.get("env", deserialize_json=True)

domain_name = "engineering"
environment = dag_config["environment"]
source_system_id = "engreg_curated"
region = "ap-southeast-2"
max_project_concurrency = 1

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

datasets=[
    "assumptions_index",
    "holds_index",
    "seal_decisions",
    "td_index"
]

# Create DAG
with DAG(
    dag_id=f"engreg_sharepointlist_{environment}_engineering_data_pipeline", 
    schedule="45 9 * * 1-5",
    tags=["engreg_sharepointlist"],
    start_date=datetime.datetime(2024, 6, 27),
    default_args=default_args,
    catchup=False,
    max_active_runs = max_project_concurrency,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #Sharepoint SpreadAPIs have specific dataformat
    engreg_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    sharepointlist_source_extraction = GlueJobOperator(
        task_id="engreg_sharepointlist_source_extraction",
        job_name=f"worley-datalake-sydney-{environment}-glue-job-sharepointlist-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
        "--source_name": "sharepointlist_engreg",
        "--metadata_type": "api#sharepointlist_engreg#extract_api",
        },
    )
   
    
    raw_crawler = GlueCrawlerOperator(
        task_id=f"engreg_sharepointlist_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-engineering-engreg-raw"
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_engreg_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-engineering-engreg-raw"
    )    

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"{table}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": source_system_id,
                    "--metadata_type": f"curated#sharepointlist#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    sharepointlist_source_extraction >> raw_crawler >> raw_crawler_sensor >> raw_to_curated()
