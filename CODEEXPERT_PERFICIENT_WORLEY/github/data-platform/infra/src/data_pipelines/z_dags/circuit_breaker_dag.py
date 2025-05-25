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
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

# Setup Global Env Variables
dag_config = Variable.get("env", deserialize_json=True)


# Datasets for Downstream Circuit Breaker
circuit_breaker_dataset = Dataset("//circuit_breaker/domain_integrated_model")

environment = dag_config["environment"]
source_system_id = "circuit_breaker"

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"


datasets = ["consumption_flexera", "country_region_mapping","entitlement_global_software_register","entitlement_worley_parsons","finance_budget","openit_monthly_summary","openit_monthly_summary_by_location","entitlement_total_headcount","entitlement_active_contract_masters","entitlement_worley_functionalgroup_to_functionalmapping"]
schema_datasets = ["consumption_flexera", "consumption_region_country_mapping","global_software_register","entitlement_worley_parsons","finance_budget","consumption_openit_monthly_summary","consumption_openit_monthly_summary_by_location","entitlement_total_headcount","entitlement_active_contract_masters","entitlement_worley_functionalgroup_to_functionalmapping"]

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "circuit_breaker_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-circuit-breaker-api-sourcing",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "circuit_breaker_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "circuit_breaker_sharepoint_sourcing": f"worley-datalake-sydney-{environment}-glue-job-circuit-breaker-sharepoint-api-sourcing" 
}

# Create DAG
with DAG(
    dag_id=f"circuit_breaker_csv_xlsx_{environment}_data_pipeline",
    schedule="@daily",
    tags=["circuit_breaker"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    circuit_breaker_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    @task_group(group_id="circuit_breaker_csv_xlsx_sourcing")
    def circuit_breaker_csv_xlsx_sourcing():
        """Task Group that runs the required steps to source circuit_breaker"""

        csv_xlsx_task = GlueJobOperator(
            task_id="circuit_breaker_csv_xlsx_parsing",
            job_name=glue_job_names["circuit_breaker_csv_xlsx_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--function_name": f"csv_xlsx#{source_system_id}",
                "--start_date": execution_date,
                "--end_date": execution_date,
            },
        )

        csv_xlsx_task
        
    sourcing_tasks = circuit_breaker_csv_xlsx_sourcing()
    
    @task_group(group_id="circuit_breaker_sharepoint_sourcing")
    def circuit_breaker_sharepoint_sourcing():
        """Task Group that runs the required steps to source circuit_breaker"""

        sharepoint_pull = GlueJobOperator(
            task_id="circuit_breaker_sharepoint_sourcing",
            job_name=glue_job_names["circuit_breaker_sharepoint_sourcing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--source_system_id": source_system_id,
                "--function_name": "circuit",
                "--start_date": execution_date,
                "--end_date": execution_date,
            },
        )

        sharepoint_pull
        
    sharepoint_sourcing_tasks = circuit_breaker_sharepoint_sourcing()

    raw_crawler = GlueCrawlerOperator(
        task_id=f"{source_system_id}_csv_xlsx_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-circuit-breaker-hexagon-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in schema_datasets:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_circuit_breaker_hexagon_raw",
                    "--table_name": f"raw_{table}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["circuit_breaker_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#{source_system_id}#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            ) 
               
    run_circuit_breaker_models_dag = BashOperator(
        task_id='run_circuit_breaker_models_dag',
        bash_command=(
            'echo "run circuit breaker models dag"'
        ),
        outlets=[circuit_breaker_dataset]
    )
    

    sharepoint_sourcing_tasks >> sourcing_tasks >> raw_crawler >> detect_schema_change() >> raw_to_curated() >> run_circuit_breaker_models_dag
