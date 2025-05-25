import datetime
import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
from airflow.models import Variable
from airflow.datasets import Dataset

from airflow.models.dag import DAG
from airflow.decorators import task_group, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Dataset for Upstream Sourcing
customer_csp_dataset = Dataset("//customer/csp/curation")

# Setup Global Env Variables
ENVIRONMENT = environment
REGION = "ap-southeast-2"
source_system_id = "csp_salesforce"
APPLICATION_NAME = "csp"
SOURCE_NAME = "salesforce"
entity_sourcing = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
tables = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
aurora_tables = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
max_project_concurrency = 5
default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-csp-salesforce-workflow-api-sourcing",
    "csp_salesforce_raw_curated": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-schema-change-detection-generic",
    "csp_salesforce_cur_to_aurora": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-iceberg-to-aurora-generic"
}

# Create DAG
with DAG(
    dag_id=f"csp_salesforce_{ENVIRONMENT}_curation_data_pipeline",
    schedule=[customer_csp_dataset],
    default_args=default_args,
    tags=["csp_salesforce"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    raw_crawler = GlueCrawlerOperator(
        task_id=f"csp_salesforce_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{ENVIRONMENT}-glue-crawler-raw-customer-csp-salesforce",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_csp_salesforce_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{ENVIRONMENT}-glue-crawler-raw-customer-csp-salesforce",
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in tables:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{ENVIRONMENT}_glue_catalog_database_customer_{source_system_id}_raw",
                    "--table_name": f"raw_{table}"
                },
            )


    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in tables:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["csp_salesforce_raw_curated"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#csp_salesforce#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )


    @task_group(group_id=f"curation_to_aurora")
    def curation_to_aurora():
        for table in aurora_tables:
            curated_to_aurora = GlueJobOperator(
                task_id=f"cur_to_aurora_{source_system_id}_{table}",
                job_name=glue_job_names["csp_salesforce_cur_to_aurora"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#csp_salesforce#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--source_system": source_system_id
                },
            )
    (
    raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> curation_to_aurora()
     
    )
 