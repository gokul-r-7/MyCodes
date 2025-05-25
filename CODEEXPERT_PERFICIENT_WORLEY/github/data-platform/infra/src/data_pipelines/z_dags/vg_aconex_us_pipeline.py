import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
import requests

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Setup Global Env Variables
source_system_id = "aconex"
region = "ap-southeast-2"
endpoint_host ="us1.aconex.com"
instance_name= "us_instance"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
# Get the current date
today = datetime.datetime.now()
year,month,day = today.strftime('%Y-%m-%d').split('-')
hour = today.strftime('%Y-%m-%dT%H:%M:%S').split('T')[1].split(':')[0]
max_project_concurrency = 1
vg_aconex_project_list = [
    "1207979025"
]
default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}

datasets=[
    # "project",
    # "Mail_inbox",
    # "Mail_sentbox",
    "workflow",
    # "UserDirectory",
    # "UserProject",
    # "UserProjectRole",
    # "docregister_custom",
    # "docregister_schema",
    # "docregister_standard"
]

#"aconex_UserDirectory": "worley-datalake-sydney-dev-glue-job-aconex-userdirectory-api-sourcing"

glue_job_names = {
    # "aconex_mail" : f"worley-datalake-sydney-{environment}-glue-job-aconex-mail-api-sourcing",
    "aconex_workflow" : f"worley-datalake-sydney-{environment}-glue-job-aconex-workflow-api-sourcing",
    # "aconex_UserProject" : f"worley-datalake-sydney-{environment}-glue-job-aconex-userproject-api-sourcing",
    # "aconex_documentregister" : f"worley-datalake-sydney-{environment}-glue-job-aconex-documentregister-api-sourcing"
}

# Create DAG
with DAG(
    dag_id=f"aconex_{environment}_{instance_name}_vg_data_pipeline", #aconex__us_instance_data_pipeline
    schedule="@daily",
    default_args=default_args,
    tags=["VG_aconex","VG_aconex_US"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task_group(group_id="vg_run_glue_jobs")
    def vg_run_glue_jobs(ProjectId):
        """Task Group to run all Glue jobs in parallel for a given project ID"""
        glue_job_tasks = []

        for job_name, job_script in glue_job_names.items():
            glue_job_task = GlueJobOperator(
                task_id=f"glue_job_{job_name}",
                job_name=job_script,
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--ProjectId": ProjectId,
                    "--endpoint_host": endpoint_host
                    # Add any other required script arguments
                },
            )
            glue_job_tasks.append(glue_job_task)

        return glue_job_tasks
    
    UserDirectory_job = GlueJobOperator(
        task_id="UserDirectory_job",
        job_name=f"worley-datalake-sydney-{environment}-glue-job-aconex-userdirectory-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        max_active_tis_per_dagrun=max_project_concurrency,
        script_args={
            "--endpoint_host": endpoint_host,
            # Add any required script arguments
        },
    )
   
    raw_crawler = GlueCrawlerOperator(
        task_id=f"aconex_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-aconex",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#aconex#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    # VG dedicated US instance pipeline
    vg_run_glue_jobs.expand(ProjectId=vg_aconex_project_list) >> raw_crawler >> raw_to_curated()
    # UserDirectory_job >> raw_crawler >> raw_to_curated()
