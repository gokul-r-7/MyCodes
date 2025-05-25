import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
import requests
import pytz

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

# Retrieve variables for process
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Datasets for Downstream Document Control
document_control_dataset = Dataset(
    "//aconex/document_control/domain_integrated_model")

# Setup Global Env Variables
domain_name = "document_control"
source_system_id = "aconex"
region = "ap-southeast-2"
endpoint_host ="uk1.aconex.co.uk"
instance_name= "uk_instance"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Get the current date
today = datetime.datetime.now(pytz.UTC)
batch_run_start_time_str = today.strftime(TIMESTAMP_FORMAT)
max_project_concurrency = 2
document_control_aconex_project_list = [
    "268452089", "268456382","268456802","268456867","268454994"
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
    "workflow",
    "docregister_custom",
    "docregister_schema",
    "docregister_standard"
]

instance_datasets=[
    "project",
    "UserDirectory",
    "UserProject",
    "UserProjectRole"
]


mail_datasets=[
    "Mail_inbox",
    "Mail_sentbox",
    "mail_document"
]

#"aconex_UserDirectory": "worley-datalake-sydney-dev-glue-job-aconex-userdirectory-api-sourcing"

glue_job_names = {
    "aconex_workflow" : f"worley-datalake-sydney-{environment}-glue-job-aconex-workflow-api-sourcing",
    "aconex_UserProject" : f"worley-datalake-sydney-{environment}-glue-job-aconex-userproject-api-sourcing",
    "aconex_UserProjectRole" : f"worley-datalake-sydney-{environment}-glue-job-aconex-userprojectrole-api-sourcing",
    "aconex_documentregister" : f"worley-datalake-sydney-{environment}-glue-job-aconex-documentregister-api-sourcing"
}
mail_glue_job_names = {
    "aconex_mail" : f"worley-datalake-sydney-{environment}-glue-job-aconex-mail-api-sourcing",
    "aconex_mail_doc" : f"worley-datalake-sydney-{environment}-glue-job-aconex-maildocument-api-sourcing"
}

# Create DAG
with DAG(
    dag_id=f"aconex_{environment}_{instance_name}_document_control_data_pipeline", #aconex__us_instance_data_pipeline
    schedule="0 0 * * 1-5",
    default_args=default_args,
    tags=["document_control_aconex","document_control_aconex_UK"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
    max_active_runs = max_project_concurrency,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    Project_job = GlueJobOperator(
        task_id="aconex_uk_list_Project_job",
        job_name=f"worley-datalake-sydney-{environment}-glue-job-aconex-project-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--endpoint_host": endpoint_host,
            "--instance_name": instance_name
            # Add any required script arguments
        },
    )


    @task_group(group_id="doc_control_run_glue_jobs_uk")
    def doc_control_run_glue_jobs(ProjectId):
        """Task Group to run all Glue jobs in parallel for a given project ID"""
        glue_job_tasks = []

        for job_name, job_script in glue_job_names.items():
            glue_job_task = GlueJobOperator(
                task_id=f"glue_job_{job_name}_uk",
                job_name=job_script,
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--ProjectId": ProjectId,
                    "--endpoint_host": endpoint_host,
                    "--instance_name": instance_name
                    # Add any other required script arguments
                },
            )
            glue_job_tasks.append(glue_job_task)

        return glue_job_tasks


    #Add conncurency level to task group
    @task_group(group_id="aconex_uk_mail_sourcing_entities")
    def mail_sourcing_entity(ProjectId):
        mail_src_raw_initial = GlueJobOperator(
            task_id=f"document_control_aconex_mail_entity_sourcing_uk",
            job_name=mail_glue_job_names["aconex_mail"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--ProjectId": ProjectId,
                "--batch_run_start_time": batch_run_start_time_str,
                "--endpoint_host": endpoint_host,
                "--instance_name": instance_name
                },
            )

    #mail_document Sourcing Aconex mail metadata
    @task_group(group_id="aconex_uk_mail_doc_sourcing_entities")
    def mail_doc_sourcing_entity(ProjectId):
        mail_src_raw_initial = GlueJobOperator(
            task_id=f"document_control_aconex_mail_document_entity_sourcing_uk",
            job_name=mail_glue_job_names["aconex_mail_doc"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--ProjectId": ProjectId,
                "--batch_run_start_time": batch_run_start_time_str,
                "--endpoint_host": endpoint_host,
                "--instance_name": instance_name
                },
            )


    UserDirectory_job = GlueJobOperator(
        task_id=f"aconex_uk_UserDirectory_job",
        job_name=f"worley-datalake-sydney-{environment}-glue-job-aconex-userdirectory-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        max_active_tis_per_dagrun=max_project_concurrency,
        script_args={
            "--endpoint_host": endpoint_host,
            "--instance_name": instance_name
            # Add any required script arguments
        },
    )
   
    raw_crawler = GlueCrawlerOperator(
        task_id=f"aconex_uk_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_aconex_uk_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw",
    )

    @task_group(group_id=f"aconex_uk_curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"aconex_uk_cur_{source_system_id}_{table}",
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
        for table in instance_datasets:
            raw_curated_entity_load = GlueJobOperator(
                task_id=f"aconex_uk_cur_{source_system_id}_{table}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#aconex#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--instance_name": instance_name
                },
            )                
            

    @task_group(group_id=f"aconex_uk_mail_curation")
    def mail_raw_to_curated():
        for mail_table in mail_datasets:
            mail_raw_curated = GlueJobOperator(
                task_id=f"aconex_uk_cur_{source_system_id}_{mail_table}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#aconex#{mail_table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    run_document_control_models_dag = BashOperator(
        task_id='run_document_control_models_dag',
        bash_command='echo "run document control models dag"',
        outlets=[document_control_dataset]
    )

    # document_control dedicated US instance pipeline
    mail_sourcing_entity.expand(ProjectId=document_control_aconex_project_list) >> mail_doc_sourcing_entity.expand(ProjectId=document_control_aconex_project_list) >> raw_crawler >> raw_crawler_sensor >> mail_raw_to_curated()
    Project_job >> doc_control_run_glue_jobs.expand(ProjectId=document_control_aconex_project_list) >> raw_crawler
    UserDirectory_job >> raw_crawler >> raw_crawler_sensor >> raw_to_curated() >> run_document_control_models_dag
