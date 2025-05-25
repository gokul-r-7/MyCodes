import datetime
import pendulum
import pytz

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Retrieve environment variable
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Global Configurations
region = "ap-southeast-2"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
max_project_concurrency = 1

# DAG-Specific Configurations
dag_id = "aconex_bot_package_cacdcwpspf06v_pipeline"
instance_name = "package_cacdcwpspf06v"
source_system_id = "aconex"

glue_job_names = {
    "source_extraction": f"worley-datalake-sydney-{environment}-glue-job-smb-connector-bot-files",
    "csv_parsing": f"worley-datalake-sydney-{environment}-glue-job-aconex-csv-xlsx-to-parquet-converter",
    "raw_to_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

with DAG(
    dag_id=dag_id,
    schedule=None,
    default_args=default_args,
    tags=["package", "us1"],
    start_date=datetime.datetime(2025, 2, 4),
    catchup=False,
    max_active_runs=max_project_concurrency,
) as dag:
    
    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task
    def format_execution_date():
        today = datetime.datetime.now(pytz.UTC)
        return today.strftime(TIMESTAMP_FORMAT)

    batch_run_start_time_str = format_execution_date()

    @task_group(group_id="source_extraction")
    def source_extraction():
        GlueJobOperator(
            task_id="source_extraction",
            job_name=glue_job_names["source_extraction"],
            region_name=region,
            wait_for_completion=True,
            script_args={
                "--batch_run_start_time": f"{batch_run_start_time_str}_UTC",
                "--source_name": f"{source_system_id}",
                "--function_name": f"{instance_name}"
            },
        )

    csv_parsing_task = GlueJobOperator(
        task_id="csv_parsing_task",
        job_name=glue_job_names["csv_parsing"],
        region_name=region,
        wait_for_completion=True,
        script_args={
            "--source_name": f"{source_system_id}",
            "--metadata_type": f"csv_xlsx#{source_system_id}#{environment}#{instance_name}",
            "--function_name": f"{instance_name}",
            "--instance_name": f"{instance_name}",
            "--connector_file_path": f"document_control/aconex/package_report/raw/instance_name={instance_name}/{batch_run_start_time_str}_UTC/",
            "--start_date": execution_date,
            "--end_date": execution_date,
            "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
        },
        trigger_rule='none_failed'
    )

    raw_crawler = GlueCrawlerOperator(
        task_id=f"aconex_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_aconex_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw"
    )

    raw_curated_generic = GlueJobOperator(
                task_id=f"cur_{source_system_id}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#acone#package_report#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--instance_name": instance_name
                },
    )

    batch_run_start_time_str >> source_extraction() >> csv_parsing_task >> raw_crawler >> raw_crawler_sensor >> raw_curated_generic 
