import datetime
import pendulum
from botocore.exceptions import ClientError
import pytz

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset


# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]


#Setup Global Env Variables
domain_name = "salesforce"
source_system_id = "o3"
#function_name = "conv_tier"
function_names = ["fabrication_status"]
region = "ap-southeast-2"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
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


glue_job_names = {
    "csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csp-salesforce-csv-xlsx-to-parquet-converter",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "o3_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"o3_custom_csv_{environment}_data_generic", 
    schedule="@once",
    default_args=default_args,
    tags=["o3"],
    start_date=datetime.datetime(2025,2,4),
    catchup=False,
    max_active_runs=max_project_concurrency,
) as dag:

    execution_date = "{{ logical_date }}"

    @task
    def format_execution_date():
        today = datetime.datetime.now(pytz.UTC)
        return today.strftime(TIMESTAMP_FORMAT)

    batch_run_start_time_str = format_execution_date()

    # CSV Parsing Group (Single Group with multiple functions)
    @task_group(group_id="csv_parsing")
    def csv_parsing():
        for fn in function_names:
            GlueJobOperator(
                task_id=f"{fn}",
                job_name=glue_job_names["csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_system_id,
                    "--metadata_type": f"{fn}#{source_system_id}",
                    "--function_name": fn,
                    "--connector_file_path": "construction/export_csv/",
                    "--start_date": "{{ ti.xcom_pull(task_ids='format_execution_date') }}",
                    "--end_date": "{{ ti.xcom_pull(task_ids='format_execution_date') }}",
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )

    # Schema Change Detection Group
    @task_group(group_id="schema_check")
    def schema_check():
        for fn in function_names:
            GlueJobOperator(
                task_id=f"{fn}_schema_check",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_construction_{source_system_id}_raw",
                    "--table_name": f"raw_{fn}"
                },
            )

    # Curation Group
    @task_group(group_id="curation")
    def curation():
        for fn in function_names:
            GlueJobOperator(
                task_id=f"{fn}_curation",
                job_name=glue_job_names["o3_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"curated_{source_system_id}",
                    "--metadata_type": f"curated#{fn}#{source_system_id}#job#iceberg",
                    "--start_date": "{{ ti.xcom_pull(task_ids='format_execution_date') }}",
                    "--end_date": "{{ ti.xcom_pull(task_ids='format_execution_date') }}",
                },
            )

    csv_tasks = csv_parsing()

    raw_crawler = GlueCrawlerOperator(
        task_id=f"o3_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-construction-o3-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_o3_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-construction-o3-raw",
    )
    
    schema_tasks = schema_check()
    curated_tasks = curation()

    chain(
        batch_run_start_time_str,
        csv_tasks,
        raw_crawler,
        raw_crawler_sensor,
        schema_tasks,
        curated_tasks
    )

