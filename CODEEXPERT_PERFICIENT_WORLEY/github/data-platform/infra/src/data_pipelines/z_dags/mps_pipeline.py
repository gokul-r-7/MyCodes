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

#check for all the parameters
# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Datasets for Downstream Engineering Domain DBT model
mps_engineering_dataset = Dataset(
    "//mps/engineering/domain_integrated_model")

#Setup Global Env Variables
domain_name = "engineering"
source_system_id = "SharePointMps_lfm"
region = "ap-southeast-2"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
max_project_concurrency = 1
instance_name = "US"
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
raw_datasets=["SharePointMps_lfm"]
csv_datasets=["mps"]
curated_datasets=["mps_log_lfm"]
glue_job_names = {
    "sharepoint_sourcing": f"worley-datalake-sydney-{environment}-glue-job-sharepoint-document-api-sourcing",
    "mps_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "sharepoint_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"mps_{environment}_{instance_name}_engineering_data_pipeline", #Sharepoint_Doc_instance_data_pipeline
    schedule="15 10 * * 1-5",
    default_args=default_args,
    tags=["engineering_mps"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
    max_active_runs = max_project_concurrency,
) as dag:
    # Gets current execution date
    execution_date = "{{logical_date}}"
    @task
    def format_execution_date():
        # Get the current date
        today = datetime.datetime.now(pytz.UTC)
        batch_run_start_time_str = today.strftime(TIMESTAMP_FORMAT)
        return batch_run_start_time_str
    batch_run_start_time_str = format_execution_date()
    @task_group(group_id="mps_sharepointdoc_source_extraction")
    def mps_sharepointdoc_source_extraction():
        for src_file in raw_datasets:
            mps_sharepointdoc_source_extraction = GlueJobOperator(
                task_id=f"mps_sharepointdoc_source_extraction_{src_file}",
                job_name=glue_job_names["sharepoint_sourcing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--batch_run_start_time": batch_run_start_time_str,
                    "--source_name": f"{src_file}",
                },
            )
    @task_group(group_id="mps_csv_xlsx_task")
    def mps_csv_xlsx_task(): 
        for src in csv_datasets:
            mps_csv_xlsx_task = GlueJobOperator(
                task_id=f"mps_csv_xlsx_parsing_{src}",
                job_name=glue_job_names["mps_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": src,
                    "--function_name": f"csv_xlsx",
                    "--connector_file_path" : f"engineering/{src}/raw/{batch_run_start_time_str}_UTC/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )   
    raw_crawler = GlueCrawlerOperator(
            task_id=f"sharepoint_csv_xlsx_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-engineering-mps-raw",
                   },
            poll_interval=5,
            wait_for_completion=False,
        )
        
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_mps_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-engineering-mps-raw"
    )
    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in curated_datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["sharepoint_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"mps_curated",
                    "--metadata_type": f"curated#mps#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    run_mps_engineering_models_dag = BashOperator(
        task_id='run_engineering_models_dag',
        bash_command='echo "run engineering models dag"',
        outlets=[mps_engineering_dataset]
    )    
    batch_run_start_time_str >> mps_sharepointdoc_source_extraction() >> mps_csv_xlsx_task() >> raw_crawler >> raw_crawler_sensor >> raw_to_curated()
