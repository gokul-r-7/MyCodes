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
jplus_o3_dataset = Dataset("//o3/jplus/o3_import")

#Setup Global Env Variables
domain_name = "finance"
source_system_id = "jplus"
region = "ap-southeast-2"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
BATCH_DATE_FORMAT= "%Y-%m-%d"
max_project_concurrency = 1
projects = ['41']
jplus_datasets = ["timecard"]

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
    "jplus_sourcing": f"worley-datalake-sydney-{environment}-glue-job-smb-connector",
    "jplus_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "jplus_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "jplus_o3_import": f"worley-datalake-sydney-{environment}-glue-job-jplus-o3-import-api"
}

with DAG(
    dag_id=f"INT1064_jplus_{environment}_finance_data_pipeline",
    schedule=[jplus_o3_dataset],
    default_args=default_args,
    tags=["finance_jplus"],
    start_date=datetime.datetime(2025,2,4),
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
    
    @task_group(group_id="jplus_source_extraction")
    def jplus_source_extraction():
        for dataset in jplus_datasets:
            jplus_source_extraction = GlueJobOperator(
                task_id=f"jplus_source_extraction_{dataset}",
                job_name=glue_job_names["jplus_sourcing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--batch_run_start_time": batch_run_start_time_str,
                    "--source_name": source_system_id,
                    "--function_name": dataset
                },
            )

    @task_group(group_id="jplus_csv_xlsx_task")
    def jplus_csv_xlsx_task(): 
        for dataset in jplus_datasets:
            jplus_csv_xlsx_task = GlueJobOperator(
                task_id=f"jplus_csv_xlsx_parsing",
                job_name=glue_job_names["jplus_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_system_id,
                    "--function_name": f"csv_xlsx",                                      
                    "--connector_file_path" : f"finance/jplus/{dataset}/raw/{batch_run_start_time_str}/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )
            
            
    raw_crawler = GlueCrawlerOperator(
            task_id=f"jplus_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-finance-jplus-raw",
                   },
            poll_interval=5,
            wait_for_completion=False,
        )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_jplus_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-finance-jplus-raw"
    )    
   
    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in jplus_datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["jplus_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"curated_{source_system_id}",
                    "--metadata_type": f"curated#{source_system_id}#{table}#job#iceberg",                                    
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    
    @task_group(group_id=f"jplus_o3_import")
    def jplus_o3_import():
        for project_id in projects:
            jplus_o3_import = GlueJobOperator(
                task_id=f"import_{source_system_id}_o3_timecard_{project_id}",
                job_name=glue_job_names["jplus_o3_import"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}",
                    "--function_name": f"o3_import",
                    "--project_id": f"{project_id}",
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",                        
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )    

    batch_run_start_time_str >> jplus_source_extraction() >> jplus_csv_xlsx_task() >> raw_crawler >> raw_crawler_sensor >> raw_to_curated() >> jplus_o3_import()
