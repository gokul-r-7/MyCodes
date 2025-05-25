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

# Load datasets from Airflow variable
datasets_config = Variable.get("static_file_datasets", deserialize_json=True)

#Setup Global Env Variables
domain_name = "engineering"

region = "ap-southeast-2"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
max_project_concurrency = 1



default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs": 1,
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

source_system_id = datasets_config.get("source_system_id", "")
raw_datasets = datasets_config.get("raw", [])
csv_datasets = datasets_config.get("csv", [])
curated_datasets = datasets_config.get("curated", [])

glue_job_names = {
    "static_file_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "static_file_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"static_file_{environment}_data_pipeline",
    schedule=None,
    default_args=default_args,
    tags=["static_files"],
    start_date=datetime.datetime(2025,2,4),
    catchup=False,
    max_active_runs=max_project_concurrency,
) as dag:
    
    
    # Gets current execution date
    execution_date = "{{logical_date}}"

       

    @task_group(group_id="static_file_csv_xlsx_task")
    def static_file_csv_xlsx_task_group(): 
        for src in csv_datasets:
            static_file_csv_xlsx_task = GlueJobOperator(
                task_id=f"static_file_csv_xlsx_parsing_{src}",
                job_name=glue_job_names["static_file_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_system_id,
                    "--metadata_type": f"csv_xlsx#{src}#{source_system_id}",                                      
                    "--function_name": f"csv_xlsx#{src}",                                        
                    "--connector_file_path": f"static_files/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )
            

    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in curated_datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["static_file_raw_curated"],
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
 

    static_file_csv_xlsx_task_group() >> raw_to_curated()
