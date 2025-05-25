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

#Datasets for Downstream Engineering Domain DBT model
e3d_engineering_dataset = Dataset("//e3d/engineering/domain_integrated_model")

#Setup Global Env Variables
domain_name = "engineering"
source_system_id = "e3d"
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


raw_datasets=["e3d"]
csv_datasets=["ado"]
curated_datasets=["ado_csv_and_xlsx"]


glue_job_names = {
    "e3d_ado_sourcing": f"worley-datalake-sydney-{environment}-glue-job-smb-connector",
    "e3d_ado_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "e3d_ado_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"e3d_ado_{environment}_engineering_data_pipeline", #E3d_ado_ModelReviewTrackerFile_data_pipeline
    schedule="50 22 * * 1-5",
    default_args=default_args,
    tags=["engineering_e3d_ado"],
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
       
    
    @task_group(group_id="e3d_ado_source_extraction")
    def e3d_ado_source_extraction():
        for src_file in raw_datasets:
            e3d_ado_source_extraction = GlueJobOperator(
                task_id=f"e3d_ado_source_extraction_{src_file}",
                job_name=glue_job_names["e3d_ado_sourcing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--batch_run_start_time": batch_run_start_time_str,
                    "--source_name": f"{src_file}",
                    "--function_name": "ado"
                },
            )
    @task_group(group_id="e3d_ado_csv_xlsx_task")
    def e3d_ado_csv_xlsx_task(): 
        for src in csv_datasets:
            e3d_ado_csv_xlsx_task = GlueJobOperator(
                task_id=f"e3d_ado_csv_xlsx_parsing_{src}",
                job_name=glue_job_names["e3d_ado_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_system_id,
                    "--metadata_type": f"csv_xlsx#vg#{src}#{source_system_id}",                                      
                    "--function_name": f"csv_xlsx#vg#{src}",                                        
                    "--connector_file_path" : f"engineering/e3d/raw/{src}/{batch_run_start_time_str}/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )
            
            
    raw_crawler = GlueCrawlerOperator(
            task_id=f"e3d_ado_csv_xlsx_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-engineering-e3d-raw",
                   },
            poll_interval=5,
            wait_for_completion=False,
        )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_e3d_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-engineering-e3d-raw"
    )    
   
    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in curated_datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["e3d_ado_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#vg_E3D#{table}#job#iceberg",                                    
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    
    run_e3d_engineering_models_dag = BashOperator(
        task_id='run_engineering_models_dag',
        bash_command='echo "run engineering models dag"',
        outlets=[e3d_engineering_dataset]
    )    

    batch_run_start_time_str >> e3d_ado_source_extraction() >> e3d_ado_csv_xlsx_task() >> raw_crawler >> raw_crawler_sensor >> raw_to_curated()
