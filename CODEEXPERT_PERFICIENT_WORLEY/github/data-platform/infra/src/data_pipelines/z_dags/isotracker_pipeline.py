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
isotracker_engineering_dataset = Dataset(
    "//isotracker/engineering/domain_integrated_model")

#Setup Global Env Variables
domain_name = "engineering"
source_system_id = "isotracker"
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
raw_datasets=["SharePointIsoTrackerBOG","SharePointIsoTrackerBOP_UM_UZ","SharePointIsoTrackerLFM","SharePointIsoTrackerMAR","SharePointIsoTrackerNRU","SharePointIsoTrackerUG","SharePointIsoTrackerCP2_STRESSCALC"]
csv_datasets=["bog","bop_um_uz","lfm","mar","nru","ug","cp2_stress_calc"]
curated_datasets=["bog","bop_um_uz","lfm","mar","nru","ug","cp2_stress_calc"]
glue_job_names = {
    "sharepoint_sourcing": f"worley-datalake-sydney-{environment}-glue-job-sharepoint-document-api-sourcing",
    "isotracker_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "sharepoint_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"isotracker_{environment}_{instance_name}_engineering_data_pipeline", #Sharepoint_Doc_instance_data_pipeline
    schedule="30 9 * * 1-5",
    default_args=default_args,
    tags=["engineering_isotracker"],
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
    @task_group(group_id="isotracker_sharepointdoc_source_extraction")
    def isotracker_sharepointdoc_source_extraction():
        for src_file in raw_datasets:
            isotracker_sharepointdoc_source_extraction = GlueJobOperator(
                task_id=f"isotracker_sharepointdoc_source_extraction_{src_file}",
                job_name=glue_job_names["sharepoint_sourcing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--batch_run_start_time": batch_run_start_time_str,
                    "--source_name": f"{src_file}",
                },
            )
    @task_group(group_id="isotracker_csv_xlsx_task")
    def isotracker_csv_xlsx_task(): 
        for src in csv_datasets:
            isotracker_csv_xlsx_task = GlueJobOperator(
                task_id=f"isotracker_csv_xlsx_parsing_{src}",
                job_name=glue_job_names["isotracker_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": f"{source_system_id}",
                    "--function_name": f"csv_xlsx#{src}",
                    "--connector_file_path" : f"engineering/isotracker/raw/{src}/{batch_run_start_time_str}_UTC/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )   
    '''raw_crawler = GlueCrawlerOperator(
            task_id=f"sharepoint_csv_xlsx_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-engineering-isotracker-raw",
                   },
            poll_interval=5,
            wait_for_completion=False,
        )'''
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
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#{source_system_id}#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    run_isotracker_engineering_models_dag = BashOperator(
        task_id='run_engineering_models_dag',
        bash_command='echo "run engineering models dag"',
        outlets=[isotracker_engineering_dataset]
    )    
    batch_run_start_time_str >> isotracker_sharepointdoc_source_extraction() >> isotracker_csv_xlsx_task() >> raw_to_curated()
