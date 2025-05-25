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
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]
dag_s3_config = Variable.get("s3_bucket_name", deserialize_json=True)


# Setup Global Env Variables for this
domain_name = "project_control"
source_system_id = "ecosys"
region = "ap-southeast-2"
raw_bucket_name = dag_s3_config["raw_bckt"]
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
max_project_concurrency = 1
instance_name = "US"



default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "catchup": False,
    "max_active_runs":1
}

raw_datasets=["cost_category", "global_locations", "global_phases", "gross_margin_category", "hours_category", "progress_category", "revenue_category", "budget_grid_cols", "budget_grid_rows", "data_type_switch", "invoice_due_categories", "progress_type", "project_risks", "project_sizes", "revenue_at_risk"]
curated_datasets=["costcategory","globallocations","globalphases","grossmargincategory","hourscategory","progresscategory","revenuecategory", "budget_grid_cols", "budget_grid_rows", "data_type_switch", "invoice_due_categories", "progress_type", "project_risks", "project_sizes", "revenue_at_risk"]



glue_job_names = {
    "ecosys_onetimeload_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "ecosys_onetimeload_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

with DAG(
    dag_id=f"ecosys_onetimeload_{environment}_{instance_name}_project_control_data_pipeline",
    schedule="@once",
    default_args=default_args,
    tags=["project_control_ecosys_onetimeload"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
    max_active_runs = max_project_concurrency   
) as dag:
    
    
    # Gets current execution date
    execution_date = "{{logical_date}}"

    @task_group(group_id="ecosys_onetimeload_csv_xlsx_task")
    def ecosys_onetimeload_csv_xlsx_task():
        ecosys_onetimeload_csv_xlsx_task = GlueJobOperator(
            task_id=f"ecosys_onetimeload_csv_xlsx_parsing",
            job_name=glue_job_names["ecosys_onetimeload_csv_xlsx_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"csv_xlsx#ecosys",
                "--function_name": f"csv_xlsx",
                "--connector_file_path" : f"project_control/ecosys/onetimeload/raw/",
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
            },
            trigger_rule='none_failed'
            )
            
            
    raw_crawler = GlueCrawlerOperator(
            task_id=f"sharepoint_csv_xlsx_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-project-control-ecosys-raw",
                   },
            poll_interval=5,
            wait_for_completion=True,
        )
    
    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in raw_datasets:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_project_control_{source_system_id}_raw",
                    "--table_name": f"raw_{table}"
                },
            )   
    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in curated_datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["ecosys_onetimeload_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#ecosys#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
 

    ecosys_onetimeload_csv_xlsx_task() >> raw_crawler >> detect_schema_change() >> raw_to_curated()
