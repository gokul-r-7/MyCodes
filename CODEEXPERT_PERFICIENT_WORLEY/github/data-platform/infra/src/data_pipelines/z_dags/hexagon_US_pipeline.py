import datetime
import pendulum
import boto3
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
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
import logging


# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Datasets for Downstream Document Control Domain DBT model
hexagon_document_control_dataset = Dataset(
    "//hexagon/document_control/domain_integrated_model")

# Setup Global Env Variables
domain_name = "document_control"
source_system_id = "hexagon"
region = "ap-southeast-2"
raw_bucket_name = f"worley-datalake-sydney-{environment}-bucket-raw-xd5ydg"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
max_project_concurrency = 1
instance_name = "US"



default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}


raw_datasets=["SharePointHexagonOFE","SharePointHexagonCON"]
csv_datasets=["ofe","con"]
curated_datasets=["hexagon_ofe","hexagon_ofe_consolidate"]


glue_job_names = {
    "sharepoint_sourcing": f"worley-datalake-sydney-{environment}-glue-job-sharepoint-document-api-sourcing",
    "hexagon_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "sharepoint_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"hexagon_{environment}_{instance_name}_document_control_data_pipeline"
          })

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)

def invoke_lambda_function(lambda_function_name,payload):
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=payload            
        )
        print('Response--->', response)
        logging.error("SNS notification got triggered as one of the task failed.Hence mark main dag as failied")
        raise AirflowFailException("A task has failed, marking the DAG as failed.")
    except Exception as e:
        raise

with DAG(
    dag_id=f"hexagon_{environment}_{instance_name}_document_control_data_pipeline", #Sharepoint_Doc_instance_data_pipeline
    schedule="0 7 * * 1-5",
    default_args=default_args,
    tags=["document_control_hexagon"],
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
       
    
    @task_group(group_id="hexagon_sharepointdoc_source_extraction")
    def hexagon_sharepointdoc_source_extraction():
        for src_file in raw_datasets:
            hexagon_sharepointdoc_source_extraction = GlueJobOperator(
                task_id=f"hexagon_sharepointdoc_source_extraction_{src_file}",
                job_name=glue_job_names["sharepoint_sourcing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--batch_run_start_time": batch_run_start_time_str,
                    "--source_name": f"{src_file}",
                },
            )
    @task_group(group_id="hexagon_csv_xlsx_task")
    def hexagon_csv_xlsx_task(): 
        for src in csv_datasets:
            hexagon_csv_xlsx_task = GlueJobOperator(
                task_id=f"hexagon_csv_xlsx_parsing_{src}",
                job_name=glue_job_names["hexagon_csv_xlsx_parsing"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_name": source_system_id,
                    "--metadata_type": f"csv_xlsx#vg{src}#{source_system_id}",
                    "--function_name": f"csv_xlsx#vg{src}",
                    "--connector_file_path" : f"document_control/hexagon/raw/{src}/{batch_run_start_time_str}_UTC/",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--metadata_table_name": f"worley-mf-sydney-{environment}-metadata-table",
                },
                trigger_rule='none_failed'
            )
            
            
    raw_crawler = GlueCrawlerOperator(
            task_id=f"document_control_hexagon_raw_crawler",
            config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-document-control-hexagon-raw",
                   },
            poll_interval=5,
            wait_for_completion=False,
        )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_hexagon_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-document-control-hexagon-raw"
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
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#hexagon#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
    
    run_hexagon_document_control_models_dag = BashOperator(
        task_id='run_document_control_models_dag',
        bash_command='echo "run document control models dag"',
        outlets=[hexagon_document_control_dataset]
    )

    
    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )    

    batch_run_start_time_str >> hexagon_sharepointdoc_source_extraction() >> hexagon_csv_xlsx_task() >> raw_crawler >> raw_crawler_sensor >> raw_to_curated() >> run_hexagon_document_control_models_dag  >> sns_notification_for_failure
