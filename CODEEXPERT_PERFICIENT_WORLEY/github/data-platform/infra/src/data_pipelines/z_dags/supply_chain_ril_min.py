import datetime
import pendulum
import os
import boto3
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import json
from pytz import timezone
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
import logging

# Retrieve variables
env_config = Variable.get("env", deserialize_json=True)
environment = env_config["environment"]
proj_config = Variable.get("supply_chain_erm_ril_min", deserialize_json=True)
project_id = proj_config["project_id"]
region_full_name = "ap-southeast-2"


# Setup Global Env Variables
source_system_id = "erm"
function_name = "registerprocess"
# DynamoDB table configuration
metadata_table_name = f"worley-mf-sydney-{environment}-metadata-table"
voucherno = "1022"
drl_query_id = "Q1991"
picklist_query_id = "Q1989"
supply_chain_ril_dataset = Dataset("//erm/supply_chain/domain_integrated_model")

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs" : 1,
    "catchup" : False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=3),
}

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"supply_chain_ril_min{environment}_data_pipeline"
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

datasets = ["ril_min_list"]


glue_job_names = {
    "ril_csv_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "ril_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "register_process": f"worley-datalake-sydney-{environment}-glue-job-erm-min-register-api-sourcing"
}

# Get current date and time and return partition path
def create_partitioned_path_hive_style():
    now=datetime.datetime.now(timezone('Australia/Sydney'))
    year=now.year
    month=now.month
    day=now.day
    hour=now.hour
    minute=now.minute
    partitioned_path=f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"
    return partitioned_path

# Create DAG
with DAG(
    dag_id=f"supply_chain_ril_min_{environment}_data_pipeline",
    schedule="@once",
    tags=["supplychain_ril"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 # This ensures only one active DAG run at a time
) as dag:

    # Create time partition path for s3
    s3_partition_path = PythonOperator(
        task_id='partition_path',
        python_callable=create_partitioned_path_hive_style
    )
    
    # sftp_sourcing = GlueJobOperator(
    #     task_id="sftp_sourcing",
    #     job_name=f"worley-datalake-{region_full_name}-{environment}-glue-job-sftp-sourcing",
    #     region_name=region,
    #     verbose=True,
    #     wait_for_completion=True,
    #     script_args={
    #         "--bucket_key": 'health_safety_environment/assurance/raw/'+'{{ ti.xcom_pull(task_ids="partition_path") }}',
    #         "--source_name": source_system_id,
    #         "--function_name": function_name
    #     },
    # )
    
    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()



    @task_group(group_id="ril_csv_sourcing")
    def ril_csv_sourcing():
        """Task Group that runs the required steps to source ril"""

        csv_task = GlueJobOperator(
            task_id="ril_csv_parsing",
            job_name=glue_job_names["ril_csv_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"csv#{source_system_id}",
                "--function_name": "csv",
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--connector_file_path" : 'supply_chain/erm/raw/'   #+'{{ ti.xcom_pull(task_ids="partition_path") }}'
            },
        )
        
    sourcing_tasks = ril_csv_sourcing()

    # raw_crawler = GlueCrawlerOperator(
    #     task_id=f"ril_csv_raw_crawler",
    #     config={
    #         "Name": f"worley-datalake-sydney-{environment}-glue-crawler-supply-chain-{source_system_id}-raw",
    #     },
    #     poll_interval=5,
    #     wait_for_completion=False,
    # )
    # # Raw crawler sensor
    # raw_crawler_sensor = GlueCrawlerSensor(
    #     task_id="wait_for_erm_raw_crawler",
    #     crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-supply-chain-{source_system_id}-raw",
    # )

    # # Detect Schema Change Task Group with batching
    # @task_group(group_id="detect_schema_change")
    # def detect_schema_change():
    #     previous_batch_group = None
    #     for i in range(0, len(datasets), 3):  # Batch size of 3
    #         batch = datasets[i:i+3]
    #         batch_group_id = f"batch_{i//3}"

    #         with TaskGroup(group_id=batch_group_id) as batch_group:
    #             for table in batch:
    #                 task = GlueJobOperator(
    #                     task_id=f"raw_{source_system_id}_{table.lower()}_schema_change",
    #                     job_name=glue_job_names["schema_change_detection"],
    #                     region_name=region,
    #                     verbose=True,
    #                     wait_for_completion=True,
    #                     max_active_tis_per_dagrun=3,
    #                     script_args={
    #                         "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_supply_chain_{source_system_id}_raw",
    #                         "--table_name": f"raw_{table.lower()}"
    #                     },
    #                 )

    #         # Set up dependency to ensure sequential execution between batches
    #         if previous_batch_group:
    #             previous_batch_group >> batch_group
    #         previous_batch_group = batch_group



    # Raw to Curated Task Group with batching
    @task_group(group_id="curation")
    def raw_to_curated():
        previous_batch_group = None
        for i in range(0, len(datasets), 3):  # Batch size of 3
            batch = datasets[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table}",
                        job_name=glue_job_names["ril_raw_curated"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_system_id": f"{source_system_id}_curated",
                            "--metadata_type": f"curated#erm#{table}#job#iceberg",
                            "--start_date": execution_date,
                            "--end_date": execution_date,
                        },
                    )                

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group


    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )
    
    # run_supply_chain_models_dag = BashOperator(
    #     task_id='run_supply_chain_ril_models_dag',
    #     bash_command='echo "run supply_chain ril models dag"',
    #     outlets=[supply_chain_ril_dataset]
    #)

    @task_group(group_id="run_register_process_glue_jobs")
    def run_register_process_glue_jobs():
        """Task Group to run Import and status process Glue jobs a given Project ID"""
        doc_reg_task = GlueJobOperator(
                task_id=f"registerprocess",
                job_name=glue_job_names["register_process"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=1,
                script_args={
                    "--source_name": source_system_id,
                    "--drl_query_id": drl_query_id,
                    "--picklist_query_id": picklist_query_id,
                    "--project_id": project_id,
                    "--metadata_table_name": metadata_table_name,
                    "--function_name": function_name
                    # Add any other required script arguments
                },
            )    
    
    (
     sourcing_tasks >> raw_to_curated() >> run_register_process_glue_jobs() >> sns_notification_for_failure
     #sourcing_tasks >> raw_to_curated() >> run_supply_chain_models_dag >> run_register_process_glue_jobs() >> sns_notification_for_failure
    )



