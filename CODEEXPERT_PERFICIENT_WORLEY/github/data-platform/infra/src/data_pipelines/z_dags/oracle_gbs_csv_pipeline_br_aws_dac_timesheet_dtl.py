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
from airflow.operators.bash import BashOperator
import json
from pytz import timezone
from airflow.datasets import Dataset
from botocore.config import Config

# Retrieve the variables
dag_config = Variable.get("oracle_gbs_sftp_variables_config", deserialize_json=True)

sftp_host = dag_config["sftp_host"]
remote_paths = json.dumps(dag_config["remote_paths"])
region_full_name = dag_config["region_full_name"]
environment = dag_config["environment"]
bucket_suffix = dag_config["bucket_suffix"]

# Setup the Global Env Variables
source_system_id = "oraclegbs"
function_name ="dac_timesheet"
txt_file_function_name="finance_ebscr9"

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"




datasets = ["br_aws_dac_timesheet_dtl"]
#high_volume_datasets=["data_rev", "data_cost"]
# Datasets for Downstream Finance
oracle_gbs_finance_dataset = Dataset(
    "//oraclegbs/finance/domain_integrated_model")

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
    "oracle_gbs_csv_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "oraclegbs_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
}

#added as part of the SNS notification
payload = json.dumps({
          "dag_name": f"oracle_gbs_csv_{environment}_data_pipeline"
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
    except Exception as e:
        raise


# Get current date and time and return the partition path
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
    dag_id=f"oracle_gbs_csv_{environment}_data_pipeline_br_aws_dac_timesheet_dtl",
    schedule=None,
    tags=["oracle_gbs_csv"],
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
    
    sftp_and_unzipping = GlueJobOperator(
        task_id="sftp_and_unzipping",
        job_name=f"worley-datalake-{region_full_name}-{environment}-glue-job-sftp-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--sftp_host": sftp_host,
            "--remote_paths": remote_paths,
            "--bucket_key": 'finance/dac/raw/'+'{{ ti.xcom_pull(task_ids="partition_path") }}',
            "--bucket_name": f"worley-datalake-{region_full_name}-{environment}-bucket-raw-{bucket_suffix}",
            "--secret_name": f"Worley-datalake-{region_full_name}-{environment}-oracle-gbs-sftp",
            "--source_name": source_system_id,
            "--function_name": function_name
        }
    )
    

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    p6_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    @task_group(group_id="oracle_gbs_csv_sourcing")
    def oracle_gbs_csv_sourcing():
        """Task Group that runs the required steps to source oracle_gbs"""

        csv_task = GlueJobOperator(
            task_id="oracle_gbs_csv_parsing",
            job_name=glue_job_names["oracle_gbs_csv_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"dac_timesheet_csv#{source_system_id}",
                "--function_name": "dac_timesheet_csv",
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--connector_file_path" : 'finance/dac/raw/'+'{{ ti.xcom_pull(task_ids="partition_path") }}'
            },
        )

        csv_task
        
    sourcing_tasks = oracle_gbs_csv_sourcing()
    
   

    raw_crawler = GlueCrawlerOperator(
        task_id=f"oracle_gbs_csv_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-finance-oracle-gbs-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in datasets:
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_finance_oracle_gbs_raw",
                    "--table_name": f"raw_{table}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["oraclegbs_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#oraclegbs#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    run_finance_models_dag = BashOperator(
        task_id='run_finance_models_dag',
        bash_command='echo "run finance models dag"',
        outlets=[oracle_gbs_finance_dataset]
    )
    
    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )

    s3_partition_path >> sftp_and_unzipping >> sourcing_tasks >> raw_crawler >> detect_schema_change() >> raw_to_curated() >> run_finance_models_dag >> sns_notification_for_failure
