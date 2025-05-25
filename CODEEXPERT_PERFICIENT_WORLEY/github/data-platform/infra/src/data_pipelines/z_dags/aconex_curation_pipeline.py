import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
import requests
import pytz

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
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

# Dataset for Upstream Sourcing
aconex_curation_dataset = Dataset("//aconex/curation")

# Datasets for Downstream Document Control
document_control_dataset = Dataset(
    "//aconex/document_control/domain_integrated_model")

# Setup Global Env Variables

REGION = "ap-southeast-2"
source_system_id = "aconex"
domain_name="Document_Control"
APPLICATION_NAME = "aconex"
SOURCE_NAME = "aconex"
datasets=[
    "Mail_inbox",
    "Mail_sentbox",
    "mail_document",
    "workflow",
    "docregister_custom",
    "docregister_schema",
    "docregister_standard"
]

instance_datasets=["project"]

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"aconex_{environment}_{domain_name}_curation_data_pipeline"
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


max_project_concurrency = 5
default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}


# Create DAG
with DAG(
    dag_id=f"aconex_{environment}_curation_data_pipeline",
    schedule=[aconex_curation_dataset],
    default_args=default_args,
    tags=["aconex",domain_name],
    start_date=datetime.datetime(2025, 4, 22),
    catchup=False,
    max_active_runs=1
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"
    
    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated_generic = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#aconex#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )
        

    run_document_control_models_dag = BashOperator(
        task_id='run_document_control_models_dag',
        bash_command='echo "run document control models dag"',
        outlets=[document_control_dataset]
    )
    
    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )    
    
    (
    raw_to_curated() >> run_document_control_models_dag >> sns_notification_for_failure
     
    )
 
