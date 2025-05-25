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

# Datasets for Downstream Document Control
document_control_dataset = Dataset(
    "//aconex/document_control/domain_integrated_model")


# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]
s3_bckt_config = Variable.get("s3_bucket_name", deserialize_json=True)
# Setup Global Env Variables
source_system_id = "aconex"
region = "ap-southeast-2"
endpoint_host ="cn1.aconexasia.com"
instance_name= "cn_instance"
raw_bucket_name = s3_bckt_config["raw_bckt"]

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Get the current date
today = datetime.datetime.now(pytz.UTC)
batch_run_start_time_str = today.strftime(TIMESTAMP_FORMAT)
year,month,day = today.strftime('%Y-%m-%d').split('-')
hour = today.strftime('%Y-%m-%dT%H:%M:%S').split('T')[1].split(':')[0]
aconex_project_s3_prefix = f"document_control/aconex/project_ids/{instance_name}/"
max_project_concurrency = 3
domain_name="Document_Control"

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}

instance_datasets=[  
    "UserDirectory",
    "UserProject",
    "UserProjectRole",]

#"aconex_UserDirectory": "worley-datalake-sydney-dev-glue-job-aconex-userdirectory-api-sourcing"

glue_job_names = {
    "aconex_UserProject" : f"worley-datalake-sydney-{environment}-glue-job-aconex-userproject-api-sourcing",
    "aconex_UserProjectRole" : f"worley-datalake-sydney-{environment}-glue-job-aconex-userprojectrole-api-sourcing",
}


#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"aconex_{environment}_{domain_name}_{instance_name}_users_data_pipeline"
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

# Create DAG
with DAG(
    dag_id=f"aconex_{environment}_{domain_name}_{instance_name}_users_data_pipeline", #aconex__us_instance_data_pipeline
    schedule="00 16 * * 1",
    default_args=default_args,
    tags=["aconex",f"aconex_{instance_name}_users",domain_name],
    start_date=datetime.datetime(2025, 4, 23),
    catchup=False,
    max_active_runs=1
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"
    #Retrieve project id's from previous task
    @task
    def retrieve_aconex_projects():
        """Task to retrieve aconex project parquet from the S3 Bucket using Wrangler"""
        aconex_project_s3_prefix = f"document_control/aconex/project_ids/{instance_name}/"

        s3_client = boto3.client('s3')
        try:
            list_object_response = s3_client.list_objects_v2(Bucket=raw_bucket_name, 
                                    Prefix=aconex_project_s3_prefix)
        except ClientError as ex:
                error_code = ex.response['Error']['Code']
                print(f"Error Code : {error_code} , Error Message : {ex}")
                sys.exit()
        
        if 'Contents' in list_object_response and len(
                    list_object_response['Contents']) == 1:
             
            # Get the key (file name) of the object
            project_file_key = list_object_response['Contents'][0]['Key']
            print(f"Aconex Project File : {project_file_key}")
            
            try:
                # Read the Project File
                project_file_obj = s3_client.get_object(Bucket=raw_bucket_name, 
                                                    Key=project_file_key)
                
                project_file_content = project_file_obj['Body'].read().decode('utf-8')

                project_id_values = [line.strip() for line in project_file_content.splitlines()]
                unique_project_id_list = list(set(project_id_values))
                print(f"Aconex Project Values : {unique_project_id_list}")
            
            except ClientError as ex:
                error_code = ex.response['Error']['Code']
                print(f"Error Code : {error_code} , Error Message : {ex}")
            
            return unique_project_id_list
        else:
            print("There are either no file or more than one file in the Aconex Project Directory")
            sys.exit()

    aconex_project_list_task = retrieve_aconex_projects()


    @task_group(group_id="run_cn_aconex_users_glue_jobs")
    def run_cn_users_glue_jobs(ProjectId):
        """Task Group to run all Glue jobs in parallel for a given project ID"""
        # User Project Task
        user_project_task = GlueJobOperator(
                task_id=f"aconex_cn_user_project",
                job_name=glue_job_names["aconex_UserProject"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--ProjectId": ProjectId,
                    "--endpoint_host": endpoint_host,
                    "--instance_name": instance_name
                    # Add any other required script arguments
                },
            )
        
        # User ProjectRole Task
        user_projectrole_task = GlueJobOperator(
                task_id=f"aconex_cn_user_projectrole",
                job_name=glue_job_names["aconex_UserProjectRole"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--ProjectId": ProjectId,
                    "--endpoint_host": endpoint_host,
                    "--instance_name": instance_name
                    # Add any other required script arguments
                },
            )
    UserDirectory_job = GlueJobOperator(
        task_id="aconex_cn_userDirectory",
        job_name=f"worley-datalake-sydney-{environment}-glue-job-aconex-userdirectory-api-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        max_active_tis_per_dagrun=max_project_concurrency,
        script_args={
            "--endpoint_host": endpoint_host,
            "--instance_name": instance_name
            # Add any required script arguments
        },
    )
  
    raw_crawler = GlueCrawlerOperator(
        task_id=f"aconex_cn_users_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_aconex_cn_users_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-document-control-aconex-raw"
    )
    

    @task_group(group_id=f"aconex_cn_users_curation")
    def raw_to_curated():
        for table in instance_datasets:
            raw_curated_entity_load = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}_cn",
                job_name=f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#aconex#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--instance_name": instance_name
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

    aconex_project_list_task >> run_cn_users_glue_jobs.expand(ProjectId=aconex_project_list_task) >> raw_crawler >> raw_crawler_sensor
    UserDirectory_job >> raw_crawler >> raw_crawler_sensor >> raw_to_curated() >> run_document_control_models_dag >> sns_notification_for_failure
