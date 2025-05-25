import datetime
import pendulum
import os
import boto3
from botocore.exceptions import ClientError
import sys
from botocore.config import Config
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
import logging
import json
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator 

# Retrieve variables from airflow environment variables
dag_config = Variable.get("env", deserialize_json=True)
dag_s3_config = Variable.get("s3_bucket_name", deserialize_json=True)

# Setup Global Env Variables
environment = dag_config["environment"]
raw_bucket_name=dag_s3_config["raw_bckt"]
region = "ap-southeast-2"
source_system_id = "ecosys"
secorg_ids = ["USGC","AMDAC","AMLA"]
initial_sourcing = ["secorglist","snapidlist","costtypepcsmapping","users","calendar","exchange_rates"]
secorg_datasets = ["header","snapshotlog","actuallog","workingforecastlog","earnedvaluelog","snapshotfull","findata","findatapcs","archival","projectKeyMembers"]
ecodata_dataset = ["ecodata"]
other_datasets = ["usersecorg","userproject"]
cur_audit_datasets = ["snapshot_process_audit"]
raw_audit_datasets = ["snapshot_processed"]
tables =    initial_sourcing + secorg_datasets + other_datasets + ecodata_dataset
cur_tables = tables + cur_audit_datasets
schema_tables = tables + raw_audit_datasets
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
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-ecosys-workflow-api-xml-sourcing",
    "ecosys_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
 
}

max_project_concurrency = 1 #For All other ecosys api's
max_ecodata_project_concurrency=2 # For ecodata api

# Datasets for Downstream project_control
project_control_dataset = Dataset("//ecosys/project_control/domain_integrated_model")

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"ecosys_sourcing_{environment}_data_pipeline"
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
    dag_id=f"ecosys_{environment}_generic_xml_api_data_pipeline",
    schedule="0 0 * * *",  #12 AM UTC Mon-Sun schedule
    default_args=default_args,
    tags=["ecosys"],
    start_date=pendulum.datetime(2024, 11, 14, tz="Asia/Calcutta"),
    catchup=False,
    max_active_runs = 1
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Start with the initial task
    start_task = DummyOperator(task_id='start_task')

    @task_group(group_id="ecosys_initial_sourcing")
    def ecosys_intial_sourcing(metadata: str,source_name: str = "ecosys" ):
              src_raw_initial = GlueJobOperator(
              task_id=f"{source_name}_initial_sourcing",
              job_name=glue_job_names["api_sourcing_job"],
              region_name=region,
              verbose=True,
              wait_for_completion=True,
              max_active_tis_per_dagrun=max_project_concurrency,
              script_args={
                   "--source_name": source_name,
                   "--function_name": metadata,
                   "--start_date": execution_date,
                   "--end_date": execution_date,
                   "--secorg_id" : '""'
            },
        )

    #Retrieve root level secorg id's from previous task
    @task
    def retrieve_ecosys_root_secorg():
        """Task to retrieve ecosys root secorg parquet from the S3 Bucket using Wrangler"""
        ecosys_root_secorg_s3_prefix = f"project_control/ecosys/rootsecorglist/"
        

        s3_client = boto3.client('s3')
        try:
            list_object_response = s3_client.list_objects_v2(Bucket=raw_bucket_name, 
                                    Prefix=ecosys_root_secorg_s3_prefix)
        except ClientError as ex:
                error_code = ex.response['Error']['Code']
                print(f"Error Code : {error_code} , Error Message : {ex}")
                sys.exit()
        
        if 'Contents' in list_object_response and len(
                    list_object_response['Contents']) == 1:
             
            # Get the key (file name) of the object
            secorg_file_key = list_object_response['Contents'][0]['Key']
            print(f"Aconex Project File : {secorg_file_key}")
            
            try:
                # Read the Project File
                project_file_obj = s3_client.get_object(Bucket=raw_bucket_name, 
                                                    Key=secorg_file_key)
                
                root_secorg_content = project_file_obj['Body'].read().decode('utf-8')

                root_secorg_id_values = [line.strip() for line in root_secorg_content.splitlines()]
                unique_root_secorg_id_list = list(set(root_secorg_id_values))
                print(f"Ecosys Secorg Values : {unique_root_secorg_id_list}")
            
            except ClientError as ex:
                error_code = ex.response['Error']['Code']
                print(f"Error Code : {error_code} , Error Message : {ex}")
            
            return unique_root_secorg_id_list
        else:
            print("There are either no file or more than one file in the Ecosys root secorg")
            sys.exit()

    ecosys_root_secorg_list_task = retrieve_ecosys_root_secorg()
    
    
    #Add conncurency level to task group
    @task_group(group_id="ecosys_sourcing_entities")
    def ecosys_entity_sourcing(secorg_id):
        source_name="ecosys"
        #for secorg_id in secorg_ids:
        for metadata in secorg_datasets:
            src_raw_entity = GlueJobOperator(
            task_id=f"{source_name}_{metadata}_entity_sourcing",
            job_name=glue_job_names["api_sourcing_job"],
            region_name=region,
            verbose=True,                
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_name,
                "--function_name": metadata,
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--secorg_id" : secorg_id

            },
        )
                
    #Add ecodata level to task group
    @task_group(group_id="ecosys_ecodata_entitiy")
    def ecosys_ecodata(secorg_id):
        source_name="ecosys"
        # for secorg_id in secorg_ids:
        for metadata in ecodata_dataset:
            src_raw_entity = GlueJobOperator(
            task_id=f"{source_name}_{metadata}_ecodata_sourcing",
            job_name=glue_job_names["api_sourcing_job"],
            region_name=region,
            verbose=True,                
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_ecodata_project_concurrency,
            script_args={
                "--source_name": source_name,
                "--function_name": metadata,
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--secorg_id" : secorg_id

            },
        )
          
                
    
    @task_group(group_id="ecosys_other_sourcing")
    def ecosys_other_sourcing(source_name: str = "ecosys" ):
        for metadata in other_datasets:
              src_raw_other = GlueJobOperator(
              task_id=f"{source_name}_{metadata}_other_sourcing",
              job_name=glue_job_names["api_sourcing_job"],
              region_name=region,
              verbose=True,
              wait_for_completion=True,
              max_active_tis_per_dagrun=max_project_concurrency,
              script_args={
                   "--source_name": source_name,
                   "--function_name": metadata,
                   "--start_date": execution_date,
                   "--end_date": execution_date,
                   "--secorg_id" : '""'
            },
        )



    raw_crawler = GlueCrawlerOperator(
        task_id=f"ecosys_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-project-control-ecosys-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )


    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in schema_tables:
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
        for table in cur_tables:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["ecosys_raw_curated"],
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

    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )

    run_project_control_models_dag = BashOperator(
        task_id='run_project_control_models_dag',
        bash_command='echo "run project_control models dag"',
        outlets=[project_control_dataset]
    )

    # End task
    end_task = DummyOperator(task_id='end_task')  # This will ensure the task runs after any successful branch)

    # Orchestrate the dag
    start_task >> ecosys_intial_sourcing.expand(metadata=initial_sourcing) >> (ecosys_root_secorg_list_task,ecosys_other_sourcing()) >> raw_crawler
    ecosys_root_secorg_list_task >> ecosys_entity_sourcing.expand(secorg_id=ecosys_root_secorg_list_task) >> ecosys_ecodata.expand(secorg_id=ecosys_root_secorg_list_task) >> raw_crawler
    raw_crawler >> detect_schema_change() >> raw_to_curated() >> sns_notification_for_failure >> run_project_control_models_dag >> end_task

 
