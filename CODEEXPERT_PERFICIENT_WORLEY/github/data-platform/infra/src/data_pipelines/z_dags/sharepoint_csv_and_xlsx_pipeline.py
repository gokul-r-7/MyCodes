import datetime
import boto3
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.appflow import AppflowRunOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python_operator import PythonOperator
import json
import uuid
import sys
from airflow.models import Variable


# Setup Global Env Variables
dag_config = Variable.get("env", deserialize_json=True)


environment = dag_config["environment"]
source_system_id = "sharepoint"

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"

datasets = ["assumptions_index", "vg_sharepoint_lfm_iso_tracker_sheet"]

default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
}

glue_job_names = {
    "sharepoint_csv_xlsx_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "sharepoint_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}


flows = [
    "sharepoint_cp2vg_listing_library_0001",
    "sharepoint_cp2vg_listing_library_0002",
    "sharepoint_cp2vg_listing_library_0003"
]

def get_appflow_details(flow_name,**kwargs):

  app_flow_client = session.client('appflow')
  flow_details = app_flow_client.describe_flow(
    flowName=flow_name
  )
  bucket_name = flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketName']
  bucket_prefix = flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketPrefix']
  return bucket_name,bucket_prefix

def parse_key(key):
    # Gets the file name from the Key.
    try:
        parts = key.split('/')
        file_name = parts.pop()
        return file_name
    except:
        raise ValueError(f"Does not contain a valid S3 key :::: check {key}")

def create_partitioned_path_hive_style():
    # Get the current date and time
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    partitioned_path = f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"
    return partitioned_path


def get_execution_id(flow_name, **kwargs):

    app_flow_client = session.client('appflow')

    response = app_flow_client.describe_flow_execution_records(
        flowName=flow_name,
        maxResults=1
    )
    
    if response['flowExecutions']:
        execution_id = response['flowExecutions'][0]['executionId']
        print(f"Execution ID: {execution_id}")
        return execution_id
    else:
        print("No execution records found.")
        return None

def move_files_hive_folder_structure(flow_name,partitioned_path_hive_style,**kwargs):

    s3 = session.client('s3')

    bucket_name,bucket_prefix = get_appflow_details(flow_name)

    print(f"Bucket_name:::::: {bucket_name}")

    execution_id = get_execution_id(flow_name)

    print(f"Execution id:::::: {execution_id}")

    source_key = f"{bucket_prefix}{flow_name}/{execution_id}"

    partitioned_destination_key = f"{bucket_prefix}{partitioned_path_hive_style}"

    print(f"Bucket prefix :::: {bucket_prefix}")

    objects = []
    paginator = s3.get_paginator('list_objects_v2')

    pages = paginator.paginate(
        Bucket=bucket_name,
        Prefix=source_key
    )
    file_out_locations = []
    try:
        for page in pages:
            for obj in page['Contents']:
                objects.append(obj)
                for copy in objects:
                    file_name = parse_key(copy['Key'])
                    copy_source = {'Bucket': bucket_name, 'Key': copy['Key'], 'file_name': file_name}
                    if file_name == execution_id:
                        print("Found meta data file, skipping file {file_name}")
                        continue
                    print(f"Copying the file..... CopySource=/{bucket_name}/{copy_source['Key']} Key={partitioned_destination_key}{file_name}")
                    print(f"Targer the file..... Target=/{bucket_name}/{copy_source['Key']} Key={partitioned_destination_key}{file_name}")
                    s3.copy_object(Bucket=bucket_name, CopySource=f"/{bucket_name}/{copy_source['Key']}", Key=f"{partitioned_destination_key}{file_name}")
                    print(f"Deleting the file..... Key={bucket_name}/{copy_source['Key']}")
                    s3.delete_object(Bucket=bucket_name, Key=f"{bucket_name}/{copy_source['Key']}")
                    file_out_locations.append(f"s3://{bucket_name}/{partitioned_destination_key}{file_name}")

        if len(file_out_locations) > 0:
            return file_out_locations

    except Exception as e:
        error_message = f"Error in move function copying files from Appflow working directory: {str(e)}"
        print(error_message)
        sys.exit(1)



# Create DAG
with DAG(
    dag_id=f"sharepoint_csv_xlsx_{environment}_data_pipeline",
    schedule="0 0 * * *",
    tags=["sharepoint_csv_xlsx"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    p6_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')


    # file_path = create_partitioned_path_hive_style()

    file_path = PythonOperator(
        task_id="file_path",
        python_callable=create_partitioned_path_hive_style
    )

    @task_group(group_id="app_flow_tasks")
    def appflow_tasks():
        tasks = []
        for i in flows:
            run_flow = AppflowRunOperator(
                task_id=f"run_flow_{i}",
                flow_name=i,
                wait_for_completion = True        
            )

            files_moved = PythonOperator(
                task_id=f"move_files_{i}",
                python_callable=move_files_hive_folder_structure,
                op_kwargs={
                    "flow_name": i,
                    "partitioned_path_hive_style": file_path.output
                }
            )
            run_flow >> files_moved
            tasks.extend([run_flow, files_moved])
        return tasks
        # files_moved = move_files_hive_folder_structure(i, file_path)

    csv_xlsx_task = GlueJobOperator(
            task_id=f"sharepoint_csv_xlsx_parsing",
            job_name=glue_job_names["sharepoint_csv_xlsx_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"csv_xlsx#vg#{source_system_id}",
                "--function_name": "csv_xlsx#vg",
                "--connector_file_path" : f"vg/sharepoint/raw/{file_path.output}",
                "--start_date": execution_date,
                "--end_date": execution_date,
                },
            trigger_rule='none_failed'
        )


    raw_crawler = GlueCrawlerOperator(
        task_id=f"sharepoint_csv_xlsx_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-vg-sharepoint",
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
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_vg_sharepoint",
                    "--table_name": f"raw_{table}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["sharepoint_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": f"{source_system_id}_curated",
                    "--metadata_type": f"curated#vg_sharepoint#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    file_path >> appflow_tasks() >> csv_xlsx_task >> raw_crawler >> detect_schema_change() >> raw_to_curated()
