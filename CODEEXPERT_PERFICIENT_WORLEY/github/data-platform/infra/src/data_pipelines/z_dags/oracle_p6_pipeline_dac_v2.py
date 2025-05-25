import datetime
import pendulum
import os
import requests
import boto3
import json
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from boto3.dynamodb.conditions import Key
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)


environment = dag_config["environment"]

source_system_id = "oracle_p6"
curation_source_system_id = "curated_oracle_p6"

region = "ap-southeast-2"
metadata_tablename=f"worley-mf-sydney-{environment}-metadata-table"

max_project_concurrency = 1

# Datasets for Downstream project_control
project_control_dataset = Dataset("//oracle_p6/project_control/dac_integrated_model")


default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}

##########################
def custom_sort(key):
    return (0, key) if key.startswith('p') else (1, key)

def get_metadata_from_ddb(source_system_id: str, metadata_type: str, region: str, metadata_table_name: str) -> dict:
    dynamo_resource = boto3.resource("dynamodb", region_name=region)
    table = dynamo_resource.Table(metadata_table_name)
    
    try:
        response = table.query(
            KeyConditionExpression=Key("SourceSystemId").eq(source_system_id) & Key("MetadataType").eq(metadata_type)
        )
        items = response["Items"]
        if items:
            return items[0]  # Assuming you want the first item in the response
        else:
            return {}  # Return an empty dictionary if no items found
    except Exception as e:
        print(f"Error fetching metadata from DynamoDB: {e}")
        return {}  # Handle exception gracefully, return empty dictionary or handle as needed        
##########################

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_system_id + "#" + "airflowRunConfig"
# Read Metadata
metadata = get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=input_keys, region = "ap-southeast-2", metadata_table_name=metadata_tablename
)

region = metadata[environment]['region']
projects = [str(d) for d in metadata[environment]['Project_DAC']['Id']]  # Convert Decimal objects to strings
data_type = metadata['dev']['Project_DAC']['data_type']
data_type['project'] = dict(sorted(data_type['project'].items(), key=lambda x: custom_sort(x[0])))
datasets = list(data_type['project'].values())
datasets_global = list(data_type['global'].values())

glue_job_names = {
    "oracle_p6_sourcing_api": f"worley-datalake-sydney-{environment}-glue-job-p6-sourcing-json-api-data",
    "oracle_p6_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
}

# Create DAG
with DAG(
    dag_id=f"oracle_p6_{environment}_data_dac_pipeline_v2",
    schedule="0 2 1,15 * *",
    tags=["oracle_p6"],
    default_args=default_args,
    start_date=datetime.datetime(2024, 11, 17),
    catchup=False,
    max_active_runs = max_project_concurrency
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    #p6 SpreadAPIs have specific dataformat
    p6_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    @task_group(group_id="oracle_p6_global_sourcing")
    def oracle_ps_global_sourcing():
        """Task Group that runs the required steps to source Global level API Data"""
        for function_name in data_type['global'].keys():
            extract_api_global = GlueJobOperator(
                task_id=f"oracle_p6_sourcing_api_global_{function_name}",
                job_name=glue_job_names["oracle_p6_sourcing_api"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--source_name": source_system_id,
                    "--function_name": function_name,
                    "--project_id": "0",
                    "--data_type" : "global"
                },
            )

    @task_group(group_id="oracle_p6_project_sourcing_v2")
    def oracle_ps_project_sourcing(project_id: str):
        """Task Group that runs the required steps to source one ProjectID for project endpoint"""
        
        function_name = data_type['project']['project']
        extract_api_project = GlueJobOperator(
            task_id=f"oracle_p6_sourcing_api_project_{function_name}",
            job_name=glue_job_names["oracle_p6_sourcing_api"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_system_id,
                "--function_name": function_name,
                "--project_id": project_id,
                "--data_type" : "project"
            }
        )
    
    @task_group(group_id="oracle_p6_other_sourcing_v2")
    def oracle_ps_other_sourcing(project_id: str):
        """Task Group that runs the required steps to source one ProjectID"""

        for function_name, curated_function_name in data_type['project'].items():
            if (function_name != 'project') and ('spread' not in function_name.lower()):
                extract_api_project = GlueJobOperator(
                    task_id=f"oracle_p6_sourcing_api_project_{function_name}",
                    job_name=glue_job_names["oracle_p6_sourcing_api"],
                    region_name=region,
                    verbose=True,
                    wait_for_completion=True,
                    max_active_tis_per_dagrun=max_project_concurrency,
                    script_args={
                        "--source_name": source_system_id,
                        "--function_name": function_name,
                        "--project_id": project_id,
                        "--data_type" : "project"
                    }
                )

    @task_group(group_id="oracle_p6_spread_sourcing_v2")
    def oracle_ps_spread_sourcing(project_id: str):
        """Task Group that runs the required steps to source one ProjectID spread data"""

        for function_name, curated_function_name in data_type['project'].items():
            if ('spread' in function_name.lower()):
                extract_api_project = GlueJobOperator(
                    task_id=f"oracle_p6_sourcing_api_project_{function_name}",
                    job_name=glue_job_names["oracle_p6_sourcing_api"],
                    region_name=region,
                    verbose=True,
                    wait_for_completion=True,
                    max_active_tis_per_dagrun=max_project_concurrency,
                    script_args={
                        "--source_name": source_system_id,
                        "--function_name": function_name,
                        "--project_id": project_id,
                        "--data_type" : "project"
                    }
                )

    raw_crawler = GlueCrawlerOperator(
        task_id=f"oracle_p6_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-project-control-oracle-p6-raw"
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_oracle_p6_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-project-control-oracle-p6-raw",
    )

    @task_group(group_id=f"curation_global")
    def raw_to_curated_global():
        for table in datasets_global:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["oracle_p6_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--source_system_id": curation_source_system_id,
                    "--metadata_type": f"curated#oracle_p6#{table}#job#iceberg"
                },
            )
    
    @task_group(group_id=f"curation")
    def raw_to_curated(project_id: str):
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["oracle_p6_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--source_system_id": curation_source_system_id,
                    "--metadata_type": f"curated#oracle_p6#{table}#job#iceberg",
                    "--project_id": project_id
                },
            )
    
    
    run_project_control_models_dag = BashOperator(
        task_id='run_project_control_models_dag',
        bash_command='echo "run project_control models dag"',
        outlets=[project_control_dataset]
    )
    
    [oracle_ps_global_sourcing(), oracle_ps_project_sourcing.expand(project_id=projects)] >> oracle_ps_other_sourcing.expand(project_id=projects) >> oracle_ps_spread_sourcing.expand(project_id=projects) >> raw_crawler >> raw_crawler_sensor >> [raw_to_curated_global(),raw_to_curated.expand(project_id=projects)] >> run_project_control_models_dag
