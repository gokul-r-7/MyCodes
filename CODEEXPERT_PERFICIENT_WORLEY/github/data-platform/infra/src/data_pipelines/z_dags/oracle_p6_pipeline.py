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

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)

# Datasets for Downstream Project Control
project_control_dataset = Dataset("//oracle_p6/project_control/domain_integrated_model")

environment = dag_config["environment"]

source_system_id = "oracle_p6"
curation_source_system_id = "curated_oracle_p6"

region = "ap-southeast-2"
metadata_tablename = f"worley-mf-sydney-{environment}-metadata-table"

max_project_concurrency = 2


default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}

##########################


def get_metadata_from_ddb(source_system_id: str, metadata_type: str, region: str, metadata_table_name: str) -> dict:
    dynamo_resource = boto3.resource("dynamodb", region_name=region)
    table = dynamo_resource.Table(metadata_table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key("SourceSystemId").eq(
                source_system_id) & Key("MetadataType").eq(metadata_type)
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
input_keys = "api#" + source_system_id + "#" + "airflow_config"
# Read Metadata
metadata = get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=input_keys, region="ap-southeast-2", metadata_table_name=metadata_tablename
)

region = metadata[environment]['region']
projects = [str(d) for d in metadata[environment]['Project']
            ['Id']]  # Convert Decimal objects to strings

# projects = ["68930"]
activity_spread_function = "activity_spread"
resource_spread_function = "resource_assignment_spread"

datasets = [
    "activitycode",
    "activitycodeassignment",
    "activitycodetype",
    "activityspread",
    "eps",
    "project_activity",
    "project_activitycode",
    "project_activitycodetype",
    "project_resourceassignment",
    "project",
    "resource",
    "resourcespread",
]
glue_job_names = {
    "oracle_p6_spread": f"worley-datalake-sydney-{environment}-glue-job-p6-extract-spread-data",
    "oracle_p6_extract_api": f"worley-datalake-sydney-{environment}-glue-job-p6-extract-export-api-data",
    "oracle_p6_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
}

# Create DAG
with DAG(
    dag_id=f"oracle_p6_{environment}_data_pipeline",
    schedule="@once",
    tags=["oracle_p6"],
    default_args=default_args,
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()

    # p6 SpreadAPIs have specific dataformat
    p6_execution_date = today.strftime('%Y-%m-%dT%H:%M:%S')

    @task_group(group_id="oracle_p6_sourcing")
    def oracle_ps_sourcing(project_id: str):
        """Task Group that runs the required steps to source one ProjectID"""

        extract_api = GlueJobOperator(
            task_id="oracle_p6_extract_api",
            job_name=glue_job_names["oracle_p6_extract_api"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": "1",
                "--project_id": project_id,
                "--start_date": execution_date,
                "--end_date": execution_date,
            },
        )

        activity_spread = GlueJobOperator(
            task_id="oracle_p6_activity_spread",
            job_name=glue_job_names["oracle_p6_spread"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_system_id,
                "--function_name": activity_spread_function,
                "--metadata_type": "2",
                "--project_id": project_id,
                "--start_date": p6_execution_date,
                "--end_date": p6_execution_date,
            },
        )

        resource_assignment = GlueJobOperator(
            task_id="oracle_p6_resource_assignment_spread",
            job_name=glue_job_names["oracle_p6_spread"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            max_active_tis_per_dagrun=max_project_concurrency,
            script_args={
                "--source_name": source_system_id,
                "--function_name": resource_spread_function,
                "--metadata_type": "3",
                "--project_id": project_id,
                "--start_date": p6_execution_date,
                "--end_date": p6_execution_date,
            },
        )

        extract_api >> [activity_spread, resource_assignment]

    raw_crawler = GlueCrawlerOperator(
        task_id=f"oracle_p6_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-raw-oracle-p6",
        },
        poll_interval=5,
        wait_for_completion=False,
    )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table}",
                job_name=glue_job_names["oracle_p6_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": curation_source_system_id,
                    "--metadata_type": f"curated#oracle_p6#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    run_project_control_models_dag = BashOperator(
        task_id='run_project_control_models_dag',
        bash_command='echo "run project control models dag"',
        outlets=[project_control_dataset]
    )

    oracle_ps_sourcing.expand(project_id=projects) >> raw_crawler >> raw_to_curated() >> run_project_control_models_dag
