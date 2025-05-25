import datetime
import pendulum
import os
import boto3
from boto3.dynamodb.conditions import Key
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)

# Datasets for Downstream Construction
construction_dataset = Dataset("//o3/construction/domain_integrated_model")
jplus_o3_dataset = Dataset("//o3/jplus/o3_import")

# Setup Global Env Variables
environment = dag_config["environment"]
# environment = 'dev'
region = "ap-southeast-2"
source_system_id = "o3"
curation_source_system_id = "curated_o3"
metadata_tablename = f"worley-mf-sydney-{environment}-metadata-table"
max_project_concurrency = 1
max_dag_concurrency = 1
masking_metadata_table_name = f"worley-mf-rbac-sydney-{environment}-database-permissions-metadata"
database_name = f"worley_datalake_sydney_{environment}_glue_catalog_database_construction_o3_raw"

############ get o3 airflow metadata from dynamo db #####################


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
        return {}
#########################################################################


# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_system_id + "#" + "project_config"

# Read Metadata
metadata = get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=input_keys, region="ap-southeast-2", metadata_table_name=metadata_tablename
)

region = metadata[environment]['region']
projects = [str(d) for d in metadata[environment]['Project']
            ['Id']]  # Convert Decimal objects to strings
api_endpoints = metadata[environment]['Project']['Endpoints']
datasets = api_endpoints['type1']+api_endpoints['type2']+api_endpoints['type3']

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}
glue_job_names = {
    "o3_sourcing_job": f"worley-datalake-sydney-{environment}-glue-job-o3-workflow-api-sourcing",
    "o3_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic"
}

# Create DAG
with DAG(
    dag_id=f"o3_{environment}_generic_data_pipeline",
    schedule="0 0 * * *",
    default_args=default_args,
    tags=["o3"],
    start_date=datetime.datetime(2024, 8, 20),
    catchup=False,
    max_active_runs=max_dag_concurrency
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"
    execution_timestamp = "{{ts}}"

    @task_group(group_id="o3_initial_sourcing")
    def o3_intial_sourcing(project_id):
        source_name = "o3"
        # for project_id in projects:
        for api_type, endpoints in api_endpoints.items():
            for endpoint in endpoints:
                safe_endpoint = endpoint.replace('/', '_').replace('-', '_')
                src_raw_initial = GlueJobOperator(
                    task_id=f"{source_name}_{safe_endpoint}_initial_sourcing",
                    job_name=glue_job_names["o3_sourcing_job"],
                    region_name=region,
                    verbose=True,
                    wait_for_completion=True,
                    max_active_tis_per_dagrun=max_project_concurrency,
                    script_args={
                        "--ProjectId": project_id,
                        "--source_name": source_name,
                        "--function_name": "extract_api",
                        "--EndPoint": endpoint,
                        "--ApiType": api_type,
                        "--metadata_type": f"raw#o3#extract_api",
                        "--masking_metadata_table_name" : masking_metadata_table_name,
                        "--database_name" : database_name
                    },
                )

    raw_crawler = GlueCrawlerOperator(
        task_id=f"o3_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-construction-o3-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_o3_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-construction-o3-raw",
    )

    @task_group(group_id=f"schema_detection")
    def detect_schema_change():
        for table in datasets:
            normalized_table_name = table.replace('/', '_').replace('-', '_')
            schema_change = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{normalized_table_name}",
                job_name=glue_job_names["schema_change_detection"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_construction_{source_system_id}_raw",
                    "--table_name": f"raw_{normalized_table_name}"
                },
            )

    @task_group(group_id=f"curation")
    def raw_to_curated():
        for table in datasets:
            table = '_'.join(table.split('/'))
            raw_curated = GlueJobOperator(
                task_id=f"cur_{source_system_id}_{table.replace('/', '_').replace('-', '_')}",
                job_name=glue_job_names["o3_raw_curated"],
                region_name=region,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--source_system_id": curation_source_system_id,
                    "--metadata_type": f"curated#o3#{table}#job#iceberg",
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                },
            )

    run_construction_models_dag = BashOperator(
        task_id='run_construction_models_dag',
        bash_command=(
            'echo "run construction models dag"'
            f"--conf '{{\"source\": \"{source_system_id}\"}}'"
        ),
        outlets=[construction_dataset]
    )

    run_jplus_o3_import_dag = BashOperator(
        task_id='run_jplus_o3_import_dag',
        bash_command=(
            'echo "run jplus_o3_import models dag"'
            f"--conf '{{\"source\": \"{source_system_id}\"}}'"
        ),
        outlets=[jplus_o3_dataset]
    )

    o3_intial_sourcing.expand(project_id=projects) >> raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> [run_construction_models_dag, run_jplus_o3_import_dag]
