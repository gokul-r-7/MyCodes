import os
import json
import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from pendulum import datetime

PROJECT_NAME = "vg"
SECRET_NAME = "worley-datalake-sydney-dev-dbt-vg_e3d-user20240716052702843600000003"

client = boto3.client('secretsmanager')

def get_secret(secret_name):
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret

secret_value = get_secret(SECRET_NAME)
username = secret_value["username"]
password = secret_value["password"]

profile_config = ProfileConfig(
    profile_name="redshift",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift-connection",
        profile_args={"schema": "vg_analytics", "user": username, "password": password}
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['DBT_VENV_PATH']}/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['DBT_PROJECT_PATH']}/{PROJECT_NAME}",
)


with DAG(
    dag_id="dbt_vg_dag",
    start_date=datetime(2024, 7, 15),
    schedule_interval="@once",
    catchup=False,
    tags=["dbt_vg"],
):

    dbt_task = DbtTaskGroup(
        execution_config=execution_config,
        profile_config=profile_config,
        project_config=project_config,
    )
