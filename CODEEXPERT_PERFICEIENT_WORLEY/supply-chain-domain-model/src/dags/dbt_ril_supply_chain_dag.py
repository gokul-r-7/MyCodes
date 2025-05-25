import os
import boto3
import json
from airflow import DAG
from airflow.models import Variable
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from pendulum import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.datasets import Dataset
from botocore.config import Config
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.exceptions import AirflowException

PROJECT_NAME = "supply_chain"

supply_chain_dataset = Dataset("//erm/supply_chain/domain_integrated_model")

gsr_dataset = Dataset("//supply_chain/domain_integrated_model/global_standard_reporting")

dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)
dag_config_env = Variable.get("env", deserialize_json=True)
environment = dag_config_env["environment"]

SECRET_NAME = dag_config["supply_chain"]

payload = json.dumps({
    "dag_name": "dbt_supply_chain_ril_{environment}_dag",
    "domain": "supply_chain"
})

client = boto3.client('secretsmanager')

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)

def get_secret(secret_name):
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise

    return json.loads(get_secret_value_response["SecretString"])

secret_value = get_secret(SECRET_NAME)
username = secret_value["username"]
password = secret_value["password"]


def check_dbt_failures(**kwargs):
    if kwargs['ti'].state == 'failed':
        raise AirflowException('Failure in dbt task group')

profile_config = ProfileConfig(
    profile_name="redshift",
    target_name="supply_chain",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_supply_chain_connection",
        profile_args={"schema": "domain_integrated_model", 
                      "user": username, "password": password}
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['DBT_VENV_PATH']}/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['DBT_PROJECT_PATH']}/{PROJECT_NAME}",
)

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "catchup": False,
}

with DAG(
    dag_id=f"dbt_supply_chain_ril_{environment}_dag",
    start_date=datetime(2024, 9, 18),
    schedule=[supply_chain_dataset],
    catchup=False,
    tags=["dbt"],
    default_args=default_args,
    max_active_runs=1
):

    ril_dbt_task = DbtTaskGroup(
        group_id="ril_dbt_task",
        execution_config=execution_config,
        profile_config=profile_config,
        project_config=project_config,
        operator_args={
            "install_deps": True,
        },
        render_config=RenderConfig(
            select=["models/Ril/ril_min_drl_list.sql"],
            load_method=LoadMode.DBT_LS
        )
    )

    ril_check = PythonOperator(
        task_id='ril_check',
        python_callable=check_dbt_failures,
        provide_context=True,
    )

    apply_redshift_permissions = LambdaInvokeFunctionOperator(
        task_id="apply_redshift_permissions",
        function_name="worley-rbac-redshift-setup",
        payload=payload,
        trigger_rule='all_done'
    )

    ril_dbt_task >> ril_check >> apply_redshift_permissions
    
