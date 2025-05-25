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

# Define project name
project_name = "construction"

# Dataset for Upstream Sourcing
construction_dataset = Dataset("//o3/construction/domain_integrated_model")

# Datasets for Downstream GSR
gsr_dataset = Dataset("//construction/domain_integrated_model/global_standard_reporting")

# Retrieve secret name variable
dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)

secret_name = dag_config["construction"]

payload = json.dumps({
            "dag_name": "dbt_construction_fts_dag",
            "domain": "construction"
          })

sm_client = boto3.client('secretsmanager')

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)


# def get_secret(secret_name):
#     try:
#         get_secret_value_response = sm_client.get_secret_value(
#             SecretId=secret_name)
#     except Exception as e:
#         raise

#     return json.loads(get_secret_value_response["SecretString"])


# secret_value = get_secret(secret_name)
# username = secret_value["username"]
# password = secret_value["password"]

PROJECT_NAME = "construction"
environment_config = Variable.get("env", deserialize_json=True)
environment = environment_config["environment"]


DBT_PROJECT_PATH = os.environ.get("DBT_CONSTRUCTION_PROJECT_PATH", "/usr/local/airflow/dags/dbt").strip()

profile_config = ProfileConfig(
    profile_name=f"dbt_{PROJECT_NAME}",
    target_name=environment,
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/{PROJECT_NAME}/profiles/profiles.yml"
    )

# profile_config = ProfileConfig(
#     profile_name="redshift",
#     target_name="construction",
#     profile_mapping=RedshiftUserPasswordProfileMapping(
#         conn_id="redshift_construction_connection",
#         profile_args={"schema": "domain_integrated_model",
#                       "user": username, "password": password}
#     ),
# )
 
def check_dbt_failures(**kwargs):
    if kwargs['ti'].state == 'failed':
        raise AirflowException('Failure in dbt task group')
    
execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['DBT_VENV_PATH']}/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

project_config = ProjectConfig(
    dbt_project_path=f"{DBT_PROJECT_PATH}/{PROJECT_NAME}",
)

with DAG(
    dag_id="dbt_construction_fts_dag",
    start_date=datetime(2024, 9, 18),
    schedule=[construction_dataset],
    catchup=False,
    tags=["dbt"],
):

    audit_dbt_task = DbtTaskGroup(
        group_id="audit_dbt_task",
        execution_config=execution_config,
        profile_config=profile_config,
        project_config=project_config,
        operator_args={
            "install_deps": True,
        },
        render_config= RenderConfig(
            select=["tag:audit"],
            load_method=LoadMode.DBT_LS
        )
    )

    dbt_task = DbtTaskGroup(
        group_id="dbt_task",
        execution_config=execution_config,
        profile_config=profile_config,
        project_config=project_config,
        operator_args={
            "install_deps": True,
            # install any necessary dependencies before running any dbt command
        },
        render_config= RenderConfig(
            exclude=["tag:audit","tag:o3"],
            load_method=LoadMode.DBT_LS
        )
    )
    
    
    dbt_check = PythonOperator(
        task_id='dbt_check', 
        python_callable=check_dbt_failures,
        provide_context=True,
    )

    apply_redshift_permissions = LambdaInvokeFunctionOperator (
        task_id="apply_redshift_permissions",
        function_name="worley-rbac-redshift-setup",
        payload=payload,
        trigger_rule='all_done'
    )
    
    run_gsr_models_dag = BashOperator(
        task_id='run_gsr_models_dag',
        bash_command='echo "run gsr models dag"',
        outlets=[gsr_dataset]
    )
    
    sns_notification_for_failure_for_other_tasks = LambdaInvokeFunctionOperator(
        task_id="sns_notification_for_failure_for_other_tasks",
        function_name="worley-data-modelling-sns-notification",
        payload=payload,
        trigger_rule='one_failed'
    )
    
    audit_dbt_task >> dbt_task >> apply_redshift_permissions >> run_gsr_models_dag >> sns_notification_for_failure_for_other_tasks