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

# TODO Variable to pass to dbt project - date
# execution_date = "2024-09-02"

# Define project name
PROJECT_NAME = "document_control"

# Dataset for Upstream Sourcing
aconex_dataset = Dataset("//aconex/document_control/domain_integrated_model")
hexagon_dataset = Dataset("//hexagon/document_control/domain_integrated_model")

# Datasets for Downstream GSR
doc_control_gsr_dataset = Dataset("//document_control/domain_integrated_model/global_standard_reporting")

# Retrieve secret name variable
dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)
dag_config_env = Variable.get("env", deserialize_json=True)
environment = dag_config_env["environment"]

# SECRET_NAME = dag_config["document_control"]

# client = boto3.client('secretsmanager')

# environment_config = Variable.get("env", deserialize_json=True)
# environment = environment_config["environment"]


config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

# Need to send the domain infor for lambda for better control
payload = json.dumps({
    "dag_name": "dbt_document_control_dag",
    "domain": "document_control"
})

lambda_client = boto3.client('lambda',
                            config=config)

# def get_secret(secret_name):
#     try:
#         get_secret_value_response = client.get_secret_value(SecretId=secret_name)
#     except Exception as e:
#         raise
#     return json.loads(get_secret_value_response["SecretString"])

# secret_value = get_secret(SECRET_NAME)
# username = secret_value["username"]
# password = secret_value["password"]


def check_dbt_failures(**kwargs):
    if kwargs['ti'].state == 'failed':
        raise AirflowException('Failure in dbt task group')
    
profile_config = ProfileConfig(
    profile_name=f"dbt_{PROJECT_NAME}",
    target_name=environment,
    profiles_yml_filepath=f"{os.environ['DBT_PROJECT_PATH']}/{PROJECT_NAME}/profiles/profiles.yml"
    )

execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['DBT_VENV_PATH']}/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['DBT_PROJECT_PATH']}/{PROJECT_NAME}",
    # dbt_vars={"role_groups": execution_date}
)

with DAG(
    dag_id="dbt_document_control_dag",
    start_date=datetime(2024, 10, 10),
    schedule=[aconex_dataset, hexagon_dataset],
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

    dbt_check = PythonOperator(
        task_id='dbt_check', 
        python_callable=check_dbt_failures,
        provide_context=True,
    )

    sns_notification_for_failure_for_modelling_task_group = LambdaInvokeFunctionOperator(
        task_id="sns_notification_for_failure_for_modelling_task_group",
        function_name="worley-data-modelling-sns-notification",
        payload=payload,
        trigger_rule='one_failed'
    )

    # apply_redshift_permissions = LambdaInvokeFunctionOperator (
    #     task_id="apply_redshift_permissions",
    #     function_name="worley-rbac-redshift-setup",
    #     payload=payload,
    #     trigger_rule='all_done'
    # )
    

    doc_control_dbt_task = DbtTaskGroup(
        group_id="doc_control_dbt_task",
        execution_config=execution_config,
        profile_config=profile_config,
        project_config=project_config,
        operator_args={
            "install_deps": True,
            # install any necessary dependencies before running any dbt command
        },
        render_config= RenderConfig(
            exclude=["tag:audit","tag:snowflake_models"],
            load_method=LoadMode.DBT_LS
        )
    )
    
    doc_control_run_gsr_models_dag = BashOperator(
        task_id='doc_control_run_gsr_models_dag',
        bash_command='echo "document_control run gsr models dag"',
        outlets=[doc_control_gsr_dataset],
        trigger_rule='all_done'
    )

    sns_notification_for_failure_for_other_tasks = LambdaInvokeFunctionOperator(
        task_id="sns_notification_for_failure_for_other_tasks",
        function_name="worley-data-modelling-sns-notification",
         payload=payload,
        trigger_rule='one_failed'
    )
     
    audit_dbt_task >> doc_control_dbt_task >> dbt_check >> sns_notification_for_failure_for_modelling_task_group  >> doc_control_run_gsr_models_dag >> sns_notification_for_failure_for_other_tasks
    
    # [doc_control_dbt_task, apply_redshift_permissions] >> doc_control_run_gsr_models_dag >> sns_notification_for_failure