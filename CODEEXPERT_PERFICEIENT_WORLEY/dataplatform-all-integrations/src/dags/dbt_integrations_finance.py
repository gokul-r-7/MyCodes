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

# Define project name
project_name = "integrations"
# Retrieve secret name variable
dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)

secret_name = dag_config["integrations"]

# Define payload to pass to lambda for permissions
payload = json.dumps({
          "schema": "finance", 
          "obt_name": ["gbs_bp_daedalus","gbs_bp_program","gbs_nobian","gbs_project_client","gbs_project_task","gbs_project","gbs_shell_program","gbs_sppt","gbs_susan_timesheet_data","gbs_vendor_software_usage_analytics"], 
          "db": "integrations",
          "dag_name": "dbt_integrations_finance"
          })

sm_client = boto3.client('secretsmanager')

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)


def get_secret(secret_name):
    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name)
    except Exception as e:
        raise

    return json.loads(get_secret_value_response["SecretString"])


secret_value = get_secret(secret_name)
username = secret_value["username"]
password = secret_value["password"]


def invoke_lambda_function(lambda_function_name, payload):
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=payload
        )
        print('Response--->', response)
    except Exception as e:
        raise


profile_config = ProfileConfig(
    profile_name="redshift",
    target_name="integrations",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_integrations_connection",
        profile_args={"schema": "finance",
                      "user": username, "password": password}
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['DBT_VENV_PATH']}/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['DBT_PROJECT_PATH']}/{project_name}",
)


#documentcontrol_dataset = Dataset("//document_control/domain_integrated_model/global_standard_reporting")

with DAG(
    dag_id="dbt_integrations_finance",
    start_date=datetime(2024, 9, 18),
    #schedule_interval="@once",
    #schedule=[documentcontrol_dataset],
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
            select=["tag:finance"],
            load_method=LoadMode.DBT_LS
        )
    )

    apply_redshift_permissions = PythonOperator(
        task_id="apply_redshift_permissions",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-rbac-redshift-apply-database-permissions', payload],
        trigger_rule='all_done'
    )

    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )

    
    audit_dbt_task >> dbt_task >> apply_redshift_permissions >> sns_notification_for_failure

