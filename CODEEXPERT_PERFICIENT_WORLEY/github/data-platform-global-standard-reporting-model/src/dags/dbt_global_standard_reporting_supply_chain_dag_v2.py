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
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

# Define project name
project_name = "global_standard_reporting_v2"
# Retrieve secret name variable
dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)

secret_name = dag_config["global_standard_reporting"]

# Define payload to pass to lambda for permissions
payload = json.dumps({
         "schema": "supply_chain", 
         "obt_name":["material_status_obt","invoice_status_obt","buyer_analysis_obt",
                       "category_management_obt","support_info_obt","spend_analysis_obt",
                       "lead_time_obt" ,"purchasing_status_report_obt","psr_s_curve_obt",
                       "fact_esr_data_obt","fact_po_header_obt",
                       "fact_tmr_data_obt","exp_material_status_obt","gantt_esr_data_obt"], 
         "db": "global_standard_reporting",
         "dag_name": "dbt_global_standard_reporting_supply_chain_dag_v2"
         })

sm_client = boto3.client('secretsmanager')


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

 

profile_config = ProfileConfig(
   profile_name="redshift",
   target_name="global_standard_reporting",
   profile_mapping=RedshiftUserPasswordProfileMapping(
       conn_id="redshift_serverless_global_standard_reporting_connection",
       profile_args={"schema": "supply_chain",
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


def check_dbt_failures(**kwargs):
   if kwargs['ti'].state == 'failed':
       raise AirflowException('Failure in dbt task group')
   
#documentcontrol_dataset = Dataset("//supply_chain/domain_integrated_model/global_standard_reporting")

with DAG(
   dag_id="dbt_global_standard_reporting_supply_chain_dag_v2",
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
           select=["tag:supply_chain"],
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
   
   apply_redshift_permissions = LambdaInvokeFunctionOperator (
       task_id="apply_redshift_permissions",
       function_name="worley-rbac-redshift-apply-database-permissions",
       payload=payload,
       trigger_rule='all_done'
   )

 

   sns_notification_for_failure_for_other_tasks = LambdaInvokeFunctionOperator(
       task_id="sns_notification_for_failure_for_other_tasks",
       function_name="worley-data-modelling-sns-notification",
       payload=payload,
       trigger_rule='one_failed'
   )
   
   audit_dbt_task >> dbt_task >> dbt_check >> sns_notification_for_failure_for_modelling_task_group >> apply_redshift_permissions >> sns_notification_for_failure_for_other_tasks