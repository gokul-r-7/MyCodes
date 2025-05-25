import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Retrieve variables from Airflow
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]
region = "ap-southeast-2"

def validate_parameters(**context):
    """
    Validate that all required parameters are present and not empty
    """

    params_dict = context['params']
    required_params = ['source_system_id', 'metadata_type', 'operation_type']

    missing_params = []
    empty_params = []
    
    for param in required_params:
        if param not in params_dict:
            missing_params.append(param)
        elif not params_dict[param] or params_dict[param].strip() == '':
                empty_params.append(param)  
    
    error_messages = []
    if missing_params:
        error_messages.append(f"Missing required parameters: {', '.join(missing_params)}")
    if empty_params:
        error_messages.append(f"Following parameters cannot be empty: {', '.join(empty_params)}")
    
    if error_messages:
        raise AirflowException('\n'.join(error_messages))
    
    # Log successful validation
    print("Parameter validation successful:")
    for param in required_params:
        print(f"{param}: {params_dict[param]}")

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Create DAG
with DAG(
    dag_id=f"dataops_{environment}_generic_pipeline", #aconex__us_instance_data_pipeline
    schedule="@once",
    default_args=default_args,
    tags=["dataops"],
    start_date=datetime.datetime(2024, 6, 27),
    catchup=False,
    params = {
        "source_system_id": Param("", type="string"),
        "metadata_type": Param("", type="string"),
        "operation_type": Param("", type="string"),
    },
) as dag:
  
        # Parameter validation task
    validate_params_task = PythonOperator(
        task_id='validate_parameters',
        python_callable=validate_parameters,
        provide_context=True
    )
    
   
    dataops_task = GlueJobOperator(
                  task_id=f"dataops_task",
                  job_name=f"worley-datalake-sydney-{environment}-glue-job-dataops-utlity-generic",
                  region_name=region,
                  verbose=True,
                  wait_for_completion=True,
                  script_args={
                      "--source_system_id": '{{ params["source_system_id"] }}',
                      "--metadata_type": '{{ params["metadata_type"] }}',
                      "--operation_type": '{{ params["operation_type"] }}'
                  },
        )
    
    dataops_utility_completion_task = BashOperator(
        task_id='dataops_utility_completion_task',
        bash_command=(
            'echo "completed running DataOps utility"'
        )
    )

    validate_params_task >> dataops_task >> dataops_utility_completion_task