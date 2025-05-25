import datetime

from airflow.models.dag import DAG
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
import pendulum

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Setup Global Env Variables
ENVIRONMENT = environment
REGION = "ap-southeast-2"
source_system_id = "ped"
APPLICATION_NAME = "ped"
SOURCE_NAME = "ped"
entity_sourcing = ["pedprojects"]
default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{ENVIRONMENT}-genai-glue-job-ped-dataverse-workflow-api-sourcing"
}

# Create DAG
with DAG(
    dag_id=f"genai_ped_{ENVIRONMENT}_generic_data_pipeline",
    schedule="00 10 * * *",
    default_args=default_args,
    tags=["genai","ped"],
    start_date=datetime.datetime(2024, 5, 1,tzinfo=pendulum.timezone("Australia/Sydney")),
    catchup=False
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    #Add conncurency level to task group
    @task_group(group_id="ped_sourcing_entities")
    def ped_entity_sourcing():
        for entity_name in entity_sourcing:
            src_raw_initial = GlueJobOperator(
                task_id=f"{APPLICATION_NAME}_{SOURCE_NAME}_entity_sourcing",
                job_name=glue_job_names["api_sourcing_job"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--application_name": APPLICATION_NAME,
                    "--source_name": SOURCE_NAME,
                    "--function_name": entity_name,
                    "--start_date": execution_date,
                    "--end_date": execution_date
                    },
                )

    (
        ped_entity_sourcing()
     
    )
