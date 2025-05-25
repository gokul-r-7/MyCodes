import datetime

from airflow.models.dag import DAG
from airflow.decorators import task_group
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Setup Global Env Variables
ENVIRONMENT = environment
REGION = "ap-southeast-2"
APPLICATION_NAME = "Sharepoint"
SOURCE_NAME = "sharepoint_gph"
FUNCTION_NAME="comprimo_pursuits"
default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{ENVIRONMENT}-genai-glue-job-sharepoint-workflow-api-sourcing"
}

# Create DAG
with DAG(
    dag_id=f"genai_sharepoint_gph_{ENVIRONMENT}_comprimo_pursuits_pipeline",
    schedule="00 10 1 * *",
    default_args=default_args,
    tags=["genai","sharepoint","gph"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    #Add conncurency level to task group
    @task_group(group_id="sharepoint_sourcing")
    def sharepoint_sourcing():
        src_raw_initial = GlueJobOperator(
                task_id=f"{APPLICATION_NAME}_{SOURCE_NAME}_entity_sourcing",
                job_name=glue_job_names["api_sourcing_job"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                script_args={
                    "--application_name": APPLICATION_NAME,
                    "--source_name": SOURCE_NAME,
                    "--function_name": FUNCTION_NAME,
                    "--start_date": execution_date,
                    "--end_date": execution_date
                    },
                )

    (
        sharepoint_sourcing()
     
    )
