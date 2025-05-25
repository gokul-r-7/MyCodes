import datetime
import datetime
import pendulum
import sys
import boto3
from botocore.exceptions import ClientError
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.decorators import task_group, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
import pendulum

# Retrieve variables
dag_config = Variable.get("env", deserialize_json=True)
environment = dag_config["environment"]

# Datasets for Downstream CSP
customer_csp_dataset = Dataset(
    "//customer/csp/curation")

# Setup Global Env Variables
ENVIRONMENT = environment
REGION = "ap-southeast-2"
source_system_id = "csp_salesforce"
APPLICATION_NAME = "csp"
SOURCE_NAME = "salesforce"
entity_sourcing = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
tables = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
aurora_tables = ["account","account_group__c","contact","jacobs_project__c","multi_office_split__c","unit__c","opportunity", "b_p_request__c", "user"]
max_project_concurrency = 5
default_args = {
    "owner": "worley",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
}
glue_job_names = {
    "api_sourcing_job": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-csp-salesforce-workflow-api-sourcing",
    "csp_salesforce_raw_curated": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-raw-to-curated-generic",
    "schema_change_detection": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-schema-change-detection-generic",
    "csp_salesforce_cur_to_aurora": f"worley-datalake-sydney-{ENVIRONMENT}-glue-job-iceberg-to-aurora-generic"
}

# Create DAG
with DAG(
    dag_id=f"csp_salesforce_{ENVIRONMENT}_delta_data_pipeline",
    schedule="30 3-23/2 * * *",
    default_args=default_args,
    tags=["csp_salesforce"],
    start_date=datetime.datetime(2024, 5, 1,tzinfo=pendulum.timezone("Australia/Sydney")),
    catchup=False
) as dag:

    # Gets current execution date
    execution_date = "{{logical_date}}"

    #Add conncurency level to task group
    @task_group(group_id="csp_salesforce_sourcing_entities")
    def csp_salesforce_sourcing_entity():
        for entity_name in entity_sourcing:
            src_raw_initial = GlueJobOperator(
                task_id=f"{APPLICATION_NAME}_{SOURCE_NAME}_{entity_name}_entity_sourcing",
                job_name=glue_job_names["api_sourcing_job"],
                region_name=REGION,
                verbose=True,
                wait_for_completion=True,
                max_active_tis_per_dagrun=max_project_concurrency,
                script_args={
                    "--application_name": APPLICATION_NAME,
                    "--source_name": SOURCE_NAME,
                    "--function_name": entity_name,
                    "--start_date": execution_date,
                    "--end_date": execution_date,
                    "--load_type": f"I"
                    },
                )

    run_csp_curation = BashOperator(
        task_id='run_customer_csp_curation',
        bash_command='echo "run customer csp curation"',
        outlets=[customer_csp_dataset]
    )

    (
        csp_salesforce_sourcing_entity() >> run_csp_curation
     
    )
 