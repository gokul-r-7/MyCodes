import datetime
import pendulum
import os
import boto3
import requests
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import json
from pytz import timezone
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
import json
from airflow.operators.python_operator import PythonOperator
from botocore.config import Config
from airflow.exceptions import AirflowFailException
import logging

# Retrieve variables
dag_config = Variable.get("assurance_sftp_variables_config", deserialize_json=True)
environment = dag_config["environment"]
region_full_name = dag_config["region_full_name"]

sftp_host = dag_config["sftp_host"]
bucket_suffix = dag_config["bucket_suffix"]

# Setup Global Env Variables
source_system_id = "assurance"
function_name = "hse"
assurance_dataset = Dataset("//assurance/health_safety_environment/domain_integrated_model")

# Create a session object
session = boto3.Session()
# Get the current region
region = session.region_name
#region = "ap-southeast-2"

default_args = {
    "owner": "worley",
    "depends_on_past": True,
    "max_active_runs" : 1,
    "catchup" : False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=3),
}

#added as part of SNS notification
payload = json.dumps({
          "dag_name": f"assurance_csv_{environment}_data_pipeline"
          })

config = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True
)

lambda_client = boto3.client('lambda',
                             config=config)


def invoke_lambda_function(lambda_function_name,payload):
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=payload            
        )
        print('Response--->', response)
        logging.error("SNS notification got triggered as one of the task failed.Hence mark main dag as failied")
        raise AirflowFailException("A task has failed, marking the DAG as failed.")
    except Exception as e:
        raise

datasets = ["action", "assessment", "event", "geography", "hazard_category", "hazard_identification", "hours", "injury", "leadership_activity", "person", "project", "system_time_period", "worley_reporting_region", "action_action_team_members", "action_plan", "action_plan_action_team_members", "action_progress", "assessment_assessment_team_members", "assessment_engineering_discipline", "assessment_finding", "assessment_plan", "assessment_plan_plan_team_members", "assessment_questionnaire", "business_line", "business_process", "company", "ems_business_process", "entity", "entity_obligation_involved_persons", "event_all_consequences_of_the_event", "event_consequence_asset", "event_consequence_environmental", "event_consequence_environmental_contaminants_contaminant_type", "event_consequence_environmental_flora_fauna_rarity", "event_consequence_environmental_type_of_ecological_loss", "event_consequence_motor_vehicle_accident", "event_consequence_motor_vehicle_accident_weather_conditions", "event_consequence_near_miss", "event_consequence_near_miss_agency", "event_consequence_near_miss_contributing_factors", "event_consequence_near_miss_environmental_conditions", "event_consequence_near_miss_possible_consequences_of_the_event", "event_consequence_quality", "event_consequence_quality_engineering_discipline", "event_consequence_quality_product_ncr_causal_factor", "event_consequence_quality_product_ncr_causal_factor_category", "event_consequence_quality_product_ncr_causal_factors", "event_consequence_quality_quality_event_management", "event_consequence_security", "event_consequence_security_type_of_security_event", "event_employment_category", "event_formal_root_cause_analysis", "event_formal_root_cause_analysis_asset_and_resources_driven", "event_formal_root_cause_analysis_environment_driven", "event_formal_root_cause_analysis_materials_driven", "event_formal_root_cause_analysis_people_driven", "event_formal_root_cause_analysis_process_driven", "event_formal_root_cause_analysis_systems_driven", "event_investigation", "event_investigation_investigation_team", "event_investigation_sequence_of_events", "geography_codes", "injury_agency", "injury_contributing_factors", "injury_environmental_conditions", "injury_mechanism", "injury_nature_and_body_part", "injury_nature_and_body_part_side_of_body", "injury_number_of_days_lost", "injury_osha_recordable_criteria_met", "leadership_activity_activity_delivery_method", "leadership_activity_conversation_topics", "person_business_line", "person_geography", "person_obligation_business_processes", "person_project", "person_subsector", "product_ncr_causal_factor", "project_hse_advisor_representative_team", "project_project_manager_team", "project_recordable_and_hpi_email_group", "questionnaire", "scurve", "subsector", "unit_of_measure", "unit_of_measure_conversion"]


glue_job_names = {
    "assurance_csv_parsing": f"worley-datalake-sydney-{environment}-glue-job-csvxlsx-data",
    "schema_change_detection": f"worley-datalake-sydney-{environment}-glue-job-schema-change-detection-generic",
    "assurance_raw_curated": f"worley-datalake-sydney-{environment}-glue-job-raw-to-curated-generic"
}

# Get current date and time and return partition path
def create_partitioned_path_hive_style():
    now=datetime.datetime.now(timezone('Australia/Sydney'))
    year=now.year
    month=now.month
    day=now.day
    hour=now.hour
    minute=now.minute
    partitioned_path=f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"
    return partitioned_path

# Create DAG
with DAG(
    dag_id=f"assurance_csv_{environment}_data_pipeline",
    schedule = "30 2 * * *",  # Every day at 2:30 AM UTC (8:00 AM IST)
    tags=["assurance_csv"],
    start_date=datetime.datetime(2024, 5, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1 # This ensures only one active DAG run at a time
) as dag:

    # Create time partition path for s3
    s3_partition_path = PythonOperator(
        task_id='partition_path',
        python_callable=create_partitioned_path_hive_style
    )
    
    sftp_sourcing = GlueJobOperator(
        task_id="sftp_sourcing",
        job_name=f"worley-datalake-{region_full_name}-{environment}-glue-job-sftp-sourcing",
        region_name=region,
        verbose=True,
        wait_for_completion=True,
        script_args={
            "--bucket_key": 'health_safety_environment/assurance/raw/'+'{{ ti.xcom_pull(task_ids="partition_path") }}',
            "--source_name": source_system_id,
            "--function_name": function_name
        },
    )
    
    # Gets current execution date
    execution_date = "{{logical_date}}"

    # Get the current date
    today = datetime.datetime.now()



    @task_group(group_id="assurance_csv_sourcing")
    def assurance_csv_sourcing():
        """Task Group that runs the required steps to source assurance"""

        csv_task = GlueJobOperator(
            task_id="assurance_csv_parsing",
            job_name=glue_job_names["assurance_csv_parsing"],
            region_name=region,
            verbose=True,
            wait_for_completion=True,
            script_args={
                "--source_name": source_system_id,
                "--metadata_type": f"csv#{source_system_id}",
                "--function_name": "csv",
                "--start_date": execution_date,
                "--end_date": execution_date,
                "--connector_file_path" : 'health_safety_environment/assurance/raw/'+'{{ ti.xcom_pull(task_ids="partition_path") }}'
            },
        )
        
    sourcing_tasks = assurance_csv_sourcing()

    raw_crawler = GlueCrawlerOperator(
        task_id=f"assurance_csv_raw_crawler",
        config={
            "Name": f"worley-datalake-sydney-{environment}-glue-crawler-health-safety-environment-{source_system_id}-raw",
        },
        poll_interval=5,
        wait_for_completion=False,
    )
    # Raw crawler sensor
    raw_crawler_sensor = GlueCrawlerSensor(
        task_id="wait_for_erm_raw_crawler",
        crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-health-safety-environment-{source_system_id}-raw",
    )

    # Detect Schema Change Task Group with batching
    @task_group(group_id="detect_schema_change")
    def detect_schema_change():
        previous_batch_group = None
        for i in range(0, len(datasets), 3):  # Batch size of 3
            batch = datasets[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"raw_{source_system_id}_{table.lower()}_schema_change",
                        job_name=glue_job_names["schema_change_detection"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        max_active_tis_per_dagrun=3,
                        script_args={
                            "--catalog_db": f"worley_datalake_sydney_{environment}_glue_catalog_database_health_safety_environment_{source_system_id}_raw",
                            "--table_name": f"raw_{table.lower()}"
                        },
                    )

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group



    # Raw to Curated Task Group with batching
    @task_group(group_id="curation")
    def raw_to_curated():
        previous_batch_group = None
        for i in range(0, len(datasets), 3):  # Batch size of 3
            batch = datasets[i:i+3]
            batch_group_id = f"batch_{i//3}"

            with TaskGroup(group_id=batch_group_id) as batch_group:
                for table in batch:
                    task = GlueJobOperator(
                        task_id=f"cur_{source_system_id}_{table}",
                        job_name=glue_job_names["assurance_raw_curated"],
                        region_name=region,
                        verbose=True,
                        wait_for_completion=True,
                        script_args={
                            "--source_system_id": f"{source_system_id}",
                            "--metadata_type": f"curated#assurance#{table}#job#iceberg",
                            "--start_date": execution_date,
                            "--end_date": execution_date,
                        },
                    )                

            # Set up dependency to ensure sequential execution between batches
            if previous_batch_group:
                previous_batch_group >> batch_group
            previous_batch_group = batch_group


    sns_notification_for_failure = PythonOperator(
        task_id="sns_notification_for_failure",
        python_callable=invoke_lambda_function,
        provide_context=True,
        op_args=['worley-data-modelling-sns-notification',payload],
        trigger_rule='one_failed'
    )
    
    
    (
    s3_partition_path >> sftp_sourcing >> sourcing_tasks >> raw_crawler >> raw_crawler_sensor >> detect_schema_change() >> raw_to_curated() >> sns_notification_for_failure
    )
