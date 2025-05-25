import boto3
import time
import logging
import os

from generic_helpers import put_events_to_eventbridge, assume_role, get_latest_success_timestamp, get_loadid_and_projectid_since_last_success

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENV = os.getenv('ENVN')

TARGET_ROLE_ARN = ""

ATHENA_RESULT_BUCKET = 'worley-datalake-sydney-dev-bucket-landing-xd5ydg'  # S3 bucket in DP
ATHENA_KMS_KEY_RESULTS = os.environ['DP_KMS_KEY']

CUSTOMER_MODEL_DB = "worley_datalake_sydney_dev_glue_catalog_database_project_control_oracle_p6_curated"
CUSTOMER_MODEL_TABLE = "curated_project_activity"
CUSTOMER_MODEL_AUDIT_TABLE = "project_audit_table"
OPS_TABLE_NAME = os.environ['OPERATIONAL_TABLE']

SRC_TARGET_MAPPING = {
    "p6": ["o3", "ecosys"]
}

sts_client = boto3.client('sts')

credentials = assume_role(sts_client, TARGET_ROLE_ARN)
logger.info('generated credentials successfully')

# Initialize DynamoDB client for Operational table
# Create an Dynamodb client using the assumed role credentials
target_dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)
logger.info('Created target account dynamodb client')
ops_table = target_dynamodb.Table(OPS_TABLE_NAME)

# Initialize Athena client
athena_client = boto3.client('athena')


def lambda_handler(event, context):
    try:
        logger.info(f"{event=}")
        logger.info(f"{context=}")
        
        source_name = event.get('source_name')
        customer_id = event.get('customer_id')

        if not event_detail_type or not customer_id:
            raise ValueError("Missing required parameters: event_detail_type or customerid.")

        target_apps = SRC_TARGET_MAPPING.get(source_name.lower())

        if target_apps is None:
            raise ...
        
        # Get the latest success timestamp from the Operational table
        recs_pull_min_timestamp = None
        for tgt_app in target_apps:
            primary_key = f"{source_name.lower()}_{tgt_app.lower()}"
            event_detail_type = f"{primary_key}#cross_reference#{customer_id.lower()}"
            rec_ts = get_latest_success_timestamp(target_dynamodb, OPS_TABLE_NAME, primary_key, event_detail_type)
            if rec_ts:
                if recs_pull_min_timestamp is None:
                    recs_pull_min_timestamp = rec_ts
                else:
                    recs_pull_min_timestamp = min(recs_pull_min_timestamp, rec_ts)
            else:
                logger.info(f"No existing records are present - {primary_key}, going with 1st run")
        
        # GET all the load and project ids which are greater than $recs_pull_max_timestamp
        loadid_projectid_info = get_loadid_and_projectid_since_last_success(source_name, target_apps, athena_client, CUSTOMER_MODEL_AUDIT_TABLE, CUSTOMER_MODEL_DB, ATHENA_RESULT_BUCKET, ATHENA_KMS_KEY_RESULTS, customer_id, recs_pull_min_timestamp)
        logger.info(f"{loadid_projectid_info=}")

        if loadid_projectid_info:
            events_to_publish = []
            
            # Prepare events for each target application
            for target_app, project_details in loadid_projectid_info.items():
                primary_key = f"{source_name.lower()}_{target_app.lower()}"
                event_detail_type = f"{primary_key}#cross_reference#{customer_id.lower()}"
                for detail in project_details:
                    event = {
                        'source': f"project_delivery_{target_app}",
                        'detail-type': event_detail_type,
                        'detail': {
                            'load_id': detail['load_id'],
                            'project_ids': detail['project_ids'],
                            'execution_timestamp': detail['execution_timestamp'],
                            'target_app': detail['target_app'],
                            'source_app': detail['source_app'],
                            'customer_id': customer_id
                        }
                    }
                    events_to_publish.append(event)
            
            # Put events to custom event bus
            event_bus_name = os.environ.get('EVENT_BUS_NAME')  # Get from environment variable
            success = put_events_to_eventbridge(events_to_publish, event_bus_name)
            
            if success:
                return {
                    'statusCode': 200,
                    'body': 'Events published successfully'
                }

        else:
            logger.info("No records found in Athena after the latest success timestamp.")
            return {
                'statusCode': 404,
                'body': 'No records found.'
            }

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Internal Server Error: {str(e)}"
        }

