import boto3
import json
import logging
import time
from datetime import datetime

from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def put_events_to_eventbridge(events_list, event_bus_name):
    """
    Put events to custom EventBridge event bus
    
    Args:
        events_list (list): List of events to be published
        event_bus_name (str): Name of the custom event bus
    """
    try:
        # Create EventBridge client
        events_client = boto3.client('events')
        
        # Prepare events in required format
        entries = []
        for event in events_list:
            entry = {
                'Source': event['source_app'],
                'DetailType': event['detail_type'],
                'Detail': json.dumps(event['detail']),
                'EventBusName': event_bus_name
            }
            entries.append(entry)
            
        # Put events in batches of 10 (EventBridge limit)
        for i in range(0, len(entries), 10):
            batch = entries[i:i + 10]
            response = events_client.put_events(Entries=batch)
            
            # Check for failed entries
            if response['FailedEntryCount'] > 0:
                failed_entries = [
                    entry for entry in response['Entries'] 
                    if 'ErrorCode' in entry
                ]
                logger.error(f"Failed to put events: {failed_entries}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error putting events to EventBridge: {str(e)}")
        raise e


# Assume the role of System Integration
def assume_role(sts_client, target_role_arn):

    response = sts_client.assume_role(
        RoleArn=target_role_arn,
        RoleSessionName='DynamoDBCrossAccountSession'
    )
    
    return response['Credentials']


def get_latest_success_timestamp(dynamodb, ops_table_name, primary_key, event_detail_type) -> str:
    try:
        ops_table = dynamodb.Table(ops_table_name)
        response = ops_table.query(
            KeyConditionExpression=Key('SourceSystemId').eq(primary_key) & Key("MetadataType").eq(event_detail_type),
            FilterExpression=Attr('status').eq('SUCCESS'),
            ProjectionExpression="execution_timestamp",
            ScanIndexForward=False
        )
    
        if 'Items' in response and response['Items']:
            latest_record = response['Items'][0]
            latest_timestamp = latest_record['timestamp']

            # converting the latest_timestamp value which is in string datetime format to datetime format in python
            latest_timestamp_datetime = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S')
            return latest_timestamp_datetime
        else:
            return None
    except ClientError as e:
            logger.error(e)
            raise e
    

def execute_athena_query(athena_client, query, database, athena_results_bucket, target_kms_key):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={
            'OutputLocation': f's3://{athena_results_bucket}/system_int_athena_results/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_KMS',
                'KmsKey': target_kms_key
            },
        }
    )
    return response['QueryExecutionId']


# Function to get the query results from Athena
def get_query_results(athena_client, query_execution_id):
    # Wait until the query execution is completed
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            logger.info("Query executed successfully.")
            break
        elif status in ['FAILED', 'CANCELLED']:
            logger.info(f"Query failed or was cancelled: {status}")
            logger.info(f"{result=}")
            return None, status
        else:
            logger.info("Waiting for query to complete...")
            time.sleep(5)

    # Get the result of the query execution
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return result['ResultSet'], status
    

def get_loadid_and_projectid_since_last_success(source_sys, target_apps, athena_client, audit_tbl_name, audit_db_name, athena_results_bucket, target_acc_kms_key, customer_id, latest_success_timestamp) -> list:
 
    # first run clause to pull data from data platform
    latest_success_timestamp = latest_success_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    query = ""
    if not latest_success_timestamp:
        for tgt, i in enumerate(target_apps):
            if i == len(target_apps)-1:
                query += f"SELECT invocation_id, project_id, execution_time, source_system, {tgt} as target_system FROM {audit_tbl_name} where source_system = {source_sys} AND customer_id = '{customer_id}'"
            else:
                query += f"SELECT invocation_id, project_id, execution_time, source_system, {tgt} as target_system  FROM {audit_tbl_name} where source_system = {source_sys} AND customer_id = '{customer_id}' UNION ALL "
    else:

        for tgt, i in enumerate(target_apps):
            if i == len(target_apps)-1:
                query += f"""
                    SELECT invocation_id, project_id, execution_time, source_system, {tgt} as target_system 
                    FROM {audit_tbl_name}
                    WHERE customer_id = '{customer_id}' AND execution_time > timestamp '{latest_success_timestamp}' AND source_system = {source_sys}
                """
            else:
                query += f"""SELECT invocation_id, project_id, execution_time, source_system, {tgt} as target_system 
                    FROM {audit_tbl_name}
                    WHERE customer_id = '{customer_id}' AND execution_time > timestamp '{latest_success_timestamp}' AND source_system = {source_sys} UNION ALL """

    logger.info(f"Executing Athena query... {query}")

    query_execution_id = execute_athena_query(athena_client, query, audit_db_name, athena_results_bucket, target_acc_kms_key)
    query_results, status = get_query_results(athena_client, query_execution_id)

    if status == "SUCCEEDED":
        logger.info(f"Retrieved the records count - {query_results['Rows']}")
        logger.info(f"Collected columns from Target ACC audit table - {query_results['Rows'][0]}")

        loadid_projectid_list = defaultdict(lambda: [])
        load_ids_info = defaultdict(lambda: 0)
        for row in query_results['Rows'][1:]:  
            loadid = row['Data'][0]['VarCharValue']
            projectids = row['Data'][1]['VarCharValue'].split(',')
            execution_time = row['Data'][2]['VarCharValue']
            source_app = row['Data'][3]['VarCharValue']
            target_app = row['Data'][4]['VarCharValue']

            loadid_projectid_list[target_app] = loadid_projectid_list[target_app].append({
                'load_id': loadid,
                'project_ids': projectids,
                'execution_timestamp': execution_time,
                'target_app': target_app,
                'source_app': source_app
            })

            load_ids_info[loadid] += 1
        
        if len(load_ids_info) != 1:
            logger.info(f"Received multiple load_ids, {load_ids_info}")
    
        return loadid_projectid_list

    else:
        logger.info(f"{query_results=}")
        logger.info(f"Query failed with status: {status}")
        raise Exception(f"Query failed with status: {status}")