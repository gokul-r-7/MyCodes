import boto3
import os
import re
import datetime


session = boto3.Session(profile_name='harshnia+arl-Admin',region_name='us-west-2')
s3 = session.client('s3')


def create_partitioned_path_hive_style():
    # Get the current date and time
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    partitioned_path = f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"
    return partitioned_path


def parse_s3_url(destination_object):
    print(destination_object)
    if destination_object.startswith("s3://"):
        parts = destination_object[5:].split('/', 1)
        bucket_name = parts[0]
        bucket_key = parts[1] if len(parts) > 1 else ""
        return bucket_name, bucket_key
    else:
        raise ValueError("The event contains not valid S3 URL")


def parse_key(key):
    # Gets the file name from the Key.
    try:
        parts = key.split('/')
        file_name = parts.pop()
        return file_name
    except:
        raise ValueError(f"Does not contain a valid S3 key :::: check {key}")
    

def move_files_hive_folder_structure(bucket_name,flow_name,execution_id,bucket_prefix):

    # bucket_name, destination_key = parse_s3_url(destination_object)
    source_key = f"{bucket_prefix}{flow_name}/{execution_id}"

    partitioned_destination_key = bucket_prefix + create_partitioned_path_hive_style()
    objects = []
    paginator = s3.get_paginator('list_objects_v2')

    pages = paginator.paginate(
        Bucket=bucket_name,
        Prefix=source_key
    )
    file_out_locations = []
    try:
        for page in pages:
            for obj in page['Contents']:
                objects.append(obj)
                for copy in objects:
                    file_name = parse_key(copy['Key'])
                    copy_source = {'Bucket': bucket_name, 'Key': copy['Key'], 'file_name': file_name}
                    if file_name == execution_id:
                        print("Found meta data file, skipping file {file_name}")
                        continue
                    print(f"Copying the file..... CopySource=/{bucket_name}/{copy_source['Key']} Key={partitioned_destination_key}{file_name}")
                    s3.copy_object(Bucket=bucket_name, CopySource=f"/{bucket_name}/{copy_source['Key']}", Key=f"{partitioned_destination_key}{file_name}")
                    print(f"Deleting the file..... Key={bucket_name}/{copy_source['Key']}")
                    s3.delete_object(Bucket=bucket_name, Key=f"{bucket_name}/{copy_source['Key']}")
                    file_out_locations.append(f"s3://{bucket_name}/{partitioned_destination_key}{file_name}")

        return partitioned_destination_key
    except Exception as e:
        error_message = f"Error in Lambda function copying files from Appflow working directory: {str(e)}"
        print(error_message)

    

def lambda_handler(event, context):

    bucket_name = event['bucket_name']
    bucket_prefix = event['bucket_prefix']
    # destination_object = event['detail']['destination-object']
    
    flow_name = event['flow-name']
    execution_id = event['execution-id']

    try:
        file_out_path = move_files_hive_folder_structure(
            bucket_name=bucket_name,
            flow_name=flow_name,
            execution_id=execution_id,
            bucket_prefix = bucket_prefix
        )

        return {
            'statusCode': 200,
            'body': {
                'message': 'Files moved successfully',
                'file_out_path': file_out_path
            }
        }
    
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }
    




# Leave this for local debugging 
if __name__ == "__main__":
    event = {'version': '0', 'id': '5211c7f7-d362-4b46-b65c-afcc11e89172', 'detail-type': 'AppFlow End Flow Run Report', 'source': 'aws.appflow', 'account': '891377181979', 'time': '2024-07-03T03:20:13Z', 'region': 'ap-southeast-2', 'resources': ['52a7be052aa910faa3752fc4fe8f09b7'], 'detail': {'flow-name': 'sharepoint_cp2vg_listing_library_0001', 'created-by': 'arn:aws:sts::891377181979:assumed-role/AWSReservedSSO_DacDataHubDataEngineers_f4336f204946ad15/_Joao.Palma@Worley.com', 'flow-arn': 'arn:aws:appflow:ap-southeast-2:891377181979:flow/sharepoint_cp2vg_listing_library_0001', 'source': 'CUSTOMCONNECTOR/vgcp2-sharepoint-connector', 'destination': 'S3', 'source-object': 'sites/worleyparsons.sharepoint.com,7a414757-0bba-4284-bea4-6f968f07b0d1,2b21370c-6618-4355-bc26-888020f990b7', 'destination-object': 's3://worley-datalake-sydney-dev-bucket-raw-xd5ydg/vg/sharepoint/raw/', 'trigger-type': 'ONDEMAND', 'num-of-records-processed': '3', 'execution-id': '6ebcb45a-a1c9-4459-bdd5-35e70660f8b7', 'num-of-records-filtered': '0', 'start-time': '2024-07-03T03:20:02.829Z[UTC]', 'num-of-documents-processed': '3', 'end-time': '2024-07-03T03:20:12.917Z[UTC]', 'num-of-record-failures': '0', 'data-processed': '1333201', 'status': 'Execution Successful'}}
    # lambda_handler(event, None)
    appflow_client = session.client('appflow')

    flow_details = appflow_client.describe_flow(
      flowName='sharepoint_cp2vg_listing_library_0002'
    )

    bucket_name = flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketName']
    prefix = flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketPrefix']
    # key = f"flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketPrefix']/{flow}/{execution_id}"

    print (x)

    flow_details['destinationFlowConfigList'][0]['destinationConnectorProperties']['S3']['bucketPrefix']