import json
import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")

gluejobname = "Bulk_upload"


def lambda_handler(event, context):
    # TODO implement
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_name = event['Records'][0]['s3']['object']['key']
    
    try:
        start_gluejob = glue.start_job_run(
        JobName = gluejobname,
        Arguments={
            '--BUCKET_NAME' : bucket_name,
            '--OBJECT_NAME' : object_name
        })
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidInputException':
            raise e
        elif e.response['Error']['Code'] == 'EntityNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceException':
            raise e
        elif e.response['Error']['Code'] == 'OperationTimeoutException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ConcurrentRunsExceededException':
            raise e
    else:
        job_runid = start_gluejob['JobRunId']
        print("Glue Job Run ID : ", job_runid)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
