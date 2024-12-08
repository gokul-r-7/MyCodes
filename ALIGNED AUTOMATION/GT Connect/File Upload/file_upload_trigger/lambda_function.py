"""Module providing function to use and access AWS services"""
import boto3
from botocore.exceptions import ClientError
glue = boto3.client("glue")
def lambda_handler(event, context):
    """Function triggers the gluejob based on S3 events"""
    print(event, context)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_name = event['Records'][0]['s3']['object']['key']
    gluejobname = "file_upload"
    try:
        start_gluejob = glue.start_job_run(
        JobName = gluejobname,
        Arguments={
            '--BUCKET_NAME' : bucket_name,
            '--OBJECT_NAME' : object_name
        })
    except ClientError as glue_error:
        if glue_error.response['Error']['Code'] == 'InvalidInputException':
            raise glue_error
        if glue_error.response['Error']['Code'] == 'EntityNotFoundException':
            raise glue_error
        if glue_error.response['Error']['Code'] == 'InternalServiceException':
            raise glue_error
        if glue_error.response['Error']['Code'] == 'OperationTimeoutException':
            raise glue_error
        if glue_error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            raise glue_error
        if glue_error.response['Error']['Code'] == 'ConcurrentRunsExceededException':
            raise glue_error
    else:
        job_runid = start_gluejob['JobRunId']
        print("Glue Job Run ID : ", job_runid)
    return {
        'Glue job Run ID': job_runid
    }
