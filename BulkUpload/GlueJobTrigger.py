import json
import boto3

glue = boto3.client("glue")

gluejobname = "Bulk_upload"


def lambda_handler(event, context):
    # TODO implement
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_name = event['Records'][0]['s3']['object']['key']
    
    start_gluejob = glue.start_job_run(
    JobName = gluejobname,
    Arguments={
        '--BUCKET_NAME' : bucket_name,
        '--OBJECT_NAME' : object_name
    })
    print(start_gluejob)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
