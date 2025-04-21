import boto3
import json
import time

def lambda_handler(event, context):
    # Initialize the boto3 client for EMR Serverless
    emr_client = boto3.client('emr-serverless')

    # Define the EMR Serverless application ID and the S3 location of your PySpark script
    application_id = '00foqncl537d2b09'  # Replace with your actual application ID
    s3_script_path = 's3://test-gokul-220797/pyspark_scripts/emr_test_pyspark.py'  # Replace with your actual S3 script location

    trigger_emr_application = emr_client.start_application(
        applicationId='00foqncl537d2b09'
        )
        
    application_run_id = trigger_emr_application
    print("Hello World")
    print(application_run_id)
    print("Hello World")
    
    
    trigger_emr_job = emr_client.start_job_run(
    applicationId='00foqncl537d2b09',
    #clientToken='string',
    executionRoleArn='arn:aws:iam::861276091626:role/service-role/AmazonEMR-ExecutionRole-1734601111548',
    jobDriver={
        'sparkSubmit': {
            'entryPoint': s3_script_path,
            'entryPointArguments': []
        }
    },
    configurationOverrides={
        'monitoringConfiguration': {
            's3MonitoringConfiguration': {
                'logUri': 's3://test-gokul-220797/EMR Logs'
            },
            'managedPersistenceMonitoringConfiguration': {
                'enabled': True
            },
            'cloudWatchLoggingConfiguration': {
                'enabled': True,
                'logGroupName': '/aws/emr-serverless',
                'logStreamNamePrefix': 'csv_to_parquet_lambda'

            }
        }
    },
    tags={
        'Gokul': 'EMR'
    },
    executionTimeoutMinutes=30,
    name='csv_to_postgres_lambda',
    mode='BATCH',
    retryPolicy={
        'maxAttempts': 1
    }
    )
    submit_job_runid = trigger_emr_job
    print("Hello World")
    print(application_run_id)
    print("Hello World")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    return {
            'statusCode': 500,
            'body': json.dumps()
        }
    
