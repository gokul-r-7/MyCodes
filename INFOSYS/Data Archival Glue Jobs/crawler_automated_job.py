import sys
import json
import boto3
import time
import datetime
import base64
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initializing boto3 resources
glue = boto3.client("glue")
dynamodb = boto3.client('dynamodb', region_name="us-west-2")
systemmanager = boto3.client('ssm')
sqs = boto3.client('sqs', region_name = 'us-west-2')

#getting workflow parameters from lambdafunction
args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#getting the parameters from glue workflow
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = glue.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

jobid = workflow_params['JOB_ID']
jobiddtl = workflow_params['JOB_ID_DTL']

jobid = str(jobid)
jobiddtl = str(jobiddtl)
print("JOB_ID = " + str(jobid))
print("JOB_ID_DTL = " + str(jobiddtl))

#Local variables
crawlerinpg = 'Crawler_Inprogress'
crawlercomplete = 'Crawler_Completed'
crawlerfailed = "Crawler_Failed"

#Generating Auditid
today = datetime.datetime.now()
date_time = today.strftime("%Y%m%d%H%M%S")
audtidin = date_time + "_" + jobid
audtidc = date_time + jobid

crawler = 'Crawler'
crawlername = jobiddtl + crawler
success_msg = "Crawler_Completed"
failure_msg = "Crawler_Failed"

message_attributes = {
    'JOB_ID': {
        'DataType': 'String',
        'StringValue': f'{jobid}'
    },
    'JOB_ID_DTL': {
        'DataType': 'String',
        'StringValue': f'{jobiddtl}'
    }
}

jobdtlkey={
    'JOB_ID':{
        'S':f'{jobid}'
    },
    'JOB_ID_DTL':{
        'S':f'{jobiddtl}'
    }
}
   
jobhdrkey={
    'JOB_ID':{
        'S':f'{jobid}'
    }
}

#DynamoDB table names
jobaudt = "ecomm-archv-job-audt"
jobcntlhdr = "ecomm-archv-job-cntl-hdr"
jobcntldtl = "ecomm-archv-job-cntl-dtl"

#Getiing JOB_DTL_STATUS from job dtl table to check the status
try:
    getitem = dynamodb.get_item(TableName = jobcntldtl, Key = jobdtlkey)
except ClientError as e:
    if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
        raise e
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        raise e
    elif e.response['Error']['Code'] == 'RequestLimitExceeded':
        raise e
    elif e.response['Error']['Code'] == 'InternalServerError':
        raise e
else:
    item = getitem['Item']
    jobdtlstatus = item['JOB_DTL_STATUS']['S']

def main():
#Updating Jobstatus in job dtl table
    try:
        updatejdinprog = dynamodb.update_item(
            TableName = jobcntldtl,
            Key = jobdtlkey,
            UpdateExpression = 'SET #attribute = :value',
            ExpressionAttributeNames={
                '#attribute': 'JOB_DTL_STATUS'
            },
            ExpressionAttributeValues={
                ':value': {'S': f'{crawlerinpg}'}
            },
            ReturnValues = 'UPDATED_NEW'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise e
        elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'ItemCollectionSizeLimitExceededException':
            raise e
        elif e.response['Error']['Code'] == 'TransactionConflictException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceeded':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        print(updatejdinprog)
        print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + crawlerinpg)

#Getting SQS url from System Manager Parameter Store
    try:
        getsqsparm = systemmanager.get_parameter(
            Name='ecomm-pme-data-archv-glue-etl-sqs-url'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalServerError':
            raise e
        elif e.response['Error']['Code'] == 'InvalidKeyId':
            raise e
        elif e.response['Error']['Code'] == 'ParameterNotFound':
            raise e
        elif e.response['Error']['Code'] == 'ParameterVersionNotFound':
            raise e
    else:
        queue_url = getsqsparm['Parameter']['Value']

#Getting Glue role from System Manager Parameter Store
    try:
        getsqsparm = systemmanager.get_parameter(
            Name='ecomm-pme-data-archv-glue-execution'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalServerError':
            raise e
        elif e.response['Error']['Code'] == 'InvalidKeyId':
            raise e
        elif e.response['Error']['Code'] == 'ParameterNotFound':
            raise e
        elif e.response['Error']['Code'] == 'ParameterVersionNotFound':
            raise e
    else:
        gluerole = getsqsparm['Parameter']['Value']

#Fetching Athenadb and s3 path from job dtl table
    try:
        getitem = dynamodb.get_item(TableName = jobcntldtl, Key = jobdtlkey)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceeded':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        item = getitem['Item']
        Entity_Group = item['ENTITY_GRP']['S']
        tablename = item['TBL_NM']['S']
        s3basepath = item['ARCHV_S3_BASE_PATH']['S']
        s3path = "s3://" + s3basepath + "/" + jobid + "/" + tablename + "/"
       
#Fetching rds url and partitionkey from job hdr table
    try:
        getitem_jh = dynamodb.get_item(TableName = jobcntlhdr, Key = jobhdrkey)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceeded':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        data = getitem_jh['Item']
        Job_Requested_by_ID = data['JOB_REQUESTED_BY_ID']['S']
       
    inprg = {
            'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidin}'}, 'AUDT_MSG': {'S':'Glue Crawler Job Started'}, 'AUDT_TYP': {'S':'Crawler Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-rds-archival-crawler-dev'}
        }

#Updating logs in Job Audit table
    try:
        putiteminp = dynamodb.put_item(TableName = jobaudt, Item = inprg)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise e
        elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ItemCollectionSizeLimitExceededException':
            raise e
        elif e.response['Error']['Code'] == 'TransactionConflictException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceede':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        print(putiteminp)

#Create and Run the Crawler
    try:
        try:
            createcrawler = glue.create_crawler(
                Name = crawlername,
                Role = gluerole,
                DatabaseName = jobid,
                Targets =
                {
                    'S3Targets':
                        [
                            {
                                'Path':s3path
                            }
                        ]
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidInputException':
                raise e
            elif e.response['Error']['Code'] == 'AlreadyExistsException':
                raise e
            elif e.response['Error']['Code'] == 'OperationTimeoutException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
                raise e
        else:
            print(createcrawler)
       
        try:
            startcrawler = glue.start_crawler(
                Name = crawlername
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'CrawlerNotRunningException':
                raise e
            elif e.response['Error']['Code'] == 'CrawlerStoppingException':
                raise e
            elif e.response['Error']['Code'] == 'OperationTimeoutException':
                raise e
        else:
            print(startcrawler)
           
        while True:
            state=glue.get_crawler(Name = crawlername)
            status=state["Crawler"]
            st=status['State']
            if(st == 'RUNNING' or st == 'STOPPING'):
                time.sleep(30)
            else:
                break
    except:
       
        try:
            Last_Crawl_Status=glue.get_crawler(Name = crawlername)
            Crawl_Status=Last_Crawl_Status['Crawler']['LastCrawl']['Status']
            if(Crawl_Status == 'SUCCEEDED'):
                deletecrawler = glue.delete_crawler(
                    Name = crawlername
                )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'CrawlerRunningException':
                raise e
            elif e.response['Error']['Code'] == 'SchedulerTransitioningException':
                raise e
            elif e.response['Error']['Code'] == 'OperationTimeoutException':
                raise e
        else:
            print(deletecrawler)
#Sending ETL Failure message to SQL  
        try:
            failsqs = sqs.send_message(
                QueueUrl = queue_url,
                MessageAttributes = message_attributes,
                MessageBody = failure_msg
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidMessageContents':
                raise e
            elif e.response['Error']['Code'] == 'UnsupportedOperation':
                raise e
        else:
            print(failsqs)
#Updating Jobstatus in job dtl table
        try:
            updatejdfail = dynamodb.update_item(
               TableName = jobcntldtl,
                Key = jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{crawlerfailed}'}
                },
                ReturnValues = 'UPDATED_NEW'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise e
            elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'ItemCollectionSizeLimitExceededException':
                raise e
            elif e.response['Error']['Code'] == 'TransactionConflictException':
                raise e
            elif e.response['Error']['Code'] == 'RequestLimitExceeded':
                raise e
            elif e.response['Error']['Code'] == 'InternalServerError':
                raise e
        else:
            print(updatejdfail)
            print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + crawlerfailed)
       
    else:
       
#Deleting the Crawler
        try:
            Last_Crawl_Status=glue.get_crawler(Name = crawlername)
            Crawl_Status=Last_Crawl_Status['Crawler']['LastCrawl']['Status']
            if(Crawl_Status == 'SUCCEEDED'):
                deletecrawler = glue.delete_crawler(
                    Name = crawlername
                )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'CrawlerRunningException':
                raise e
            elif e.response['Error']['Code'] == 'SchedulerTransitioningException':
                raise e
            elif e.response['Error']['Code'] == 'OperationTimeoutException':
                raise e
        else:
            print(deletecrawler)

       
#Updating logs in job audit table
        comp = {
            'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidc}'}, 'AUDT_MSG': {'S':'Glue Crawler Job Completed'}, 'AUDT_TYP': {'S':'Crawler Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-rds-archival-crawler-dev'}
        }

        try:
            putitemcomp = dynamodb.put_item(TableName = jobaudt, Item = comp)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise e
            elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                raise e
            elif e.response['Error']['Code'] == 'ItemCollectionSizeLimitExceededException':
                raise e
            elif e.response['Error']['Code'] == 'TransactionConflictException':
                raise e
            elif e.response['Error']['Code'] == 'RequestLimitExceede':
                raise e
            elif e.response['Error']['Code'] == 'InternalServerError':
                raise e
        else:
            print(putitemcomp)

#Updating job status in job dtl table

        try:
            updatejdcomp = dynamodb.update_item(
               TableName = jobcntldtl,
                Key = jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{crawlercomplete}'}
                },
                ReturnValues = 'UPDATED_NEW'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise e
            elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'ItemCollectionSizeLimitExceededException':
                raise e
            elif e.response['Error']['Code'] == 'TransactionConflictException':
                raise e
            elif e.response['Error']['Code'] == 'RequestLimitExceeded':
                raise e
            elif e.response['Error']['Code'] == 'InternalServerError':
                raise e
        else:
            print(updatejdcomp)
            print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + crawlercomplete)
           
        try:
            Successsqs = sqs.send_message(
                QueueUrl = queue_url,
                MessageAttributes = message_attributes,
                MessageBody = success_msg
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidMessageContents':
                raise e
            elif e.response['Error']['Code'] == 'UnsupportedOperation':
                raise e
        else:
            print(Successsqs)

#Executing the main function based on the job dtl status        
if jobdtlstatus == "ETL_Completed":
    main()
else:
    exit
