import sys
import json
import boto3
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
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
import re

# Initializing boto3 resources
glue = boto3.client("glue")
dynamodb = boto3.client('dynamodb', region_name="us-west-2")
s3 = boto3.client("s3")
systemmanager = boto3.client('ssm')
secretmanager = boto3.client("secretsmanager")
sqs = boto3.client('sqs', region_name = 'us-west-2')

#getting workflow parameters from lambdafunction
args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

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
etlinprog = 'ETL_Inprogress'
etlcomplete = 'ETL_Completed'
etlfailed = 'ETL_Failed'

#Generating Auditid
today = datetime.datetime.now()
date_time = today.strftime("%Y%m%d%H%M%S")
audtidin = date_time + "_" + jobiddtl
audtidc = date_time + jobiddtl

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

success_msg = "ETL_Completed"
failure_msg = "ETL_Failed"

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
                ':value': {'S': f'{etlinprog}'}
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
        print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + etlinprog)
       
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


#Fetching Exec query and s3 path from job dtl table
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
        ExecParm = item['EXEC_QUERY']
        Source_Primary_Key = item['SRC_PRIMARY_KEY']['S']
        tablename = item['TBL_NM']['S']
        s3basepath = item['ARCHV_S3_BASE_PATH']['S']
        s3path = "s3://" + s3basepath + "/" + jobid + "/" + tablename + "/"

        dynamodbcolumn = []
        for key,value in ExecParm.items():
            for key1,value1 in value.items():
                dynamodbcolumn.append(key1)

        Season_Attribute = "season_year"
        new_date = "created_date"
        FromDate = "fromDate"
        ToDate = "toDate"
       
    inprg = {
        'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidin}'}, 'AUDT_MSG': {'S':'Glue ETL Job Started'}, 'AUDT_TYP': {'S':'ETL Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-dynamodb-archival-etl-dev'}
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

# Script generated for node DynamoDB table  
    try:
# scan the DDB table
        DynamoDBtable_node1 = glueContext.create_dynamic_frame.from_options("dynamodb",
            connection_options={
                "dynamodb.input.tableName": tablename
            }
        )

# Script generated for node Filter
        df = DynamoDBtable_node1.toDF()
       
        if tablename == "ecomm-event-drop-size-stg":
            Date_Attribute = "created"
        elif tablename == "ecomm-futures-order-confirmation-v2-stg" or tablename == "ecomm-futures-order-confirmation-emails-v2-stg":
            Date_Attribute = "lastModifiedAtUTC"

        if Date_Attribute not in dynamodbcolumn:
            season = dynamodbcolumn[0]
            year = value[f'{season}']['S']
            print(season)
            print(year)
            df = df.filter(col(season)==year)
        elif Season_Attribute not in dynamodbcolumn:
            date = dynamodbcolumn[0]
            fromDate = value[f'{date}']['M']['fromDate']['S']
            toDate = value[f'{date}']['M']['toDate']['S']
            df = df.withColumn(new_date, col(date).cast(DateType()))
            df = df.filter(col(new_date).between(fromDate, toDate))
        elif Season_Attribute and Date_Attribute in dynamodbcolumn:
            date= dynamodbcolumn[0]
            season = dynamodbcolumn[1]
            year = value[f'{season}']['S']
            fromDate = value[f'{date}']['M']['fromDate']['S']
            toDate = value[f'{date}']['M']['toDate']['S']
            df = df.withColumn(new_date, col(date).cast(DateType()))
            df = df.filter((col(season)==year) & (col(new_date).between(fromDate, toDate)))
            print("season and date")
           
        rec_count = df.count()
        print("DynamoDB Record counts for Archival : ", rec_count)
        if df.count()>0:

            Transform2 = DynamicFrame.fromDF(df, glue_ctx=glueContext, name="df")
   
    # Script generated for node S3 bucket
            S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
                frame=Transform2,
                connection_type="s3",
                format="glueparquet",
                connection_options={
                    "path": s3path,
                },
                format_options={"compression": "gzip"},
                transformation_ctx="S3bucket_node3",
            )
        else:
            splitList=s3path.split('/', 3)
            bucket_name = splitList[len(splitList)-2]
            folder_name = splitList[len(splitList)-1]
            s3.put_object(Bucket=bucket_name, Key=(folder_name))
           
            table_name = tablename.replace("-","_")
           
            try:
                get_athena_database = glue.get_database(
                    Name = jobid
                )
                print(get_athena_database)
            except:
                crt_db = glue.create_database(
                    DatabaseInput={
                        'Name': jobid,
                    }
                )
                crt_tbl = glue.create_table(
                    DatabaseName=jobid,
                    TableInput={
                        'Name': table_name,
                        'StorageDescriptor': {
                            'Columns': [
                                {
                                    'Name': Date_Attribute,
                                    'Type': 'string',
                                },
                                {
                                    'Name': Source_Primary_Key,
                                    'Type': 'string',
                                }
                            ],
                        'Location': s3path,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                                'Name': 'SerdeInformation',
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                            },
                        }
                    }
                )
            else:
                crt_tbl = glue.create_table(
                    DatabaseName=jobid,
                    TableInput={
                        'Name': table_name,
                        'StorageDescriptor': {
                            'Columns': [
                                {
                                    'Name': Date_Attribute,
                                    'Type': 'string',
                                },
                                {
                                    'Name': Source_Primary_Key,
                                    'Type': 'string',
                                }
                            ],
                        'Location': s3path,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                                'Name': 'SerdeInformation',
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                            },
                        }
                    }
                )
           
    except:
#Sending ETL Failure message to SQS
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

#Updating job status in job dtl table
        try:
            updatejdfail = dynamodb.update_item(
               TableName = jobcntldtl,
                Key = jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{etlfailed}'}
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
            print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + etlfailed)
           
    else:
       
        comp = {
            'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidc}'}, 'AUDT_MSG': {'S':'Glue ETL Job Completed'}, 'AUDT_TYP': {'S':'ETL Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-dynamodb-archival-etl-dev'}
        }

#Updating logs in job audit table
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
                    ':value': {'S': f'{etlcomplete}'}
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
            print("JOB_ID#JOB_DTL_ID#STATUS from dtl table :" + jobid + "#" + jobiddtl + "#" + etlcomplete)

#Executing the main function based on the job dtl status
if jobdtlstatus == "ETL_Triggered":
    main()
elif jobdtlstatus == "ETL_Completed":
    exit
else:
    exit
   
   
job.commit()
