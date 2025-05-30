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
print("JOB_ID_DTL = " + str(jobid))

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
jobcntlsrcschema = "ecomm-archv-job-cntl-src-schema"

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
    print(getitem)
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
        Source_Primary_Key = item['SRC_PRIMARY_KEY']['S']
        databasename = item['SCHEMA_NM']['S']
        query = item['EXEC_QUERY']['S']
        query = str(query)
        qry = "(" + query + ") sampletable"
        tablename = item['TBL_NM']['S']
        srcpktype = item['SRC_PK_TYPE']['S']
        partitioncolumn = item['DB_PARALLEL_READ_KEY']['S']
        numpartition = item['DB_PARALLEL_READ_LIMIT']['N']
        numpartitions = int(numpartition)
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
        secret_manager_param = data['SECRETS_MANAGER_PARAM_ID']['S']

    inprg = {
        'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidin}'}, 'AUDT_MSG': {'S':'Glue ETL Job Started'}, 'AUDT_TYP': {'S':'ETL Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-rds-archival-etl-dev'}
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

#Getting RDS secretmanager from System Manager Parameter Store
    try:
        getsecretsparm = systemmanager.get_parameter(
            Name = secret_manager_param
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
        secret_manager_name = getsecretsparm['Parameter']['Value']

#Receiving username and password from secretmanager
    try:
        getrdssm = secretmanager.get_secret_value(
            SecretId = secret_manager_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        rdssecrets = json.loads(getrdssm['SecretString'])
        username = rdssecrets['username']
        password = rdssecrets['password']
        datastore = rdssecrets['engine']
        rds_url = rdssecrets['host']
        port_num = rdssecrets['port']
        port = str(port_num)
        rdsurl = "jdbc:" + datastore + "://" + rds_url + ":" +  port + "/" + databasename
        print(rdsurl)
        oldstr = tablename + ".*"
        newstr = "min(" + tablename + "." + partitioncolumn + ") as lowerbound, max(" + tablename + "." + partitioncolumn + ") as upperbound"
        newqry = query.replace(oldstr, newstr)
        newquery = "(" + newqry + ") sampletable"
        print(newquery)

#Script generated for node JDBC Connection
    try:
        parallelrd_df = spark.read \
                          .format("jdbc") \
                          .option("url", rdsurl) \
                          .option("dbtable", newquery) \
                          .option("user", username) \
                          .option("password", password) \
                          .load()
       
        parallelrd_df.show()
        if parallelrd_df.filter(parallelrd_df.lowerbound.isNotNull()).count()>0:
            lowerboundlst =  parallelrd_df.select('lowerbound').rdd.flatMap(lambda x: x).collect()
            upperboundlst =  parallelrd_df.select('upperbound').rdd.flatMap(lambda x: x).collect()
            if srcpktype == "INT":
                lowerbound = int(str(lowerboundlst[0]))
                upperbound = int(str(upperboundlst[0]))
            elif srcpktype == "CHAR":
                lowerbound = str(lowerboundlst[0])
                upperbound = str(upperboundlst[0])

            df = spark.read \
                   .format("jdbc") \
                   .option("url", rdsurl) \
                   .option("dbtable", qry) \
                   .option("user", username) \
                   .option("password", password) \
                   .option("partitionColumn", partitioncolumn) \
                   .option("lowerBound", lowerbound) \
                   .option("upperBound", upperbound) \
                   .option("numPartitions", numpartitions) \
                   .load()
                   
            rec_count = df.count()
            print("RDS Record counts for Archival : ", rec_count)
       
            JDBCConnection_node1 = DynamicFrame.fromDF(df, glueContext,"dynamicdf")

# Script generated for node S3 bucket
            S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
                frame=JDBCConnection_node1,
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
           
            if tablename == "order_event":
                Date_Attribute = "last_modified"
            elif tablename == "order_submit":
                Date_Attribute = "created"
            else:
                Date_Attribute = "last_modified_timestamp"
           
            try:
                get_athnna_database = glue.get_database(
                    Name = jobid
                )
                print(get_athnna_database)
            except:
                crt_db = glue.create_database(
                    DatabaseInput={
                        'Name': jobid,
                    }
                )
                crt_tbl = glue.create_table(
                    DatabaseName=jobid,
                    TableInput={
                        'Name': tablename,
                        'StorageDescriptor': {
                            'Columns': [
                                {
                                    'Name': Date_Attribute,
                                    'Type': 'timestamp',
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
                        'Name': tablename,
                        'StorageDescriptor': {
                            'Columns': [
                                    {
                                        'Name': Date_Attribute,
                                        'Type': 'timestamp',
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
        if datastore == "postgresql":
           
            crt_query = "(select " + databasename + ".describe_table('" + databasename + "','" + tablename + "')) sampletable"
           
            crt_tbl_df = spark.read \
                   .format("jdbc") \
                   .option("url", rdsurl) \
                   .option("dbtable", crt_query) \
                   .option("user", username) \
                   .option("password", password) \
                   .load()
   
            crt_tbl_df.show()
            crt_qry1 = crt_tbl_df.select('describe_table').rdd.flatMap(lambda x: x).collect()
            crt_qry = [i.replace('\n   ', '') for i in crt_qry1]
   
            index_query = "(select indexdef from pg_indexes where schemaname = '" + databasename + "' and tablename ='" + tablename + "') sampletable"
           
            indx_tbl_df = spark.read \
                   .format("jdbc") \
                   .option("url", rdsurl) \
                   .option("dbtable", index_query) \
                   .option("user", username) \
                   .option("password", password) \
                   .load()
                   
           
            indx_tbl_df.show()
            indx_qry = indx_tbl_df.select('indexdef').rdd.flatMap(lambda x: x).collect()
   
    #Updating Create Table Query in ecomm-archv-job-cntl-src-schema
            try:
                updatecrtqry = dynamodb.update_item(
                   TableName = jobcntlsrcschema,
                    Key = jobdtlkey,
                    UpdateExpression = 'SET #attribute = :value',
                    ExpressionAttributeNames={
                        '#attribute': 'TBL_DDL'
                    },
                    ExpressionAttributeValues={
                        ':value': {'S': f'{crt_qry}'}
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
                print(updatecrtqry)
   
    #Updating Index Queries in jobcntlsrcschema table
            try:
                updateindxqry = dynamodb.update_item(
                   TableName = jobcntlsrcschema,
                    Key = jobdtlkey,
                    UpdateExpression = 'SET #attribute = :value',
                    ExpressionAttributeNames={
                        '#attribute': 'TBL_INDICES'
                    },
                    ExpressionAttributeValues={
                        ':value': {'L':[ {'S': f'{indx_qry}'}]}
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
                print(updateindxqry)
               
               
        comp = {
            'JOB_ID':{'S':f'{jobid}'}, 'AUDT_ID':{'S':f'{audtidc}'}, 'AUDT_MSG': {'S':'Glue ETL Job Completed'}, 'AUDT_TYP': {'S':'ETL Glue job'}, 'JOB_ID_DTL': {'S':f'{jobiddtl}'}, 'MDULE_NM':{'S':'Archive'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-archv-rds-archival-etl-dev'}
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
