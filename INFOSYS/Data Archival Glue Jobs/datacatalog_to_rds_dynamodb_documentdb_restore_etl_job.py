import sys
import ast
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
dynamodb = boto3.client('dynamodb', region_name="us-west-2")
systemmanager = boto3.client('ssm')
secretmanager = boto3.client("secretsmanager")
sqs = boto3.client('sqs', region_name = 'us-west-2')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RESTORE_JOB_ID', 'RESTORE_JOB_ID_DTL'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rest_jobid  = args['RESTORE_JOB_ID']
rest_jobiddtl = args['RESTORE_JOB_ID_DTL']

#Local variables
restinprog = 'RST_Inprogress'
restcomplete = 'RST_Completed'
restfailed = 'RST_Failed'
success_msg = "RST_Completed"
failure_msg = "RST_Failed"
failure_msg_noparm = "RST_Failed_No_Parameters"

#DynamoDB table names
jobaudt = "ecomm-archv-job-audt"
jobcntlhdr = "ecomm-archv-job-cntl-hdr"
jobcntldtl = "ecomm-archv-job-cntl-dtl"
jobcntlsrcschema = "ecomm-archv-job-cntl-src-schema"
restorejobcntlhdr = "ecomm-archv-restore-job-cntl-hdr"
restorejobcntldtl = "ecomm-archv-restore-job-cntl-dtl"

#Getting SQS url from System Manager Parameter Store
try:
    getsqsparm = systemmanager.get_parameter(
        Name='ecomm-pme-data-archv-rstr-sqs-url'
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

#Sending ETL Failure message with no parameters to SQS
def rstfailednoparm():
    try:
        failsqsnoparm = sqs.send_message(
            QueueUrl = queue_url,
            MessageBody = failure_msg_noparm
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidMessageContents':
            raise e
        elif e.response['Error']['Code'] == 'UnsupportedOperation':
            raise e
    else:
        print(failsqsnoparm)
       
if len(rest_jobid) and len(rest_jobiddtl) == 0:
    etlfailednoparm()
else:
#Generating Auditid
    today = datetime.datetime.now()
    date_time = today.strftime("%Y%m%d%H%M%S")
    audtidin = date_time + "_" + rest_jobiddtl
    audtidc = date_time + rest_jobiddtl

    rest_jobdtlkey={
        'RESTORE_JOB_ID':{
            'S':f'{rest_jobid}'
        },
        'RESTORE_JOB_ID_DTL':{
            'S':f'{rest_jobiddtl}'
        }
    }
   
    rest_jobhdrkey={
        'RESTORE_JOB_ID':{
            'S':f'{rest_jobid}'
        }
    }

    message_attributes = {
        'RESTORE_JOB_ID': {
            'DataType': 'String',
            'StringValue': f'{rest_jobid}'
        },
        'RESTORE_JOB_ID_DTL': {
            'DataType': 'String',
            'StringValue': f'{rest_jobiddtl}'
        }
    }

#Sending RST_Failed message with job parameters to sqs
    def restorefailedsqs():
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

#Updating job status in restore job dtl table as RST_Failed
    def restorefailed():
        try:
            updatejdfail = dynamodb.update_item(
                TableName = restorejobcntldtl,
                Key = rest_jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'RESTORE_JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{restfailed}'}
                },
                ReturnValues = 'UPDATED_NEW'
            )
        except ClientError as e:
            restorefailedsqs()
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
            print("JOB_ID # JOB_DTL_ID # STATUS from Restore dtl table : " + rest_jobid + " # " + rest_jobiddtl + " # " + restfailed)
           
    def restorecompleted():
        try:
            successsqs = sqs.send_message(
                QueueUrl = queue_url,
                MessageAttributes = message_attributes,
                MessageBody = success_msg
            )
        except ClientError as e:
            restorefailedsqs()
            if e.response['Error']['Code'] == 'InvalidMessageContents':
                raise e
            elif e.response['Error']['Code'] == 'UnsupportedOperation':
                raise e
        else:
            print("Sent success message to SQS", successsqs)
#Updating job status in job dtl table as RST_Completed
        try:
            updatejdcomp = dynamodb.update_item(
                TableName = restorejobcntldtl,
                Key = rest_jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'RESTORE_JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{restcomplete}'}
                },
                ReturnValues = 'UPDATED_NEW'
            )
        except ClientError as e:
            restorefailedsqs()
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
            print("JOB_ID # JOB_DTL_ID # STATUS from Restore dtl table :" + rest_jobid + " # " + rest_jobiddtl + " # " + restcomplete)
               
#Updating logs in job audit table
        try:
            putitemcomp = dynamodb.put_item(TableName = jobaudt, Item = comp)
        except ClientError as e:
            restorefailedsqs()
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

#Fetching secret Manager name from Restore job hdr table
    def secretmanagerdata():
        try:
            getitem_rsjh = dynamodb.get_item(TableName = restorejobcntlhdr, Key = rest_jobhdrkey)
        except ClientError as e:
            restorefailedsqs()
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'RequestLimitExceeded':
                raise e
            elif e.response['Error']['Code'] == 'InternalServerError':
                raise e
        else:
            datars = getitem_rsjh['Item']
            secret_manager_name = datars['SECRETS_MANAGER_KEY']['S']
            print("secret_manager_name :" + secret_manager_name)
           
       
#Receiving host, username and password from secretmanager
        try:
            getsm = secretmanager.get_secret_value(
                SecretId = secret_manager_name
            )
        except ClientError as e:
            restorefailedsqs()
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
            secrets = json.loads(getsm['SecretString'])
            global username
            global password
            username = secrets['username']
            password = secrets['password']
            datastore = secrets['engine']
            global host
            host = secrets['host']
            port_num = secrets['port']
            port = str(port_num)
            mysqlurl = "jdbc:" + datastore + "://" + host + ":" +  port + "/" + databasename
            postgresurl = "jdbc:" + datastore + "://" + host + ":" +  port + "/" + databasename
            global documentdburl
            documentdburl = "mongodb://" + host
           
    def fixkey(key):
        # toy implementation
        #print("fixing {}".format(key))
        return key.replace("_h_", "-").replace("_s_", " ")

    def normalize(data):
        #print("normalizing {}".format(data))
        if isinstance(data, dict):
            data = {fixkey(key): normalize(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [normalize(item) for item in data]
        return data

#Fetching Archive Jobid, jobdtlid, jobdtlstatus job dtl table
    try:
        getitemrsdtl = dynamodb.get_item(TableName = restorejobcntldtl, Key = rest_jobdtlkey)
    except ClientError as e:
        restorefailedsqs()
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceeded':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        print(getitemrsdtl)
        itemrsdtl = getitemrsdtl['Item']
        jobid = itemrsdtl['ARCHV_JOB_ID']['S']
        jobiddtl = itemrsdtl['ARCHV_JOB_ID_DTL']['S']
        rest_jobdtlstatus = itemrsdtl['RESTORE_JOB_DTL_STATUS']['S']

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
#Fetching  Database name and Table name from job dtl table
    try:
        getitemdtl = dynamodb.get_item(TableName = jobcntldtl, Key = jobdtlkey)
    except ClientError as e:
        restorefailedsqs()
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        elif e.response['Error']['Code'] == 'RequestLimitExceeded':
            raise e
        elif e.response['Error']['Code'] == 'InternalServerError':
            raise e
    else:
        itemdtl = getitemdtl['Item']
        Entity_Group = itemdtl['ENTITY_GRP']['S']
        global tablename
        global databasename
        databasename = itemdtl['SCHEMA_NM']['S']
        tablename = itemdtl['TBL_NM']['S']
        print("TableName : ", tablename)
        print("DatabaseName : ", databasename)
        schema_name = "cart"
        Postgrestablename = schema_name + "." + tablename
        documentdbtablename = tablename + "_restore"
        dynamodbtablename = tablename + "-restore"
       
    def main():
#Updating Jobstatus in job dtl table as RST_Inprogress
        try:
            updatejdinprog = dynamodb.update_item(
                TableName = restorejobcntldtl,
                Key = rest_jobdtlkey,
                UpdateExpression = 'SET #attribute = :value',
                ExpressionAttributeNames={
                    '#attribute': 'RESTORE_JOB_DTL_STATUS'
                },
                ExpressionAttributeValues={
                    ':value': {'S': f'{restinprog}'}
                },
                ReturnValues = 'UPDATED_NEW'
            )
        except ClientError as e:
            restorefailedsqs()
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
            print("JOB_ID # JOB_DTL_ID#STATUS from Restore dtl table :" + rest_jobid + " # " + rest_jobiddtl + " # " + restinprog)

#Fetching datastore type from job hdr table
        try:
            getitem_jh = dynamodb.get_item(TableName = jobcntlhdr, Key = jobhdrkey)
        except ClientError as e:
            restorefailedsqs()
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
            elif e.response['Error']['Code'] == 'RequestLimitExceeded':
                raise e
            elif e.response['Error']['Code'] == 'InternalServerError':
                raise e
        else:
            data_jh = getitem_jh['Item']
            Job_Requested_by_ID = data_jh['JOB_REQUESTED_BY_ID']['S']
            datastore_type = data_jh['DATASOURCE_TYPE']['S']
       
        global tablename  
        if datastore_type == "DYNAMODB":
            tablename = tablename.replace("-", "_")
            print(tablename)
       
        inprg = {
        'JOB_ID':{'S':f'{rest_jobid}'}, 'AUDT_ID':{'S':f'{audtidin}'}, 'AUDT_MSG': {'S':'Glue Restoration Job Started'}, 'AUDT_TYP': {'S':'RST Glue job'}, 'JOB_ID_DTL': {'S':f'{rest_jobiddtl}'}, 'MDULE_NM':{'S':'Restore'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-restore-qa'}
        }
       
#Updating logs in Job Audit table
        try:
            putiteminp = dynamodb.put_item(TableName = jobaudt, Item = inprg)
        except ClientError as e:
            restorefailedsqs()
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
           
        global comp
           
        comp = {
            'JOB_ID':{'S':f'{rest_jobid}'}, 'AUDT_ID':{'S':f'{audtidc}'}, 'AUDT_MSG': {'S':'Glue Restoration Job Completed'}, 'AUDT_TYP': {'S':'RST Glue job'}, 'JOB_ID_DTL': {'S':f'{rest_jobiddtl}'}, 'MDULE_NM':{'S':'Restore'}, 'TBL_NM':{'S':f'{tablename}'}, 'JOB_REQUESTED_BY_ID':{'S':f'{Job_Requested_by_ID}'}, 'ENTITY_GRP':{'S':f'{Entity_Group}'}, 'REC_ADD_TS':{'S':f'{today}'}, 'ADD_MDULE_NM':{'S':'ecomm-pme-data-restore-qa'}
        }
# Script generated for node Data Catalog table
        Datasource = glueContext.create_dynamic_frame.from_catalog(database = jobid, table_name = tablename)
        datasource = Datasource.toDF()
#        datasource.show()
        record_count = datasource.count()
        print("DataCatalog Record counts for Restoration : ", record_count)

# Script generated for node MySQL table    
        def mysqlrestore():
           
            try:
                secretmanagerdata()
               
                if Entity_Group == "Cartv5_GRP1":
                    MySQL_Restore_Connection = "ecomm-pme-cartv5-connection-dev"
                elif Entity_Group == "OrderEvent_GRP1":
                    MySQL_Restore_Connection = "ecomm-pme-order-event-connection-dev"
                elif Entity_Group == "OrderSubmit_GRP1":
                    MySQL_Restore_Connection = "ecomm-pme-order-submit-connection-dev"

                MySQL = glueContext.write_dynamic_frame.from_jdbc_conf(frame = Datasource, catalog_connection = MySQL_Restore_Connection, connection_options = {"dbtable": tablename, "database": databasename}, transformation_ctx = "MySQL")
            except:
                restorefailedsqs()
                restorefailed()
            else:
                restorecompleted()
       
        def postgresrestore():
            try:
                secretmanagerdata()

                Column_To_Check = "archive_job_id"
               
                if (Column_To_Check in datasource.columns):
                   
                    datasource_notnull= datasource.filter(datasource.archive_job_id.isNotNull())
                    datasource_notnull_count = datasource_notnull.count()
                    print(datasource_notnull_count)
                   
                    if datasource_notnull_count > 0:
                        postgres_notnull_node = DynamicFrame.fromDF(datasource_notnull, glueContext,"dynamicdf")
                        postgres1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = postgres_notnull_node, catalog_connection = "ecomm-pme-cartv6-connection-dev",connection_options = {"dbtable": Postgrestablename, "database": databasename, "stringtype": "unspecified"}, transformation_ctx = "postgres1")
                       
                    datasource_null = datasource.filter(datasource.archive_job_id.isNull())
                    datasource_null_count = datasource_null.count()
                    print(datasource_null_count)
                   
                    if datasource_null_count > 0:
               
                        datasource_null_drop = datasource_null.drop("archive_job_id")
                        datasource_null_drop_count = datasource_null_drop.count()
                        print(datasource_null_drop_count)
               
                        postgres_null_node = DynamicFrame.fromDF(datasource_null_drop, glueContext,"dynamicdf")
                        postgres2 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = postgres_null_node, catalog_connection = "ecomm-pme-cartv6-connection-dev",connection_options = {"dbtable": Postgrestablename, "database": databasename, "stringtype": "unspecified"}, transformation_ctx = "postgres2")
               
                else:
               
                    postgres = glueContext.write_dynamic_frame.from_jdbc_conf(frame = Datasource, catalog_connection = "ecomm-pme-cartv6-connection-dev",connection_options = {"dbtable": Postgrestablename, "database": databasename, "stringtype": "unspecified"}, transformation_ctx = "postgres")

            except:
                restorefailedsqs()
                restorefailed()
            else:
                restorecompleted()
                   
        def dynamodbrestore():
            try:
# Script generated for DynamoDB node
                glueContext.write_dynamic_frame_from_options(
                    frame=Datasource,
                    connection_type="dynamodb",
                    connection_options={
                        "dynamodb.output.tableName": dynamodbtablename
                    }
                )
            except:
                restorefailedsqs()
                restorefailed()
            else:
                restorecompleted()
                   
        def documentdbrestore():
            try:
                secretmanagerdata()
#Script Generated for DocumentDB Node  
                rec_count = datasource.count()
                print(rec_count)
               
                if datasource.count()>0:
                    results = datasource.toJSON().map(lambda j: json.loads(j)).collect()
                    newResults=[]
                    for result in results:
                        data = normalize(result)
                        newResult = json.dumps(data,indent=2,ensure_ascii=False)
                        newResults.append(newResult)
                    Partition_Size = 100
                    newDfTemp = sc.parallelize(newResults, Partition_Size)
                    newDf = spark.read.json(newDfTemp)
                    newDf.show()
                   
        #            newDf = newDf.withColumn(new_date, col(date).cast(DateType()))
                    Documentdb_node = DynamicFrame.fromDF(newDf, glueContext,"dynamicdf")

               
                    write_documentdb_options = {
                        "uri": documentdburl,
                        "database": databasename,
                        "collection": documentdbtablename,
                        "username": username,
                        "password": password,
                        "ssl": "true",
                        "ssl.domain_match": "false"
                    }
                    glueContext.write_dynamic_frame.from_options(Documentdb_node, connection_type="documentdb", connection_options=write_documentdb_options)
            except:
                restorefailedsqs()
                restorefailed()
            else:
                restorecompleted()
#Executing the function based on the datastore type        
        if datastore_type == "MySQL":
            mysqlrestore()
        elif datastore_type == "POSTGRESQL":
            postgresrestore()
        elif datastore_type == "DYNAMODB":
            dynamodbrestore()
        elif datastore_type == "DOCUMENTDB":
            documentdbrestore()
        else:
            exit
   
#Executing the main function based on the job dtl status
    if rest_jobdtlstatus == "RST_Triggered":
        main()
    else:
        exit

job.commit()