import io
import sys
import json
import boto3
import base64
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from awsglue.dynamicframe import DynamicFrame
import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'OBJECT_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BucketName = args['BUCKET_NAME']
ObjectName = args['OBJECT_NAME']

s3 = boto3.client('s3')
sns = boto3.client('sns')
secret_manager = boto3.client("secretsmanager")

string_split = ObjectName.split("/") 
filename = string_split[1]
string_split1 = filename.split(".")
postgrestablename = string_split1[0]

if 'SAP' in ObjectName:
    column_names = ["BELNR", "CURRENCY", "CUSTOMER_ID", "EVENT_ALT_ID", "EVENT_ID", "EVENT_NM", "EVENT_PARENT_ID", "EVEENT_PROD_ID", "GLOBAL_ID", "HCO_ADRS_CITY", "HCO_ADRS_CNTRY_CD", "HCO_ADRS_LN_1", "HCO_ADRS_LN_2", "HCO_ADRS_PSTL_CD", "HCO_ADRS_RGN_CD", "HCO_ALT_NM", "HCO_ID", "HCO_NM", "HCO_TAX_ID", "HEALTH_CARE_ORG", "HEALTH_CARE_PROF", "INVC_TYPE", "MEETING_ID", "PATIENT_ORG", "PO_INFO", "PROD_DTL_PRCNT", "PROD_LLY_ID", "PROD_NM", "SPEND_AMT", "SPEND_DT_PD", "SPEND_ID", "SPEND_PURPOSR_CD", "SPEND_PURPOSE_SECONDARY_CD", "SPEND_TRAVEL_DETAILS_CITY", "SPEND_TRAVEL_DETAILS_CNTRY_CD", "SPEND_TRAVEL_DETAILS_ID", "SPEND_TRAVEL_DETAILS_RGN_CD", "SPEND_TYPE_CD", "TAX_CODE", "TAX RATE", "TRANSACTION_ID", "VAT_REG_NUM", "WEB_DR"]
elif 'TPO' in ObjectName:
    column_names = ["CLINICAL_TRIAL_ALIAS", "CURRENCY", "HCO_ADRS_CITY", "HCO_ADRS_CNTRY_CD", "HCO_ADRS_LN_1", "HCO_ADRS_LN_2", "HCO_ADRS_RGN_CD", "HCO_ID", "HCO_ID_TYP", "HCO_NM", "HCP_ADRS_CITY", "HCP_ADRS_CNTRY_CD", "HCP_ADRS_LN_1", "HCP_ADRS_LN_2", "HCP_ADRS_RGN_CD", "HCP_FRST_NM", "HCP_ID", "HCP_ID_TYP", "HCP_LAST_NM", "HCP_MDL_NM", "HCP_PRFSNL_DSGN_CD", "HCP_SFX", "SITE_ID", "SPEND_AMT", "SPEND_DT_PD", "SPEND_ID", "SPEND_PURPOSE_CD", "SPEND_TRAVEL_DETAILS_CITY", "SPEND_TRAVEL_DETAILS_CNTRY_CD", "SPEND_TRAVEL_DETAILS_RGN_CD","SPEND_TYP_CD", "SRC_SYS_CD"]

#postgresurl = "jdbc:postgresql://postgresql.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:5432/postgres"
#username = "postgres_admin"
#password = "password"

Topic_ARN = "arn:aws:sns:eu-west-1:646156652167:demo-sns"
secret_name = "postgres_secrets"
postgresdbname = "postgres"
Subject = "File Upload Error"
#postgrestablename = "SampleExcelsheet"

def publish_sns_notification(subject, message):
    sns_response = sns.publish(TopicArn = Topic_ARN, Message = message, Subject = subject)
    return sns_response['ResponseMetadata']['HTTPStatusCode']

def get_secrets(Secret_Name):
    try:
        get_secret_value_response = secret_manager.get_secret_value(
            SecretId=Secret_Name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

def ingest_data_in_postgres():
    postgres_node = DynamicFrame.fromDF(Excel_Sparkdf, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
        #                    "database": postgresdbname,
                            "dbtable": postgrestablename,
                            "user": username,
                            "password": password
                        }
    postgres = glueContext.write_dynamic_frame.from_options(postgres_node, connection_type = "postgresql", connection_options=write_postgre_options,transformation_ctx = "postgres")

def read_excel_from_s3():
    global Excel_Sparkdf
    try:
        s3_object = s3.get_object(Bucket=BucketName, Key=ObjectName)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise e
        elif e.response['Error']['Code'] == 'InvalidObjectState':
            raise e
    else:
        Excel_df = pd.read_excel(io.BytesIO(s3_object['Body'].read()))
        Excel_df = Excel_df.fillna('')
        df=Excel_df.applymap(str)
        Excel_Sparkdf = spark.createDataFrame(df)
        return Excel_Sparkdf

#Check table names 
def check_tablenames():
    query = "(select tablename from pg_catalog.pg_tables where schemaname != 'information_schema' and schemaname != 'pg_catalog') sampletable"
    df = spark.read \
            .format("jdbc") \
            .option("url", postgresurl) \
            .option("dbtable", query) \
            .option("user", username) \
            .option("password", password) \
            .load()
    if df.count() > 0:
        tables_list =  df.select('tablename').rdd.flatMap(lambda x: x).collect()
        return tables_list


secret_manager_data = get_secrets(secret_name)
rdssecrets= json.loads(secret_manager_data)
username = rdssecrets['username']
password = rdssecrets['password']
hostname = rdssecrets['host']
port = str(rdssecrets['port'])
postgresurl = "jdbc:postgresql://" + hostname + ":" + port + "/" + postgresdbname

tableslist = check_tablenames()


#Validating the Source data
#Checking the FileType
if "xlsx" in ObjectName:
    df = read_excel_from_s3()
    df.show()
    Record_count = df.count()
    column_list = list(df.columns)
    column_set = set(column_list)
#Checking the Count     
    if Record_count > 30000:
        Message = "Maximum number of rows exceeded in file submitted. Maximum allowed is 30,000 rows or records per file"
        snsresponse = publish_sns_notification(Subject, Message)
    if Record_count == 0:
        Message = "Error: Submitted file is empty, please upload a completed file"
        snsresponse = publish_sns_notification(Subject, Message)
    elif len(column_list) != len(column_names):
        Message = "Error: Number of columns are not correct. Please refer to the excel template link above for the correct number of columns and resubmit the file."
        snsresponse = publish_sns_notification(Subject, Message)
    elif column_list != column_names:
        Message = "Error: Column heading(s) does not match the template for this source. Please refer to the excel template link above for the correct column headers and resubmit the file"
        snsresponse = publish_sns_notification(Subject, Message)
    elif len(column_list) != len(column_set):
        Message = "Error: Column heading(s) has duplicates, it doesn't match template for this source Please refer the excel template link above for correct column headers & resubmit the file"
        snsresponse = publish_sns_notification(Subject, Message)
    elif postgrestablename in tableslist:
        Message = "Error: Duplicate file cannot be imported. Please submit file with unique name."
        snsresponse = publish_sns_notification(Subject, Message)
    else:
        ingest_data_in_postgres()
        Success_Subject = "File Uploaded Successfully"
        Success_Message = "File Uploaded Successfully without any error"
        snsresponse = publish_sns_notification(Success_Subject, Success_Message)
else:
    Message = "Error: File format is invalid. Please convert the file to excel (xlsx) and resubmit the file."
    snsresponse = publish_sns_notification(Subject, Message)
    

job.commit()
