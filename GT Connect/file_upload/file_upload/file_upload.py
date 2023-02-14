"""Module Provides required functions"""
import io
import sys
import json
import base64
from datetime import datetime
import pytz
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from botocore.exceptions import ClientError
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
#Initializing AWS Services
s3 = boto3.client('s3')
sns = boto3.client('sns')
secret_manager = boto3.client("secretsmanager")
string_split = ObjectName.split("/")
filename = string_split[1]
string_split1 = filename.split(".")
uploaded_file_name = string_split1[0]
IST = pytz.timezone("Asia/Kolkata")
file_uploaded_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
#Sending SNS Notification to USER
def publish_sns_success_notification(message):
    """This funtion sends notification to SNS"""
    topic_arn = "arn:aws:sns:ap-south-1:673573194392:gtconnect-sns"
    subject = "File Uploaded Successfully"
    sns_response = sns.publish(TopicArn = topic_arn, Message = message, Subject = subject)
    return sns_response['ResponseMetadata']['HTTPStatusCode']
def publish_sns_failure_notification(message):
    """This funtion sends notification to SNS"""
    topic_arn = "arn:aws:sns:ap-south-1:673573194392:gtconnect-sns"
    subject = "File Upload Error"
    sns_response = sns.publish(TopicArn = topic_arn, Message = message, Subject = subject)
    return sns_response['ResponseMetadata']['HTTPStatusCode']
#Getting Credentials from SecretManager
def get_secrets():
    """This function gets data from secret manager"""
    secretname = "RdsSecretforGtConnectAppUser"
    try:
        get_secret_value_response = secret_manager.get_secret_value(
            SecretId=secretname
        )
    except ClientError as secret_error:
        raise secret_error
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether secret string or binary one of fields populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret
#Ingesting data to GTConnect PostgresDB
def ingest_data_in_postgres():
    """This Function ingestion data to postgresdb"""
    postgrestablename = schema + "." + 'source_transaction_data'
    read_excel_from_s3_df = read_excel_from_s3()
    source_transaction_data_schema = source_transaction_data_df.schema
    excel_sparkdf = spark.createDataFrame(read_excel_from_s3_df, \
                        source_transaction_data_schema)
    postgres_node = DynamicFrame.fromDF(excel_sparkdf, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
                            "dbtable": postgrestablename,
                            "user": username,
                            "password": password
                        }
    postgres_ingestion = glueContext.write_dynamic_frame.from_options(postgres_node, \
        connection_type = "postgresql", connection_options=write_postgre_options, \
        transformation_ctx = "postgres")
    return postgres_ingestion
#Inserting entries in FileUpload Metadata table
def add_data_in_fileuploadmetadata(file_upload_metadata_df,):
    """This functions puts entries in FileUploadMetadatatable"""
    metadata_table = schema + "." + 'file_upload_metadata'
    postgres_node = DynamicFrame.fromDF(file_upload_metadata_df, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
                            "dbtable": metadata_table,
                            "user": username,
                            "password": password
                        }
    postgres_metadata_entries = glueContext.write_dynamic_frame.from_options(postgres_node, \
        connection_type = "postgresql", connection_options=write_postgre_options, \
        transformation_ctx = "postgres")
    return postgres_metadata_entries
#Reading Excelfile data from S3 bucket
def read_excel_from_s3():
    """This function reads data from s3"""
    try:
        s3_object = s3.get_object(Bucket=BucketName, Key=ObjectName)
    except ClientError as s3_error:
        raise s3_error
    else:
        excel_df = pd.read_excel(io.BytesIO(s3_object['Body'].read()))
        excel_df = excel_df.fillna('')
        return excel_df
#Checking Filenames in FileUpload Metadata table
def check_tablenames():
    """This function checks tablenames"""
    empty_tablelist = []
    query = '(select "uploaded_file_name" from \
    gtconnect.file_upload_metadata where "status" = ' +  "'Success') sampletable"
    check_tablenames_df = spark.read \
            .format("jdbc") \
            .option("url", postgresurl) \
            .option("dbtable", query) \
            .option("user", username) \
            .option("password", password) \
            .load()
    if check_tablenames_df.count() > 0:
        tables_list =  check_tablenames_df.select \
        ('uploaded_file_name').rdd.flatMap(lambda x: x).collect()
        return tables_list
    return empty_tablelist
def get_source_transcation_data():
    """This Function reads source transaction table"""
    postgrestablename = schema + "." + 'source_transaction_data'
    get_source_transcation_data_df = spark.read \
            .format("jdbc") \
            .option("url", postgresurl) \
            .option("dbtable", postgrestablename) \
            .option("user", username) \
            .option("password", password) \
            .load()
    return get_source_transcation_data_df
#Fetching Credentials from SecretManager
secret_manager_data = get_secrets()
rdssecrets= json.loads(secret_manager_data)
username = rdssecrets['RDS_USERNAME']
password = rdssecrets['RDS_PASSWORD']
hostname = rdssecrets['RDS_HOST']
schema = rdssecrets['RDS_SCHEMA']
postgresdbname = rdssecrets['RDS_DBNAME']
postgresurl = "jdbc:postgresql://" + hostname + ":" + \
    str(rdssecrets['RDS_PORT']) + "/" + postgresdbname
tableslist = check_tablenames()
source_transaction_data_df = get_source_transcation_data()
column_names = source_transaction_data_df.columns
#Validations before Ingesting data into PostgresDB
#Checking the FileType in S3 Bucket
def main():
    """This is main function"""
    status = 'Failure'
    source_code = ''
    source_description = ''
    if "xlsx" in ObjectName:
        read_excel_from_s3_df = read_excel_from_s3()
        print(read_excel_from_s3_df)
        total_records_in_file = read_excel_from_s3_df.shape[0]
        column_list = list(read_excel_from_s3_df.columns.values)
        column_set = set(column_list)
    #Checking the Total  Number Records in a Excel File
        if total_records_in_file == 0 or total_records_in_file > 30000:
            message = "Error: Number of rows should be more than " \
            "0 and less then 30,000 in the file"
            snsresponse = publish_sns_failure_notification(message)
    #Checking Number of ColumnNames in Excel Files & FileUpload Template
        elif len(column_list) != len(column_names):
            message = "Error: Number of columns are not correct. " \
            "Please refer the excel template link above for " \
            "the correct number of columns and resubmit the file."
            snsresponse = publish_sns_failure_notification(message)
    #Checking the ColumnNames in Excel Files and FileUpload Template
        elif column_list != column_names:
            message = "Error: Column heading(s) does not match " \
            "the template for this source. Please refer to the "  \
            "excel template link above for the correct " \
            "column headers and resubmit the file"
            snsresponse = publish_sns_failure_notification(message)
    #Checking for Duplicate ColumnNames in Excel Files
        elif len(column_list) != len(column_set):
            message = "Error: Column heading(s) has duplicates, " \
            "it doesn't match template for this source Please " \
            "refer the excel template link above for correct " \
            "column headers & resubmit the file"
            snsresponse = publish_sns_failure_notification(message)
    #Checking for If the Excel file data is already there in GTConnect PostgresDB
        elif uploaded_file_name in tableslist:
            message = "Error: Duplicate file cannot be "\
            "imported. Please submit file with unique name."
            snsresponse = publish_sns_failure_notification(message)
        else:
    #Ingesting ExcelFile  Data to GTConnect PostgresDB
            try:
                ingest_data_in_postgres()
            except ClientError as ingestion_error:
                message = "Error: GTConnect DB Connection Failed"
                snsresponse = publish_sns_failure_notification(message)
                raise ingestion_error
            else:
                status = 'Success'
                message = "File Uploaded Successfully without any error"
                snsresponse = publish_sns_success_notification(message)
                print(snsresponse)
    else:
        message = "Error: File format is invalid. Please " \
        "convert the file to excel (xlsx) and resubmit the file."
        snsresponse = publish_sns_failure_notification(message)
        total_records_in_file = 0
    if "xlsx" in ObjectName and 'source_system_cd' in column_list and \
    len(list(read_excel_from_s3_df['source_system_cd'])) > 0:
        source_system_cd_list = list(read_excel_from_s3_df['source_system_cd'])
        source_code = source_system_cd_list[0]
        source_description = source_system_cd_list[0]
    file_upload_metadata_columns = ["uploaded_file_name", \
    "file_uploaded_time", "total_records_in_file", "source_code", \
    "source_description", "status", "failure_reason"]
    file_upload_metadata_data = [(uploaded_file_name, \
    file_uploaded_time, total_records_in_file, source_code, \
    source_description, status, message)]
    file_upload_metadata_df = spark.createDataFrame(data = \
        file_upload_metadata_data, schema = file_upload_metadata_columns)
    add_data_in_fileuploadmetadata(file_upload_metadata_df)
main()
job.commit()
