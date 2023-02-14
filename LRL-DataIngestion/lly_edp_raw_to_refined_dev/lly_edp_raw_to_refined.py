"""Module Provides required functions"""
import io
import sys
import json
import base64
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
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
secret_manager = boto3.client("secretsmanager")
string_split = ObjectName.split("/")
filename = string_split[1]
string_split1 = filename.split(".")
uploaded_file_name = string_split1[0]
postgres_tablename = uploaded_file_name.split(".", 1)[0]
prefix_name = ObjectName.split("/", 1)[0]
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
def ingest_excel_data_in_postgres():
    """This Function ingestion data to postgresdb"""
    postgrestablename = schema + "." + postgres_tablename
    read_excel_from_s3_df = read_excel_from_s3()
    excel_sparkdf = spark.createDataFrame(read_excel_from_s3_df)
    postgres_ingestion = excel_sparkdf.write.format("jdbc") \
        .option("url", postgresurl) \
        .option("dbtable", postgrestablename) \
        .option("user", username) \
        .option("password", password) \
        .option("truncate", "true") \
        .mode("overwrite").save()
    return postgres_ingestion
def ingest_csv_data_to_postgres():
    """This Function ingestion data to postgresdb"""
    postgrestablename = schema + "." + postgres_tablename
    read_csv_from_s3_dynamicframe = read_csv_from_s3()
    read_csv_from_s3_df = read_csv_from_s3_dynamicframe.toDF()
    postgres_ingestion = read_csv_from_s3_df.write.format("jdbc") \
        .option("url", postgresurl) \
        .option("dbtable", postgrestablename) \
        .option("user", username) \
        .option("password", password) \
        .option("truncate", "true") \
        .mode("overwrite").save()
    return postgres_ingestion
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
def read_csv_from_s3():
    """This function reads data from s3"""
    read_csv_from_s3bucket = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": ["s3://" + BucketName + "/" + ObjectName],
            "recurse": True,
        },
        transformation_ctx="read_csv_from_s3bucket",
    )
    return read_csv_from_s3bucket
#Fetching Credentials from SecretManager
secret_manager_data = get_secrets()
rdssecrets= json.loads(secret_manager_data)
username = rdssecrets['RDS_USERNAME']
password = rdssecrets['RDS_PASSWORD']
hostname = rdssecrets['RDS_HOST']
postgresdbname = rdssecrets['RDS_DBNAME']
schema = rdssecrets['RDS_SCHEMA']
postgresurl = "jdbc:postgresql://" + hostname + ":" + \
    str(rdssecrets['RDS_PORT']) + "/" + postgresdbname
#Validations before Ingesting data into PostgresDB
#Checking the FileType in S3 Bucket
def main():
    """This is main function"""
    if "xlsx" in ObjectName:
        read_excel_from_s3_df = read_excel_from_s3()
        print(read_excel_from_s3_df)
        try:
            ingest_excel_data_in_postgres()
        except ClientError as ingestion_error:
            message = "Error: GTConnect DB Connection Failed"
            print(message)
            raise ingestion_error
        else:
            message = "File Uploaded Successfully without any error"
            print(message)
    elif "csv" in ObjectName:
        read_csv_from_s3_df = read_csv_from_s3()
        print(read_csv_from_s3_df)
        try:
            ingest_csv_data_to_postgres()
        except ClientError as ingestion_error:
            message = "Error: GTConnect DB Connection Failed"
            print(message)
            raise ingestion_error
        else:
            message = "File Uploaded Successfully without any error"
            print(message)
    else:
        message = "Error: File format is invalid. Please " \
        "convert the file to excel (xlsx) or CSV and resubmit the file."
        print(message)
main()
job.commit()
