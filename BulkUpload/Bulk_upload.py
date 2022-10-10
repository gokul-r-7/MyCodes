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
secret_manager = boto3.client("secretsmanager")



column_names = ["Id", "First Name", "Last Name", "Gender", "Country", "Age", "Date", "Salary"]


#postgresurl = "jdbc:postgresql://postgresql.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:5432/postgres"
#username = "postgres_admin"
#password = "password"

secret_name = "postgres_secrets"
postgresdbname = "postgres"
postgrestablename = "sample_excel_sheet"


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
        df=Excel_df.applymap(str)
        Excel_Sparkdf = spark.createDataFrame(df)
        return Excel_Sparkdf


#Validating the Source data
#Checking the FileType
if "xlsx" in ObjectName:
    df = read_excel_from_s3()
    df.show()
    Record_count = df.count()
    column_list = list(df.columns)
    column_set = set(column_list)
#Checking the Count     
    if Record_count > 90000:
        print("Maximum number of rows exceeded in file submitted. Maximum allowed is 30,000 rows or records per file")
    elif Record_count == 0:
        print("Error: Submitted file is empty, please upload a completed file")
    elif len(column_list) != len(column_names):
        print("Error: Number of columns are not correct. Please refer to the excel template link above for the correct number of columns and resubmit the file.")
    elif column_list != column_names:
        print("Error: Column heading(s) does not match the template for this source. Please refer to the excel template link above for the correct column headers and resubmit the file")
    elif len(column_list) != len(column_set):
        print("Error: Column heading(s) has duplicates, it doesn't match template for this source Please refer the excel template link above for correct column headers & resubmit the file")
    else:
        secret_manager_data = get_secrets(secret_name)
        rdssecrets= json.loads(secret_manager_data)
        username = rdssecrets['username']
        password = rdssecrets['password']
        hostname = rdssecrets['host']
        port = str(rdssecrets['port'])
        postgresurl = "jdbc:postgresql://" + hostname + ":" + port + "/" + postgresdbname
        ingest_data_in_postgres()
else:
    print("Error: File format is invalid. Please convert the file to excel (xlsx) and resubmit the file.")
    




    







job.commit()