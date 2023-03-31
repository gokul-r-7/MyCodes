"""Module Provides required functions"""
# pylint: disable=import-error
# pylint: disable=wildcard-import
import io
import sys
import json
import base64
import uuid
from datetime import datetime
import pytz
import boto3
import psycopg2
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
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
ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
string_split = ObjectName.split("/")
uploaded_file_name = string_split[2]
IST = pytz.timezone("Asia/Kolkata")
file_uploaded_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
EVENT_ID = str(uuid.uuid4())
SPEND_ID = str(uuid.uuid4())
def publish_ses_notification(subject,message):
    """This funtion sends notification to SES"""
    ses_response = ses.send_email(
        Source = source_mail,
        SourceArn = source_arn,
        Destination = {
        'ToAddresses' : destinationemail[0],
        },
        Message={
        'Body': {
            'Text': {
                'Charset': 'UTF-8',
                'Data': message,
            },
        },
        'Subject': {
            'Charset': 'UTF-8',
            'Data': subject,
        }
    })
    return ses_response['ResponseMetadata']['HTTPStatusCode']
#Getting Credentials from SecretManager
def get_secrets(secretname):
    """This function gets data from secret manager"""
    try:
        get_secret_value_response = secret_manager.get_secret_value(
            SecretId=secretname
        )
    except ClientError as secret_error:
        raise secret_error
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return secret
    decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return decoded_binary_secret
#Ingesting data to GTConnect PostgresDB
def ingest_data_in_postgres():
    """This Function ingestion data to postgresdb"""
    if "SAP" in ObjectName:
        postgrestable = "sap_transactions_temp"
        read_excel_from_s3_df = read_excel_from_s3()
    elif "TPO" in ObjectName:
        postgrestable = "tpo_transactions_temp"
        read_excel_from_s3df = read_excel_from_s3()
        read_excel_from_s3_df = read_excel_from_s3df.applymap(str)
    else:
        postgrestable = "upload_transactions_temp"
        read_excel_from_s3df = read_excel_from_s3()
        read_excel_from_s3df.columns = column_names
        read_excel_from_s3_df = read_excel_from_s3df.applymap(str)
    postgrestablename = schema + "." + postgrestable
    print(read_excel_from_s3_df)
    print(read_excel_from_s3_df.info())
    excel_sparkdf = spark.createDataFrame(read_excel_from_s3_df)
    excel_sparkdf = excel_sparkdf.withColumn("upload_transaction_file_id", lit(job_id[0]))
    excel_sparkdf = excel_sparkdf.withColumn("EVENT_ID", lit(EVENT_ID))
    excel_sparkdf = excel_sparkdf.withColumn("SPEND_ID", lit(SPEND_ID))
    excel_sparkdf = excel_sparkdf.withColumn("source_id", lit(source_id[0]))
    excel_sparkdf = excel_sparkdf.withColumn("country_id", lit(country_id[0]))
    excel_sparkdf = excel_sparkdf.withColumn("created_by", lit(created_by[0]))
#    excel_sparkdf = excel_sparkdf.withColumn("updated_by", lit(updated_by[0]))
    excel_sparkdf = excel_sparkdf.withColumn("created_at", lit(created_at[0]))
    excel_sparkdf = excel_sparkdf.withColumn("updated_at", lit(updated_at[0]))
    if postgrestable == "tpo_transactions_temp":
        excel_sparkdf =  excel_sparkdf.withColumn("spend_amt",\
            excel_sparkdf["spend_amt"].cast('float'))
    elif postgrestable == "upload_transactions_temp":
        integer_columns = ['event_created_by_id', 'event_alternate_id', 'event_address_id',\
        'event_address_source_id','event_customer_id','event_customer_source_id',\
            'event_customer_alt_id1', 'spend_travel_detail_id']
        excel_columns = excel_sparkdf.columns
        column_integers = [cols for cols in excel_columns if cols \
                            in integer_columns]
        if len(column_integers) != 0:
            excel_sparkdf = excel_sparkdf.select([excel_sparkdf.cast(IntegerType()) \
                .alias(col) for col in column_integers])
    excel_sparkdf.show()
    excel_sparkdf.printSchema()
    postgres_node = DynamicFrame.fromDF(excel_sparkdf, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
                            "dbtable": postgrestablename,
                            "user": username,
                            "password": password,
                            "customJdbcDriverS3Path": \
                                "s3://lly-gtconnect-certs-dev/postgresql-42.5.4.jar",
                            "customJdbcDriverClassName": "org.postgresql.Driver"
                        }
    postgres_ingestion = glueContext.write_dynamic_frame.from_options(postgres_node, \
        connection_type = "postgresql", connection_options=write_postgre_options, \
        transformation_ctx = "postgres")
    return postgres_ingestion
#Inserting entries in FileUpload Metadata table
def add_data_in_fileuploadmetadata(postgres_update_query):
    """This functions puts entries in FileUploadMetadatatable"""
    postgres_connection = None
    try:
        postgres_connection = psycopg2.connect(user = username, password = password, \
        host = hostname, port = 5432, database = postgresdbname)
    except psycopg2.Error as postgres_error:
        return postgres_error
    cursor = postgres_connection.cursor()
    cursor.execute(postgres_update_query)
    postgres_connection.commit()
    postgres_connection.close()
    return postgres_connection
def get_postgres_data(postgrestable):
    """This function helps to read the postgres table"""
    postgrestablename = schema + "." + postgrestable
    connection_options_pg = {
        "url": postgresurl,
        "dbtable": postgrestablename,
        "user": username,
        "password": password,
        "customJdbcDriverS3Path": "s3://lly-gtconnect-certs-dev/postgresql-42.5.4.jar",
        "customJdbcDriverClassName": "org.postgresql.Driver"}
    dynamic_frame=glueContext.create_dynamic_frame.from_options(
        connection_type='postgresql',
        connection_options = connection_options_pg
        )
    get_postgres_data_df = dynamic_frame.toDF()
    return get_postgres_data_df
#Reading Excelfile data from S3 bucket
def read_excel_from_s3():
    """This function reads data from s3"""
    try:
        s3_object = s3.get_object(Bucket=BucketName, Key=ObjectName)
    except ClientError as s3_error:
        raise s3_error
    excel_df = pd.read_excel(io.BytesIO(s3_object['Body'].read()))
    excel_df = excel_df.fillna('')
    return excel_df
try:
    #Fetching Credentials from SecretManager
    secret_manager_data = get_secrets("GTConnectRdsSecretforAppUser")
    rdssecrets= json.loads(secret_manager_data)
    username = rdssecrets['RDS_USERNAME']
    password = rdssecrets['RDS_PASSWORD']
    hostname = rdssecrets['RDS_HOST']
    schema = rdssecrets['RDS_SCHEMA']
    postgresdbname = rdssecrets['RDS_DBNAME']
    postgresurl = "jdbc:postgresql://" + hostname + ":" + \
        str(rdssecrets['RDS_PORT']) + "/" + postgresdbname
    email_secret_data = get_secrets("GTConnectEmailSecrets")
    emailsecrets= json.loads(email_secret_data)
    source_mail = emailsecrets['SOURCE_EMAIL']
    source_arn = emailsecrets['SOURCE_ARN']
    upload_transaction_df = get_postgres_data("upload_transaction_files")
    job_id = upload_transaction_df.select('id').where(upload_transaction_df.\
                file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    source_id = upload_transaction_df.select('source_id').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    country_id = upload_transaction_df.select('country_id').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    created_by = upload_transaction_df.select('created_by').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    updated_by = upload_transaction_df.select('updated_by').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    created_at = upload_transaction_df.select('created_at').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    updated_at = upload_transaction_df.select('updated_at').where(\
        upload_transaction_df.file_name == uploaded_file_name).rdd.flatMap(lambda x: x).collect()
    print(source_id[0],country_id[0],created_by[0],updated_by[0],created_at[0],updated_at[0])
    users_df = get_postgres_data("users")
    destinationemail = users_df.select('email').where(users_df.id == \
                        created_by[0]).rdd.flatMap(lambda x: x).collect()
    print(destinationemail[0])
    user_name_field = users_df.select('user_name').where(users_df.id == \
                        created_by[0]).rdd.flatMap(lambda x: x).collect()
    user_name = user_name_field[0].split('-')
    print(user_name[0])
    check_tables_names_df = upload_transaction_df.select("file_name").\
        where(upload_transaction_df.status == "Success")
    if check_tables_names_df.count() > 0:
        tables_list =  check_tables_names_df.select \
            ('file_name').rdd.flatMap(lambda x: x).collect()
    else:
        tables_list = []
    if 'SAP' in ObjectName:
        sap_transaction_df = get_postgres_data("sap_transactions_temp")
        sap_columns = sap_transaction_df.columns
        ignore_columns = ['id','source_id', 'country_id',\
        'upload_transaction_file_id','created_by', 'updated_by',\
        'created_at', 'updated_at']
        column_names = [sap_column for sap_column in sap_columns \
            if sap_column not in ignore_columns]
        print(column_names)
    elif 'TPO' in ObjectName:
        tpo_transaction_df = get_postgres_data("tpo_transactions_temp")
        tpo_columns = tpo_transaction_df.columns
        ignore_columns = ['id','source_id', 'country_id',\
        'upload_transaction_file_id','created_by', 'updated_by',\
        'created_at', 'updated_at', 'EVENT_ID']
        column_names = [tpo_column for tpo_column in tpo_columns \
            if tpo_column not in ignore_columns]
        print(column_names)
    else:
        rel_source_manual_data_fields_df = get_postgres_data("rel_source_manual_data_fields")
        manual_source_data_fields = rel_source_manual_data_fields_df.\
            select('manual_source_data_field_id').\
        where(rel_source_manual_data_fields_df.source_id == source_id[0]).where(\
        rel_source_manual_data_fields_df.manual_custom_label_id.isNotNull()).\
            rdd.flatMap(lambda x: x).collect()
        print(manual_source_data_fields)
        lov_manual_source_data_fields_df = get_postgres_data("lov_manual_source_data_fields")
        columnnameslist = lov_manual_source_data_fields_df.select("column_name").filter(\
        lov_manual_source_data_fields_df.id.isin(manual_source_data_fields)).\
            rdd.flatMap(lambda x: x).collect()
        column_names = []
        for columnnames in columnnameslist:
            COLUMN_VARIABLE = (''.join(['_' + columns if columns.isupper() \
                                   else columns for columns in columnnames]).lstrip())
            column_names.append(COLUMN_VARIABLE)
        print(column_names)
except Exception as error_log: # pylint: disable=broad-except
    update_query_error_log = "UPDATE gtconnect.upload_transaction_files SET status = '" \
        + "Failure" + "', failure_reason = '" + str(error_log) + \
            "' WHERE file_name = '" + uploaded_file_name + "'"
    add_data_in_fileuploadmetadata(update_query_error_log)
#Validations before Ingesting data into PostgresDB
def main(): #pylint: disable=too-many-statements
    """This is main function"""
    try:
        status = 'Failure'
        subject = "File Upload Error"
        if "xlsx" in ObjectName:
            read_excel_from_s3_df = read_excel_from_s3()
            total_records_in_file = read_excel_from_s3_df.shape[0]
            if uploaded_file_name in {'SAP', 'TPO'}:
                column_list = list(read_excel_from_s3_df.columns.values)
                column_set = set(column_list)
            else:
                read_excel_from_s3_df.columns = column_names
                column_list = list(read_excel_from_s3_df.columns.values)
                column_set = set(column_list)
        #Checking the Total  Number Records in a Excel File
            if total_records_in_file == 0 or total_records_in_file > 30000:
                message = uploaded_file_name + " Error: Number of rows " \
                "should be more than 0 and less then 30,000 in the file"
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" has Error: Number of rows " \
                "should be more than 0 and less then 30,000 in the file.\n\n" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
        #Checking Number of ColumnNames in Excel Files & FileUpload Template
            elif len(column_list) != len(column_names):
                message = uploaded_file_name + " Error: Number of columns " \
                "are not correct Please refer the excel template link above for " \
                "the correct number of columns and resubmit the file."
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" has Error: Number of columns " \
                "are not correct Please refer the excel template link above for " \
                "the correct number of columns and resubmit the file.\n\n" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
        #Checking the ColumnNames in Excel Files and FileUpload Template
            elif column_list != column_names:
                message = uploaded_file_name + " Error: Column heading(s)" \
                "does not match the template for this source. Please refer to the " \
                "excel template link above for the correct " \
                "column headers and resubmit the file"
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" Error: Column heading(s)" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
        #Checking for Duplicate ColumnNames in Excel Files
            elif len(column_list) != len(column_set):
                message = uploaded_file_name + " Error: Column heading(s)" \
                "has duplicates it doesn't match template for this source Please" \
                "refer the excel template link above for correct " \
                "column headers & resubmit the file"
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" is uploaded successfully.\n\n" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
        #Checking for If the Excel file data is already there in GTConnect PostgresDB
            elif uploaded_file_name in tables_list:
                message = uploaded_file_name + " Error: Duplicate file cannot be "\
                "imported. Please submit file with unique name."
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" has Error: Duplicate file cannot be "\
                "imported. Please submit file with unique name.\n\n" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
            else:
        #Ingesting ExcelFile  Data to GTConnect PostgresDB
                try:
                    ingest_data_in_postgres()
                except Exception as ingestion_error: # pylint: disable=broad-except
                    message = uploaded_file_name + ingestion_error
                    sesresponse = publish_ses_notification(subject, message)
                    email_message = "Hi "+user_name[0]+",\n\n" \
                    "Your File "+uploaded_file_name+" has Error"+ingestion_error+".\n\n" \
                    "Regards,\n" \
                    +user_name[0]+"."
                    sesresponse = publish_ses_notification(subject,email_message)
                    raise ingestion_error
                status = 'Success'
                subject = "File Uploaded Successful"
                message = uploaded_file_name + " File Uploaded Successfully" \
                " without any error"
                email_message = "Hi "+user_name[0]+",\n\n" \
                "Your File "+uploaded_file_name+" is uploaded successfully.\n\n" \
                "Regards,\n" \
                +user_name[0]+"."
                sesresponse = publish_ses_notification(subject,email_message)
                print(sesresponse)
        else:
            message = uploaded_file_name + " Error: File format is invalid. Please " \
            "convert the file to excel (xlsx) and resubmit the file."
            email_message = "Hi "+user_name[0]+",\n\n" \
            "Your File "+uploaded_file_name+" Error: File format is invalid. Please " \
            "convert the file to excel (xlsx) and resubmit the file.\n\n" \
            "Regards,\n" \
            +user_name[0]+"."
            sesresponse = publish_ses_notification(subject,email_message)
            total_records_in_file = 0
        update_query = "UPDATE gtconnect.upload_transaction_files SET status = '" \
            + status + "', failure_reason = '" + message + \
                "' WHERE file_name = '" + uploaded_file_name + "'"
        add_data_in_fileuploadmetadata(update_query)
    except Exception as error_logs: # pylint: disable=broad-except
        update_query_error_logs = "UPDATE gtconnect.upload_transaction_files SET status = '" \
                + status + "', failure_reason = '" + str(error_logs) + \
                    "' WHERE file_name = '" + uploaded_file_name + "'"
        add_data_in_fileuploadmetadata(update_query_error_logs)
main()
job.commit()