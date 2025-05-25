import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as t
from pyspark.sql import Window
import yaml
import os
from worley_helper.configuration.config import Configuration
from worley_helper.spark.runner import SparkDataRunner
from worley_helper.transformations import Transforms
from worley_helper.utils.constants import REGION, TIMESTAMP_FORMAT, TIMEZONE_UTC
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.aws import S3, DynamoDB, SecretsManager
from worley_helper.utils.email_helpers import send_email, generate_generic_success_body, generate_generic_failure_body, generate_generic_warning_body
import traceback
from worley_helper.utils.logger import get_logger
import requests
from io import StringIO, BytesIO
import json
import datetime
import time
import pandas as pd
from datetime import datetime, timezone

logger = get_logger(__name__)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_system_id",
        "function_name",
        "metadata_table_name",
        "project_id",
        "environment"
    ],
)

def execute_import_api(url, headers, csv_buffer, ImportType, UpdateOnly, file_name="upload.csv") -> bool | str:
    try:
        logger.info(f"Create import url : {url}")
        # Make sure buffer is at the beginning
        csv_buffer.seek(0)

        body = {
            'ImportType': ImportType,
            'UpdateOnly': UpdateOnly,
            'File': f'@{file_name};type=text/csv'
        }

        files = {
            'file': (file_name, csv_buffer, 'text/csv')
        }

        resp = requests.post(url=url, headers=headers, data=body, files=files)
        resp_json = resp.json()
        import_id = resp_json.get('id', 0)
        if resp.status_code != 200:
            raise Exception(f"Error: {resp.status_code} - {resp_json.get('message', 'Unknown error')}")
        else:
            return True, import_id

    except Exception as ex:
        traceback.format_exc(ex)
        return False, 0

def get_import_messages(import_id, import_status_url, headers):
    resp = requests.get(import_status_url+'/messages', headers=headers)
    if resp.status_code == 200:
        messages = resp.json()['data']
    else:
        messages = resp.json()['Errors']
    return messages

def get_import_status(import_id, import_status_url, headers):
    import_url = import_status_url.format(import_id)
    logger.info(f"Import URL : {import_url}")
    
    resp = requests.get(import_url, headers=headers)
    resp_json = resp.json()
    logger.info(resp_json)
    message = {
                'import_id' : import_id
              }
    if resp.status_code == 200:
        message['Status'] = resp_json.get('Status')
        message['DateModified'] = resp_json.get('DateModified')
        message['CreatedByUser'] = resp_json.get('CreatedByUser')
        message['Progress'] = resp_json.get('Progress')
        message['SuccessCount'] = resp_json.get('SuccessCount')
        message['ErrorCount'] = resp_json.get('ErrorCount')
        message['WarningCount'] = resp_json.get('WarningCount')
        message['ErrorMessage'] = get_import_messages(import_id, import_url, headers)
    else:
        message['Status'] = 'Failed'
        message['ErrorMessage'] = resp_json
    return message

try:
    logger.info("Setting up the job args")

    source_system_id = args.get("source_system_id")
    function_name = args.get("function_name")
    metadata_table_name = args.get("metadata_table_name")
    job_name = args["JOB_NAME"]
    job_run_id = args["JOB_RUN_ID"]
    project_id = args["project_id"]
    current_env = args.get("environment")

    logger.info("Job args are set up")
    logger.info("Job Name: " + job_name)
    logger.info("project_id --> " + project_id)

    input_keys = "api#" + source_system_id + "#" + function_name
    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_system_id, metadata_type=input_keys
    )
        
    logger.info(f" Metadata Response :{metadata}")

    url = metadata['api_parameters']['api_host'].format(project_id)
    import_type = metadata['job_parameters']['import_type']
    update_only = metadata['job_parameters']['update_only']
    import_url = metadata['api_parameters']['import_status_api_host']

    o3_database = metadata['job_parameters']['o3_curated_db']
    o3_iwps_table = metadata['job_parameters']['o3_iwps_table']
    o3_warehouse = metadata['job_parameters']['o3_warehouse']
    jplus_database = metadata['job_parameters']['jplus_curated_db']
    jplus_timecard_table = metadata['job_parameters']['jplus_timecard_table']
    jplus_warehouse = metadata['job_parameters']['jplus_warehouse']
    
    sender_email = metadata["sender_email"]
    receiver_email = metadata["receiver_email"]
    receiver_email = receiver_email if isinstance(metadata["receiver_email"], list) else list(receiver_email)
    smtp_server = metadata["smtp_server"]
    smtp_port = metadata["smtp_port"]

    raw_bucket = metadata['job_parameters']['raw_bucket']
    audit_folder_location = metadata['job_parameters']['audit_folder_location']
    kms_key_id = metadata['job_parameters']['kms_key_id']

    allowed_final_status = ["Success", "Failed"]
    max_duration_seconds = 120
    poll_interval_seconds = 5


    logger.info("==================Fetching DB Secrets ============")
    sm = SecretsManager(default_region=REGION)
    secret_params = json.loads(sm.get_secret(metadata['api_parameters']['secret_key']))
    APIKey = secret_params.get("APIKey")

    headers = {"APIKey" : APIKey}
    logger.info("==================Fetching DB Secrets : Success ============")

    audit_date = generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(TIMESTAMP_FORMAT)

    logger.info("Audit Date : " + str(audit_date))

    s3_client = S3(raw_bucket, REGION)
    folder_name=f"{audit_folder_location}"
    file_name=f"{source_system_id}_{function_name}_{str(project_id)}.txt"
    content,file_status=s3_client.read_s3_file(folder_name,file_name)

    logger.info("Audit File Existence Status : " + str(file_status))
    logger.info("Audit File Content : " + str(content))
    if file_status:
        incremental_date_project_file=content.split(',')[3]
        incremental_from_date=incremental_date_project_file
        logger.info("incremental_date_project_file --> " + incremental_date_project_file)
    else:
        incremental_from_date = '1900-01-01 00:00:00'
        logger.info("incremental_date_project_file not found, using default date as incremental date")
        logger.info("incremental_from_date --> " + incremental_from_date)


except Exception as e:
    logger.error("Error Occured at setting up the job args " + str(e))
    logger.error(traceback.format_exc())
    raise e

try:
    logger.info("Setting up the spark job")

    conf = (
            SparkConf()
            .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.sources.partitionOverwriteMode","dynamic")
            .set("spark.sql.catalog.glue_catalog_a", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.glue_catalog_a.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.sql.catalog.glue_catalog_a.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .set("spark.sql.catalog.glue_catalog_a.warehouse", o3_warehouse)
            .set("spark.sql.catalog.glue_catalog_b", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.glue_catalog_b.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.sql.catalog.glue_catalog_b.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .set("spark.sql.catalog.glue_catalog_b.warehouse", jplus_warehouse)
        )

    logger.info("Spark Configuration is set up")
    logger.info("Creating Spark Context")
    logger.info("Creating Glue Context")

    sc = SparkContext(conf=conf).getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    logger.info("Spark Context and Glue Context are created")
    logger.info("Setting up the job")
    job.init(job_name + source_system_id + function_name, args)
except Exception as e:
    traceback.format_exc("Error Occured at setting up the spark session " + str(e))
    raise e


try:
    logger.info("Reading the data from iceberg tables")

    df_jplus = spark.read.format("iceberg").load(f"glue_catalog_b.{jplus_database}.{jplus_timecard_table}")
    df_jplus = df_jplus.filter(f"execution_date >= '{incremental_from_date}'")
    jplus_count = df_jplus.count()
    logger.info("JPlus Count : " + str(jplus_count))

    if jplus_count != 0:

        df_iwps = spark.read.format("iceberg").load(f"glue_catalog_a.{o3_database}.{o3_iwps_table}")
        df_iwps = df_iwps.filter(f"is_current = 1 and projectid = {project_id}")
        df_iwps = df_iwps.selectExpr("projectid","projectcode","name","cast(coalesce(ActualHours,'0') as float) as ActualHours", "datemodified_ts")
        windowSpec = Window.partitionBy("projectid","projectcode","name").orderBy(F.desc("datemodified_ts"))
        df_iwps = df_iwps.withColumn("rn", F.row_number().over(windowSpec)).where("rn=1").select("projectid","projectcode","name","ActualHours")
    
        df_iwps.printSchema()
        df_iwps.show(truncate=False)

        df_jplus = df_jplus.select("project_num","fiwp","timecard_date","st_hours", "ot_hours").withColumn("timecard_date", F.to_date(F.col("TIMECARD_DATE"),"MMM dd, yyyy"))
        df_jplus = df_jplus.groupBy("project_num","fiwp").agg((F.sum("st_hours") + F.sum("ot_hours")).alias("ActualHours"))
        df_jplus = df_jplus.join(df_iwps, [df_jplus.project_num == df_iwps.projectcode, df_jplus.fiwp == df_iwps.name], "left").select("project_num", "fiwp", ((df_jplus.ActualHours).cast(t.FloatType())+F.coalesce(df_iwps.ActualHours, F.lit(0))).alias("ActualHours"))
        df_jplus = df_jplus.selectExpr("fiwp as `IWP Number`", "ActualHours as `Actual Hours`")

        df_jplus.printSchema()
        df_jplus.show(truncate=False)

        csv_buffer = StringIO()
        df_jplus.toPandas().to_csv(csv_buffer, index=False)

        status, import_id = execute_import_api(url, headers, csv_buffer, ImportType=import_type, UpdateOnly=update_only, file_name="upload.csv")
        import_state = {}

        if status:
            logger.info("Data Import Started with ID: " + str(import_id))
            start_time = time.time()
            while True:
                try:
                    import_state = get_import_status(import_id, import_url, headers)
                    import_status = import_state['Status']
            
                    if import_status in allowed_final_status:
                        logger.info(f"Job finished with status: {import_status}")
                        break
                    else:
                        time.sleep(poll_interval_seconds)
            
                    # Check time limit
                    if time.time() - start_time > max_duration_seconds:
                        logger.info("Time limit exceeded (2 minutes). Exiting loop.")
                        break
                except Exception as e:
                    logger.error(f"Error while checking job status: {e}")
                    time.sleep(poll_interval_seconds)

            logger.info(f"Import Status : {import_state}")
            if import_state['Status'] == 'Success':

                if import_state['WarningCount'] > 0:
                    logger.info("Data Import Succeeded with Warning")
                    subject, email_body = generate_generic_warning_body(current_env, source_system_id, "o3", "JPlus O3 Integration", job_run_id, import_state)
                    send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port)
                else:
                    logger.info("Data Import Succeeded")
                    subject, email_body = generate_generic_success_body(current_env, source_system_id, "o3", "JPlus O3 Integration", job_run_id, import_state)
                    send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port)

                    project_audit_details=f"{str(project_id)},{source_system_id},{function_name},{audit_date}"  
                    logger.info("project_audit_details --> " + project_audit_details)

                    if s3_client.upload_to_s3(project_audit_details,f"{audit_folder_location}/{file_name}",kms_key_id,is_gzip=False):
                        logger.info(f"Uploaded Jplus Audit info for {function_name} to {raw_bucket}/{audit_folder_location}/{file_name}")
                    else:
                        raise Exception("Audit file upload failed")

            else:
                subject, email_body = generate_generic_failure_body(current_env, source_system_id, "o3", "JPlus O3 Integration", job_run_id, import_state)
                raise Exception("Data Import Failed")
        else:
            subject, email_body = generate_generic_failure_body(current_env, source_system_id, "o3", "JPlus O3 Integration", job_run_id, import_state)
            logger.error("Data Import Failed")
            raise Exception("Data Import Failed")
    else:
        logger.info("No data found for jplus, skipping the import")
        subject, email_body = generate_generic_success_body(current_env, source_system_id, "o3", "JPlus O3 Integration", job_run_id, {"message": "No data found for jplus, skipping the import"})
        email_body += "\nNo data found for jplus, skipping the import"
        logger.info("Sending success email")
        send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port)

except Exception as e:
    send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port)
    traceback.format_exc("Error Occured at setting up the job args " + str(e))
    raise e
