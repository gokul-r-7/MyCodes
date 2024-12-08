import sys
import re
import uuid
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
import logging

# Setup CloudWatch logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#Output s3 path
account_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/account_dim_sum/"
profile_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/profile_dim_sum/"
transaction_adobe_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/transaction_adobe_fact/"
transaction_okta_user_agg_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/transaction_okta_user_agg_fact/"
transcation_okta_day_agg_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/transcation_okta_day_agg_fact/"

#PartitionKeys for the target files
account_dim_sum_partitionkeys = ["time_key"]
profile_dim_sum_partitionkeys = ["time_key"]
transaction_adobe_fact_partitionkeys = ["Activity_Name"]
transaction_okta_user_agg_fact_partitionkeys = ["authentication_method"]
transcation_okta_day_agg_partitionkeys = ["Authentication_method"]

glue_client = boto3.client('glue')
s3 = boto3.client('s3')
starttime = datetime.now()
start_time = starttime.strftime("%Y-%m-%d %H:%M:%S")
unique_id = str(uuid.uuid4())
job_name = "CIAM_ETL"
job_log_database_name = "ciam_test3"
job_log_table_name = "job_log_table"
job_log_table_path = "s3://cci-dig-aicoe-data-sb/processed/ciam_data1/job_log_table/"

def job_lob_table_data(job_load_type, endtime, runtimeseconds, account_dim_count, profile_dim_count, adobe_fact_count, user_agg_fact_count, day_agg_count):
    job_log_data = {
        "id" : unique_id,
        "job_name" : job_name,
        "job_load_type" : job_load_type,
        "job_start_time" : start_time,
        "job_end_time" : endtime,
        "job_run_time" : runtimeseconds,
        "account_dim_sum_count" : account_dim_count,
        "profile_dim_sum_count" : profile_dim_count,
        "transaction_adobe_fact_count" : adobe_fact_count,
        "transaction_okta_user_agg_fact_count" : user_agg_fact_count,
        "transcation_okta_day_agg_output" :  day_agg_count,
        "created_date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        df = spark.createDataFrame([job_log_data])
        return df
    except Exception as e:
        logger.error(f"Error creating job log data: {e}")
        raise

def check_table_exists(database_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Table {table_name} does not exist in database {database_name}.")
            return False
        else:
            logger.error(f"Error checking if table exists: {e}")
            raise

def check_load_type(database_name, table_name):
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
        )
        df = dynamic_frame.toDF()
        load_type_values = df.select("job_load_type").distinct().collect()

        for row in load_type_values:
            if row['job_load_type'] == 'Latest 13 Months':
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking load type: {e}")
        raise

def load_data_from_athena(sql_query, load_full_data=True):
    try:
        if load_full_data:
            logger.info(f"Loading full data for query: {sql_query}")
            read_df = spark.sql(sql_query)
        else:
            logger.info(f"Loading latest month data for query: {sql_query}")
            read_df = spark.sql(sql_query)
        return read_df
    except Exception as e:
        logger.error(f"Error loading data from Athena: {e}")
        raise

def write_to_s3(df, output_path, partitionkey):
    try:
        df = df.repartitionByRange(1, partitionkey)
        write_df = df.write \
            .partitionBy(partitionkey) \
            .format("parquet") \
            .option("compression", "gzip") \
            .mode("overwrite") \
            .save(output_path)
        logger.info(f"Data written to S3: {output_path}")
        return write_df
    except Exception as e:
        logger.error(f"Error writing data to S3: {e}")
        raise

# Step 1: Check if the table exists
try:
    if check_table_exists(job_log_database_name, job_log_table_name):
        logger.info(f"Table {job_log_table_name} exists in database {job_log_database_name}.")
    else:
        logger.warning(f"Table {job_log_table_name} does not exist in database {job_log_database_name}.")
except Exception as e:
    logger.error(f"Error checking table existence: {e}")
    raise

# Step 2: Main Logic for Loading Data
try:
    if check_load_type(job_log_database_name, job_log_table_name):
        logger.info("Load type: Latest 13 Months")
        account_dim_sum_df = load_data_from_athena(account_dim_sum_1, load_full_data=False)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_1, load_full_data=False)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact, load_full_data=False)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact, load_full_data=False)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg, load_full_data=False)
    else:
        logger.info("Load type: Latest 13 Months")
        account_dim_sum_df = load_data_from_athena(account_dim_sum_13, load_full_data=True)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_13, load_full_data=True)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact, load_full_data=True)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact, load_full_data=True)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg, load_full_data=True)

    account_dim_sum_df.cache()
    profile_dim_sum_df.cache()
    transaction_adobe_fact_df.cache()
    transaction_okta_user_agg_fact_df.cache()
    transcation_okta_day_agg_df.cache()

    # Processing and writing the data
    account_dim_sum_df = account_dim_sum_df.withColumn("time_key", F.to_date(account_dim_sum_df["time_key"], "yyyy-MM-dd"))
    profile_dim_sum_df = profile_dim_sum_df.withColumn("time_key", F.to_date(profile_dim_sum_df["time_key"], "yyyy-MM-dd"))
    write_to_s3(account_dim_sum_df, account_dim_sum_output, account_dim_sum_partitionkeys)
    write_to_s3(profile_dim_sum_df, profile_dim_sum_output, profile_dim_sum_partitionkeys)
    write_to_s3(transaction_adobe_fact_df, transaction_adobe_fact_output, transaction_adobe_fact_partitionkeys)
    write_to_s3(transaction_okta_user_agg_fact_df, transaction_okta_user_agg_fact_output, transaction_okta_user_agg_fact_partitionkeys)
    write_to_s3(transcation_okta_day_agg_df, transcation_okta_day_agg_output, transcation_okta_day_agg_partitionkeys)
except Exception as e:
    logger.error(f"Error in main ETL process: {e}")
    raise

# Step 3: Log Job Metrics
try:
    endtime = datetime.now()
    end_time = endtime.strftime("%Y-%m-%d %H:%M:%S")
    run_time = endtime - starttime
    hours, remainder = divmod(run_time.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    runtime_formatted = f"{hours:02}:{minutes:02}:{seconds:02}"

    job_log_df = job_lob_table_data("Latest 13 Months", end_time, runtime_formatted, 0, 0, 0, 0, 0)
    job_log_df.show()

except Exception as e:
    logger.error(f"Error logging job metrics: {e}")
    raise
