import sys
import re
import json
import base64
import boto3
import os
import time
import requests
from datetime import datetime
from pyspark.sql.functions import lit
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import col, when
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, struct, array
import requests
import json


# Init the logger
logger = get_logger(__name__)


# Create a GlueContext

spark_conf = SparkSession.builder.appName("GlueJob").getOrCreate()

# sc = SparkContext()
glueContext = GlueContext(spark_conf)
spark = glueContext.spark_session
job = Job(glueContext)


# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["source_name","table_name", "metadata_table_name",]
)

print(args)

source_name = args.get("source_name")
table_name = args.get("table_name")
metadata_table_name = args.get("metadata_table_name")


# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#extract_api"
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

adt_export_config = metadata
logger.info(f" ERM API Export Config :{adt_export_config}")



############## Load ADR Workflow Export API Config ################
region = adt_export_config['aws_region']
api_base_url = adt_export_config['api_parameter']['api_base_url']
api_tablename = adt_export_config['api_parameter']['api_tablename']
sample_data_path = adt_export_config['job_parameter']['schema_output_s3']
bucket_name = adt_export_config['job_parameter']['bucket_name']
raw_data_location = adt_export_config['job_parameter']['input_path']
take = int(adt_export_config['api_parameter']['api_init_take_offset'])
skip = int(adt_export_config['api_parameter']['api_init_skip_offset'])
chunk_index = int(adt_export_config['api_parameter']['api_chunk_index'])
sampling_fraction = float(adt_export_config['job_parameter']['sampling_fraction'])
sampling_seed = int(adt_export_config['job_parameter']['sampling_seed'])


### Print 
print("base_url Name -> " + api_base_url )
print("bucket_name Name -> " + bucket_name )
print("raw_data_location Name -> " + raw_data_location )
print("table_name Name -> " + table_name )
print("api_tablename Name -> " + api_tablename )
print(f"Initial Take value ->  {take}" )
print(f"Initial skip value ->  {skip}" )
print(f"chunk_index Value-> {chunk_index}")
print(f"sampling_fraction Value-> {sampling_fraction}")
print(f"sampling_seed Value-> {sampling_seed}")


# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(
    adt_export_config['api_parameter']['secret_key'], region))

api_key = secret_param_key.get("api_key")



#main code
headers = {
    'x-api-key': api_key
}


while True:
    url = f"{api_base_url}/{api_tablename}?Take={take}&Skip={skip}"
    print(f"Fetching: {url}")

    response = requests.get(url, headers=headers, verify=False)
    print(f"{url} response is : {response}")
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        break

    try:
        json_response = response.json()
        records = json_response.get("data", [])
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        break

    count = len(records)
    print(f"Chunk {chunk_index}: {count} records")

    if not records:
        print("No records found. Exiting.")
        break

    # Convert records to Spark DataFrame
    # rdd = spark.sparkContext.parallelize(records)
    # df = spark.read.json(rdd)
    rdd = spark.sparkContext.parallelize([json.dumps(record) for record in records])
    df = spark.read.json(rdd)

    partition_date = generate_timestamp_string(
                timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
    partition_str = f"{table_name}/{get_partition_str_mi(partition_date)}"
    
            # Path to where you want to store Parquet files
    output_parquet_path = f"s3://{bucket_name}/{raw_data_location}/{partition_str}/"
    print(f"Parquet files will be written to {output_parquet_path} ")
    
            # Write DataFrame to Parquet files
    #df.write.mode("append").parquet(output_parquet_path)
    df.coalesce(1).write.mode("append").parquet(output_parquet_path)
    logger.info(f"data returned to the path {output_parquet_path}")
    print(f"Wrote {count} records to {output_parquet_path}")

    # Sample the DataFrame
    json_sample = df.sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)

    # Path to write the sample data
    sample_parquet_path = f"s3://{bucket_name}/{sample_data_path}/{table_name}/"
    print(f"Sampled Parquet files will be written to {sample_parquet_path}")

    # Write sampled data to S3
    json_sample.write.mode("append").parquet(sample_parquet_path)
    logger.info(f"Sample data returned to the path {sample_parquet_path}")
    if count < take:
        print("Final chunk received. Exiting loop.")
        break

    skip += take
    chunk_index += 1