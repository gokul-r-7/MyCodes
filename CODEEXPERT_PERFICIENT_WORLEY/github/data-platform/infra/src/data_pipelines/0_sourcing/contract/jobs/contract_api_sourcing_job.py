# import os

# print("success")
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
    sys.argv, ["JOB_NAME", "source_name",
               "table_name", "metadata_table_name"]
)

print(args)

source_name = args.get("source_name")
table_name = args.get("table_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#extract_api"
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

contract_export_config = metadata
logger.info(f" ERM API Export Config :{contract_export_config}")

############## Load ERM Workflow Export API Config ################
bucket_name = contract_export_config['job_parameter']['bucket_name']
raw_data_location = contract_export_config['job_parameter']['input_path']
relationalized_data_path = contract_export_config['job_parameter']['temp_output_path']
relationalized_path = relationalized_data_path + \
    table_name + "/relationalized_data/" + table_name
parquet_data_path = contract_export_config['job_parameter']['output_s3']
region = contract_export_config['aws_region']
endpoint = contract_export_config['api_parameter']['endpoint']
sample_data_path = contract_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(
    contract_export_config['job_parameter']['sampling_fraction'])
sampling_seed = contract_export_config['job_parameter']['sampling_seed']
name = contract_export_config['name']
full_incr = contract_export_config['job_parameter']['full_incremental']
status_timeout = contract_export_config['job_parameter']['status_timeout']
status_check = contract_export_config['job_parameter']['status_check']
auth_type = contract_export_config['api_parameter']['auth_type']

# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(
    contract_export_config['auth_api_parameter']['secret_key'], region))

client_id = secret_param_key.get("client_id")
client_secret = secret_param_key.get("client_secret")

contract_export_config['auth_api_parameter']['auth_body']['client_id'] = client_id
contract_export_config['auth_api_parameter']['auth_body']['client_secret'] = client_secret

resource = contract_export_config['auth_api_parameter']['auth_body']['resource']

print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("contract_export_config -> " + str(contract_export_config))
print("full_incr --> " + full_incr)
print("status_timeout --> " + str(status_timeout))
print("status_check --> " + str(status_check))
print("resource --> " + str(resource))
########################################################


def token_retry_mechanism(bucket_name, http_client):
    response, api_status, status_code = http_client.run()
    if status_code == 401:
        response, api_status, status_code = http_client.run()
        print(response)
        print(api_status)
        print(status_code)

    return response, api_status, status_code

########################################################

# Function to convert schema fields to StringType recursively


def convert_schema_to_string(schema):
    updated_fields = []
    for field in schema.fields:
        if field.name == 'rows' and isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            # Convert all fields inside the 'rows' array to StringType
            updated_element_type = convert_schema_to_string(
                field.dataType.elementType)
            updated_fields.append(StructField(field.name, ArrayType(
                updated_element_type), nullable=field.nullable))
        else:
            # Convert field to StringType
            updated_fields.append(StructField(
                field.name, StringType(), nullable=field.nullable))
    return StructType(updated_fields)

########################################################

success = False
kms_key_id = contract_export_config['job_parameter']['kms_key_id']

while True:
    try:
        contract_export_config['api_parameter']['api_method'] = "get"
        http_client = HTTPClient(contract_export_config)

        logger.info(f"Making request to contract API endpoint: {contract_export_config['api_parameter']['endpoint']}")
    
        # calling the API and if token is expired , it will generate the token again and retry
        contract_data_response, api_status, status_code = token_retry_mechanism(bucket_name, http_client)
        if contract_data_response is None or status_code is None:
          logger.info(f"Unable to fetch data from Contract API.")
          exit(1)
    
        if status_code == 200:
            json_data = json.dumps(contract_data_response)
            
            now = datetime.now()
            formatted_now = now.strftime("%Y-%m-%d-%H-%M-%S")
            
            object_name = f"temp/{source_name}/{table_name}/raw_data/{table_name}_{formatted_now}.json"
            
            if contract_data_response:
                if s3_client.upload_to_s3(json_data, object_name, kms_key_id, is_gzip=False):
                    logger.info(f"Uploaded Contract {table_name} info to {object_name}")
                    success = True
                else:
                    logger.error("Failed to upload extracted json raw file to S3")
                    success = False
            else:
                logger.error("Failed to fetch Contract API payload")
                success = False
        else:
            logger.error(f"Contract API request failed with status code: {status_code}")
            success = False

    except Exception as e:
        print("Error during contract data fetch --> " + str(e))
        sys.exit(1)
    
    try:
        if success:
            json_file_path = f"s3://{bucket_name}/{object_name}"
            logger.info(f"Processing data from: {json_file_path}")
    
            # Convert JSON to a DataFrame
            json_df = spark.read.option("inferSchema", "true").json(json_file_path)
            json_df.printSchema()
            #json_df.show(truncate=False)
    
            # Convert the schema to StringType recursively
            updated_schema = convert_schema_to_string(json_df.schema)
    
            # Print the updated schema to verify
            logger.info(f"Updated Schema: {updated_schema}")
    
            json_df = spark.read.schema(updated_schema).json(json_file_path)
            json_filtered = json_df.select("*")
 
            partition_date = generate_timestamp_string(
                timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
            partition_str = f"{get_partition_str_mi(partition_date)}"
    
            # Path to where you want to store Parquet files
            output_parquet_path = f"s3://{bucket_name}/{raw_data_location}/{table_name}/{partition_str}/"
    
            # Write DataFrame to Parquet files
            json_filtered.write.mode("append").parquet(output_parquet_path)
            logger.info(f"data written to the path {output_parquet_path}")
    
            sample_data = json_filtered.sample(
                # Adjust the fraction as needed
                withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
    
            # Path to where you want to store Parquet files
            sample_parquet_path = f"s3://{bucket_name}/{sample_data_path}/{table_name}/"
            sample_data.write.mode("append").parquet(sample_parquet_path)
            logger.info(f"sample data returned to the path {sample_parquet_path}")

            logger.info(f"Cleaning up temporary data: {json_file_path}")
            s3_client.delete_file_from_s3(object_name)
            
            break

    except Exception as e:
        print("Error --> " + str(e))
        sys.exit(1)