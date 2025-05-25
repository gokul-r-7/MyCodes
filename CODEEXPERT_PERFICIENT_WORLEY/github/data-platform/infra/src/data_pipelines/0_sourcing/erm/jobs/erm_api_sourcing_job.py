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
               "table_name", "metadata_table_name", "queryID"]
)

print(args)

source_name = args.get("source_name")
table_name = args.get("table_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
queryID = args["queryID"]
print("queryID --> " + queryID)

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#extract_api"
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

erm_export_config = metadata
logger.info(f" ERM API Export Config :{erm_export_config}")

############## Load ERM Workflow Export API Config ################
bucket_name = erm_export_config['job_parameter']['bucket_name']
raw_data_location = erm_export_config['job_parameter']['input_path']
relationalized_data_path = erm_export_config['job_parameter']['temp_output_path']
relationalized_path = relationalized_data_path + \
    table_name + "/relationalized_data/" + table_name
parquet_data_path = erm_export_config['job_parameter']['output_s3']
region = erm_export_config['aws_region']
endpoint = erm_export_config['api_parameter']['endpoint']
sample_data_path = erm_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(
    erm_export_config['job_parameter']['sampling_fraction'])
sampling_seed = erm_export_config['job_parameter']['sampling_seed']
name = erm_export_config['name']
full_incr = erm_export_config['job_parameter']['full_incremental']
status_timeout = erm_export_config['job_parameter']['status_timeout']
status_check = erm_export_config['job_parameter']['status_check']

queryID_partition = f"queryID={queryID}"

auth_type = erm_export_config['api_parameter']['auth_type']

# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(
    erm_export_config['auth_api_parameter']['secret_key'], region))

client_id = secret_param_key.get("client_id")
client_secret = secret_param_key.get("client_secret")

erm_export_config['auth_api_parameter']['auth_body']['client_id'] = client_id
erm_export_config['auth_api_parameter']['auth_body']['client_secret'] = client_secret

print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("erm_export_config -> " + str(erm_export_config))
print("full_incr --> " + full_incr)
print("status_timeout --> " + str(status_timeout))
print("status_check --> " + str(status_check))

########################################################


def token_retry_mechanism(bucket_name, http_client):
    response, api_status, status_code = http_client.run()
    if status_code == 401:
        temp_path_deletion = "supply_chain/erm/erm_token/erm_token.txt"
        logger.info(f"Deleting folder path {temp_path_deletion}")
        s3_client.delete_file_from_s3(temp_path_deletion)
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
kms_key_id = erm_export_config['job_parameter']['kms_key_id']

while True:
    try:
        # Query to get the publish ID
        erm_export_config['api_parameter']['api_method'] = "get"
        endpoint_get_publish_id = f"{endpoint}Publish?id={table_name}&query={queryID}&setPublishStatus=true"
        print("endpoint_get_publish_id --> " + endpoint_get_publish_id)
        erm_export_config['api_parameter']['endpoint'] = f"{endpoint_get_publish_id}"
        http_client = HTTPClient(erm_export_config)
        logger.info(f" {endpoint_get_publish_id}")
        logger.info(f" {erm_export_config}")
    
        # calling the API and if token is expired , it will generate the token again and retry
        workflow_publish_id_response, api_status, status_code = token_retry_mechanism(bucket_name, http_client)
        #added to handle api error
        print(workflow_publish_id_response)
        print(api_status)
        print(status_code)
        #added as part of call back erm status
        logger.info(f" Validating PublishId Request to ERM using Response: {workflow_publish_id_response} , Status : {api_status}, Status Code : {status_code}")            
        if workflow_publish_id_response is None or status_code is None:
          logger.info(f"Unable to generate PublishId from ERM. Check if TableName and QueryId paramaters are correct")
          exit(1)
    
        # calling the API to check the status of API , either it can be Processing, Completed or CompletedwithErrors
        logger.info(f"Proceeding to check job status for PublishId {workflow_publish_id_response}")
        endpoint_get_publish_status = f"{endpoint}PublishStatus?id={workflow_publish_id_response}"
        erm_export_config['api_parameter']['endpoint'] = f"{endpoint_get_publish_status}"
        http_client = HTTPClient(erm_export_config)
    
        # calling the API and if token is expired , it will generate the token again and retry
        workflow_publish_status_resp, api_status, status_code = token_retry_mechanism(
            bucket_name, http_client)
        print(workflow_publish_status_resp)
        print(api_status)
        print(status_code)
    
        # Checking the status, if it's processing it will wait and retry until status_timeout parameter
        if workflow_publish_status_resp.upper() == "PROCESSING" or workflow_publish_status_resp.upper() == "READY":
            timeout = status_timeout
            interval = status_check  # Time to wait between checks in seconds
            start_time = time.time()
            time.sleep(30)
            while True:
                workflow_publish_status_resp, api_status, status_code = token_retry_mechanism(
                    bucket_name, http_client)
                if workflow_publish_status_resp.upper() != "PROCESSING" and workflow_publish_status_resp.upper() != "READY":
                    print(f"Status is {workflow_publish_status_resp}. Proceeding with the next step.")
                    break
    
                if time.time() - start_time >= timeout:
                    print(f"Timeout {status_timeout} reached. Exiting the loop.")
                    break
    
                print(f"Status is 'Processing'. Trying again after {interval} seconds.")
                time.sleep(interval)
    
        workflow_publish_status_response = workflow_publish_status_resp.replace('"', "")
    
        # if API response is CompletedwithErrors then breaking the code and coming out of it.
        if workflow_publish_status_response.upper() == "COMPLETEDWITHERRORS" or workflow_publish_status_response.upper() == "FAILED":
            sys.exit(1)
    
        # if API response is Completed then moving ahead with next steps
        if workflow_publish_status_response.upper() == "COMPLETED":
    
            endpoint_get_signed_url_id = f"{endpoint}PublishFileSignedUrl?id={workflow_publish_id_response}"
            print("endpoint_get_signed_url_id --> " + endpoint_get_signed_url_id)
            erm_export_config['api_parameter']['endpoint'] = f"{endpoint_get_signed_url_id}"
            http_client = HTTPClient(erm_export_config)
    
            # calling the API to get the signed URL to download data from it.
            workflow_publish_signed_url_resp, api_status, status_code = token_retry_mechanism(bucket_name, http_client)
    
            # removing starting and ending " from Signed URL
            workflow_publish_signed_url_response = workflow_publish_signed_url_resp.replace('"', "")
            print(workflow_publish_signed_url_response)
    
            if "PUBLISH TRANSACTION ID" in workflow_publish_signed_url_response.upper():
                os._exit(0)
    
            # getting the file downloaded as it's not needed any authentication
            response_signed_url = requests.get(workflow_publish_signed_url_response, headers=None, timeout=None, verify=False)
    
            #added as part of 0KB file check
            if response_signed_url.status_code == 200:
    
                # Check the size of the JSON response
                response_size = len(response_signed_url.content)  # Size in bytes
    
                if response_size == 0:
                    logger.info(f"The SingnedURL JSON file response is empty, {response_size} bytes.")
                    logger.info("Exiting the program Successfully")
                    os._exit(0)
                else:
                    logger.info(f"SingnedURL JSON response file size is: {response_size} bytes.")
            else:
                logger.error(f"Error: Unable to download SignedURL, Received status code {response_signed_url.status_code}")
                sys.exit(1)
                
            response_signed_url_json = response_signed_url.json()
            json_data = json.dumps(response_signed_url_json)
    
            # Current date and time
            now = datetime.now()
            # Formatting date and time
            formatted_now = now.strftime("%Y-%m-%d-%H-%M-%S")
            print("formatted_now --> " + str(formatted_now))
            
            object_name = f"temp/{source_name}/{table_name}/raw_data/{table_name}_{queryID}_{formatted_now}.json"
            
            #upload json file to temp folder in raw bucket
            if response_signed_url:
    
                if s3_client.upload_to_s3(json_data, object_name, kms_key_id, is_gzip=False):
                    logger.info(f"Uploaded ERM {table_name} info to {object_name}")
                    success = True
                else:
                    logger.error("Failed to upload extrated json raw file to S3")
                    success = False
            else:
                logger.error("Failed to fetch ERM Export API payload")
                success = False
    
    except Exception as e:
        print("Error --> " + str(e))
        sys.exit(1)
    
    try:
        if success:
            json_file_path = f"s3://{bucket_name}/{object_name}"
            print(json_file_path)
    
            # Convert JSON to a DataFrame
            json_df = spark.read.option("inferSchema", "true").json(json_file_path)
            json_df.printSchema()
            #json_df.show(truncate=False)
    
            # Convert the schema to StringType recursively
            updated_schema = convert_schema_to_string(json_df.schema)
    
            # Print the updated schema to verify
            print("Updated Schema:")
            print(updated_schema)
    
            json_df = spark.read.schema(updated_schema).json(json_file_path)
            json_df_flattened = json_df.selectExpr("tableName", "inline(rows)")
            #json_df_flattened.show(truncate=False)
    
            json_dropped = json_df_flattened.drop("TrxNo").drop("DateTime")
    
            json_filtered = json_dropped.filter(
                json_dropped.tableName != "Metadata")
    
            partition_date = generate_timestamp_string(
                timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
            partition_str = f"{queryID_partition}/{get_partition_str_mi(partition_date)}"
    
            # Path to where you want to store Parquet files
            output_parquet_path = f"s3://{bucket_name}/{raw_data_location}/{table_name}/{partition_str}/"
    
            # Write DataFrame to Parquet files
            json_filtered.write.mode("append").parquet(output_parquet_path)
            logger.info(f"data returned to the path {output_parquet_path}")
    
            sample_data = json_filtered.sample(
                # Adjust the fraction as needed
                withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
    
            # Path to where you want to store Parquet files
            sample_parquet_path = f"s3://{bucket_name}/{sample_data_path}/{table_name}/"
            sample_data.write.mode("append").parquet(sample_parquet_path)
            logger.info(f"sample data returned to the path {sample_parquet_path}")
            
            #commenting async call to ERM 
            #if full_incr.upper() == "F":            
            #   endpoint_callback_erm = f"{endpoint}PublishExternalStatus?id={workflow_publish_id_response}&status=2&async=true"
            # else:
            #    endpoint_callback_erm = f"{endpoint}PublishExternalStatus?id={workflow_publish_id_response}&status=2"            

            # Callback ERM
            erm_export_config['api_parameter']['api_method'] = "put"
            endpoint_callback_erm = f"{endpoint}PublishExternalStatus?id={workflow_publish_id_response}&status=2"
            erm_export_config['api_parameter']['endpoint'] = f"{endpoint_callback_erm}"
            http_client = HTTPClient(erm_export_config)
            logger.info(f" {endpoint_callback_erm}")
            logger.info(f" {erm_export_config}")
            
            # calling the call back erm API and if token is expired , it will generate the token again and retry
            logger.info(f" Making Call Back Request to ERM : {endpoint_callback_erm}")
            erm_callback_response, api_status, status_code = token_retry_mechanism(
            bucket_name, http_client)
            print(erm_callback_response)
            print(api_status)
            print(status_code)
            
            #added as part of call back erm status
            logger.info(f" Validating Call Back Request to ERM using Response: {erm_callback_response} , Status : {api_status}, Status Code : {status_code}")            
            if erm_callback_response is not None and erm_callback_response > 0 and status_code == 200:
                logger.info(f"CallBack to ERM was successfull. Updated rows at ERM : {erm_callback_response} ")

                # Delete any existing staaging data for this job
                logger.info(f"Delete staging data after job is finished - {json_file_path}")
                s3_client.delete_file_from_s3(object_name)

            if erm_callback_response is not None and erm_callback_response == 0 and status_code == 200:
                logger.info(f"CallBack to ERM Failed. Updated rows : {erm_callback_response} ")

                # Delete any existing staaging data for this job
                logger.info(f"Delete staging data after job is finished - {json_file_path}")
                s3_client.delete_file_from_s3(object_name)
                sys.exit(1)

            if erm_callback_response is  None or status_code is  None:
                logger.info(f"CallBack to ERM failed. API Status Code : {status_code} ")
                logger.info(f"CallBack to ERM failed. Track with PublishID : {workflow_publish_id_response} ")

                # Delete any existing staaging data for this job
                logger.info(f"Delete staging data after job is finished - {json_file_path}")
                s3_client.delete_file_from_s3(object_name)                
                sys.exit(1)
    
    except Exception as e:
        print("Error --> " + str(e))
        sys.exit(1)