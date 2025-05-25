import sys
import json
import base64
import boto3
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, save_spark_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN,TIMEZONE_UTC
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import col, when
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import explode
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from worley_helper.utils.constants import (
    TIMEZONE_SYDNEY,
    DATETIME_FORMAT,
    REGION,
    DATE_FORMAT,
    AUDIT_DATE_COLUMN,
    TIMEZONE_UTC,
    TIMESTAMP_FORMAT
    )
from worley_helper.utils.date_utils import (
    generate_timestamp_string,
    generate_today_date_string,
    generate_formatted_snapshot_date_string
    )
import datetime
import pytz

# Run parallel thread
MAX_WORKERS = 100
FOLDER_TIMESTAMP_FORMAT: str = "%Y-%m-%d-%H-%M-%S"
def run_parallel_threads(threads, max_workers=MAX_WORKERS):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_thread = {executor.submit(thread.run): thread for thread in threads}

        for future in as_completed(future_to_thread):
            thread = future_to_thread[future]
            try:
                future.result()  # This will re-raise any exception caught during execution
            except Exception as exc:
                logger.info(f'Error - {thread.task_id} generated an exception: {exc}')
            finally:
                if thread.has_failed():
                    logger.info(f'Error - {thread.task_id} failed with exception: {thread.get_exception()}')

# Thread Handler
class ThreadHandler(threading.Thread):
    def __init__(self, task_id, task_func, *task_args, **task_kwargs):
        super().__init__()
        self.task_id = task_id
        self.task_func = task_func
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        self._status = 'Initialized'
        self._exception = None
        # self.logger = logger

    def run(self):
        try:
            self._set_status('Running')
            logger.info(f"Task {self.task_id} started.")

            # Execute the task function with the provided arguments
            self.task_func(*self.task_args, **self.task_kwargs)

            self._set_status('Completed')
            logger.info(f"Task {self.task_id} completed successfully.")
        except Exception as e:
            self._exception = e
            self._set_status('Failed')
            logger.info(f"Error - Task {self.task_id} encountered an error: {e}")

    def _set_status(self, status):
        self._status = status

    def get_status(self):
        return self._status

    def has_failed(self):
        return self._exception is not None

    def get_exception(self):
        return self._exception

# Extract content from API
def extract_content(response, response_type, status_code):
    SUCCESS = True
    FAILURE = False
    status_code = status_code
    try:
        content_extractors = {
            'json': lambda: response.json(),
            'binary': lambda: response.content,
            'xml': lambda: response.content,
            'text': lambda: response.text,
            'raw': lambda: response
        }
        content_extractor = content_extractors.get(response_type, response.json)
        return content_extractor(), SUCCESS, status_code
    except Exception as e:
        logger.info(f"Error extracting content: {e}")
        return None, FAILURE , status_code


#Get all fields from schema    
def collect_schema_fields(schema, prefix=""):
    fields_info = []
    
    for field in schema.fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        data_type = type(field.dataType).__name__  # Get the class name as a string
        
        # Collect field name and data type
        fields_info.append((field_name, data_type))
        
        # Check and handle nested structs
        if isinstance(field.dataType, StructType):
            fields_info.extend(collect_schema_fields(field.dataType, field_name))
        
        # Check and handle arrays of structs
        elif isinstance(field.dataType, ArrayType):
            element_type = field.dataType.elementType
            element_type_name = type(element_type).__name__
            if isinstance(element_type, StructType):
                fields_info.extend(collect_schema_fields(element_type, f"{field_name}.element"))
            else:
                fields_info.append((f"{field_name}.element", element_type_name))
    
    return fields_info

# Raw file count
def get_raw_downloaded_files(raw_bckt,tmp_files_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(raw_bckt)
    count = 0
    for object in bucket.objects.filter(Prefix=tmp_files_path):
        if object.key.endswith('.json'):
            count += 1
    print(f" {count} total count in {tmp_files_path}")
    return count

# Make API call
def call_api(api_endpoint, username, password, query_params, api_timeout):
    
    # Set headers
    authData = f"{username}:{password}"
    base64AuthData = base64.b64encode(authData.encode()).decode()
    authHeaders = {
        "Authorization": f"Basic {base64AuthData}",
        "Content-Type": "application/json",
    }

    # Log API request
    logger.info(f"API call made to {api_endpoint}")
    retry_attempt = 1
    MAX_RETRIES = 2
    while retry_attempt <= MAX_RETRIES:
        try:
            response = requests.get(
                api_endpoint,
                params=query_params,
                headers=authHeaders,
                timeout=api_timeout,
                verify=False,
            )
            # Extract json content from the response
            logger.info(f"Validate the response and Extract json content and write to file")
            status_code = response.status_code
            if 200 <= status_code < 300:
                logger.info(f"API call made to {api_endpoint} completed, Status code: {response.status_code}")
                response, status, status_code = extract_content(response, 'json', status_code)
                return response, status, status_code
            else:
                logger.error(f"API call to {api_endpoint} failed with status code: {status_code}")
                return None, False, status_code
        except requests.exceptions.Timeout as e:
            # Retry for any other error
            logger.info(f"Error in calling Project list API: {e}")
            if retry_attempt == MAX_RETRIES:
                logger.error(f"Maximum retries of {MAX_RETRIES} done so exiting with timeout error")
                raise  # Indicates other error
            logger.info(f"Request timed out. Retrying...")
            retry_attempt=retry_attempt+1
            api_timeout = api_timeout+60
            
        except requests.exceptions.RequestException as e:
            logger.info(f"Error in calling Export API: {e}")
            if response.status_code in [401, 403]:
                # If authentication failure, refresh token and retry
                logger.info(f"Received {response.status_code} error. Refreshing token and retrying...")
                return None, False, response.status_code  # Indicates authentication failure
            else:
                # Retry for any other error
                logger.info(f"Received {response.status_code} error. Retrying...")
                raise
        except Exception as e:
            # Handle all other exceptions
            logger.info(f"An error occurred: {e}")
            raise  # Indicates other error



# Get Package API data
def get_packages_data(
        api_endpoint,
        username,
        password,
        api_timeout,
        api_tmp_folder_path,
        output_file_name
):
    try:
        package_data, api_status, status_code = call_api(api_endpoint, username, password, query_params={}, api_timeout=api_timeout)


        if api_status and package_data:
            object_name = f"{api_tmp_folder_path}/{output_file_name}"

            if s3_client.upload_to_s3(bytes(json.dumps(package_data).encode('UTF-8')), object_name, kms_key_id,is_gzip=False):
                logger.info(f"Uploaded mail response info to {object_name}")
                return True
            else:
                logger.info(f"Upload failed mail response info to {object_name}")
                return False
        else:
            logger.info(f"API failed mail for endpoint {api_endpoint}")
            return False
    except Exception as e:
        logger.info(f"Error in api responce process - {e}")
        raise


# Init the audit date
audit_date_value = generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name","endpoint_host","ProjectId"]
)

source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
endpoint_host = args["endpoint_host"]
ProjectId = args["ProjectId"]
print("ProjectId --> " + ProjectId)

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)
    
logger.info(f" Metadata Response :{metadata}")

aconex_package_export_config = metadata
logger.info(f" Aconex package Export Config :{aconex_package_export_config}")

#############  Load Aconex package Export API Config ################
bucket_name = aconex_package_export_config['job_parameter']['bucket_name']
raw_data_location = aconex_package_export_config['job_parameter']['input_path']
raw_data_location = f"{raw_data_location}/{ProjectId}/raw_data"
project_id_partition = f"Project={ProjectId}"
relationalized_data_path = aconex_package_export_config['job_parameter']['input_path']
relationalized_data_path = f"{relationalized_data_path}/{ProjectId}/relationalized_data"
parquet_data_path = aconex_package_export_config['job_parameter']['output_s3']
row_tag = aconex_package_export_config['job_parameter']['row_tag']
root_name = aconex_package_export_config['job_parameter']['root_tag']
region = aconex_package_export_config['aws_region']
incremental_default_date = aconex_package_export_config['job_parameter']['incremental_default_date']
page_size=aconex_package_export_config['page_size']
incremental_criteria_folder_location = aconex_package_export_config['job_parameter']['incremental_criteria_folder_location']
endpoint_suffix=aconex_package_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_package_export_config['api_parameter']['endpoint_prefix']
aconex_package_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint_url=aconex_package_export_config['api_parameter']['endpoint'] 
sample_data_path = aconex_package_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_package_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_package_export_config['job_parameter']['sampling_seed']
name = aconex_package_export_config['name']
full_incr = aconex_package_export_config['job_parameter']['full_incr']
temp_deletion_path = aconex_package_export_config['job_parameter']['input_path']
temp_deletion_path = f"{temp_deletion_path}/{ProjectId}"
api_retry_count = aconex_package_export_config['api_parameter']['api_retry']
today = datetime.datetime.now(pytz.UTC)
current_batch_run_time = today.strftime(TIMESTAMP_FORMAT)
print("temp_deletion_path " + temp_deletion_path)


auth_type = aconex_package_export_config['api_parameter']['auth_type']

#############  Generate Dynamic Config for Aconex package Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_package_export_config['api_parameter']['secret_key'], region))

Username = secret_param_key.get("username")
Password = secret_param_key.get("password")

authData = f"{Username}:{Password}"
base64AuthData = base64.b64encode(authData.encode()).decode()

aconex_package_export_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"

print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("row_tag -> " + row_tag)
print("root_name -> " + root_name)
print("aconex_package_export_config -> " + str(aconex_package_export_config))
print("incremental_default_date -> " + str(incremental_default_date))
print("incremental_criteria_folder_location -> " + str(incremental_criteria_folder_location))
print("endpoint_suffix -> " + str(endpoint_suffix))
print("endpoint_prefix -> " + str(endpoint_prefix))
print("full_incr --> " + full_incr)

#######################################################


success=False
kms_key_id = aconex_package_export_config['job_parameter']['kms_key_id']

try:
    if ProjectId == ProjectId:
        counter = 1
        endpoint_project_url = endpoint_url + "/" + ProjectId + "/packages"           
        print("endpoint_project_url --> " + endpoint_project_url)
        aconex_package_export_config['api_parameter']['endpoint']=f"{endpoint_project_url}"
        http_client = HTTPClient(aconex_package_export_config)
        print("Processing the loop for endpoint_project_url --> " + endpoint_project_url)
        package_project_json_response, api_status,status_code = http_client.run()
        api_call_attempt=0

        while (api_call_attempt < api_retry_count):
         
        
         if "403" in str(status_code):
               print("Skipping the loop for endpoint_project_url --> " + endpoint_project_url)
               logger.info("Skipping the loop for endpoint_project_url --> " + endpoint_project_url)
               #continue  # Skip the current iteration and continue with the next one
               job.commit()
               os._exit(0)

         elif status_code == 200:
               break
         else:
               print(f"API Attempt {api_call_attempt} failed. Retrying")
               api_call_attempt=api_call_attempt+1
        current_batch_run_time_str = generate_formatted_snapshot_date_string(
                                     current_batch_run_time,
                                     FOLDER_TIMESTAMP_FORMAT,
                                     TIMESTAMP_FORMAT
                                    )         
        if package_project_json_response:
            logger.info("Package api call successful. Proceeding with next step")
            packageContents=package_project_json_response.get('content')
            packageNumbers = []
            apiThreads = []
            for packageContent in packageContents:
                packageNumber=packageContent.get('packageNumber')
                packageNumbers.append(packageNumber)
                api_endpoint=None
                for link in packageContent.get('links'):
                    if link.get('rel') == 'self':
                        packageDetailsUrl=link.get('href')
                        api_endpoint = "https://" + endpoint_host + packageDetailsUrl
                        tmp_object_name=f"{packageNumber}_{current_batch_run_time}.json"
                        api_timeout=aconex_package_export_config['api_parameter']['api_timeout']
                        if not api_timeout:
                            api_timeout = 60
                        api_task_id = f"{ProjectId}_{packageNumber}_{current_batch_run_time_str}"
                        apiThreads.append(
                                ThreadHandler(
                                task_id=api_task_id,
                                task_func=get_packages_data,
                                api_endpoint=api_endpoint,
                                username=Username,
                                password=Password,
                                api_timeout=api_timeout,
                                api_tmp_folder_path=f"{raw_data_location}",
                                output_file_name=tmp_object_name
                                    )
                                )
            total_packages=len(packageNumbers)
            logger.info(f"Total number of packages available to get package details -> {total_packages}")
            logger.info(f"Total threads to process{len(apiThreads)}")

            if apiThreads and len(apiThreads) > 0:
                run_parallel_threads(apiThreads)
                #Check for the response count against the batch count and proceed data processing.
                raw_files_count = get_raw_downloaded_files(bucket_name,raw_data_location)
                print(f"Raw files generated - {raw_files_count}")
                if raw_files_count == len(packageNumbers):
                    print(f"File count matches with Packages count")

                    success=True
                else:
                    success=False
        else:
            logger.error("Failed to fetch Oracle Aconex package Export API payload")
            success = False         
        
except Exception as e:
       print("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code



try:
    if success:
        
        # Load whole json file into a DynamicFrame
        mapped_df_orig = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    f"s3://{bucket_name}/{raw_data_location}/"
                    ],
                    "recurse": True
            },
            format="json",
            format_options={"rowTag": f"{row_tag}"},
            transformation_ctx=f"{function_name}"
        )
        
        logger.info("Dataframe created")
        # Get the schema from the DynamicFrame and collect the fields with data types
        fields_info = collect_schema_fields(mapped_df_orig.schema())

        #Convert all columns to string data type except struct and array
        for field_name, data_type in fields_info:
            if data_type not in ['StructType', 'ArrayType','NullType']:
                mapped_df_orig = mapped_df_orig.resolveChoice(specs=[(field_name, 'cast:string')])
        logger.info(f"Schema after type casting all data types to string")
        
        spark_df = mapped_df_orig.toDF()
        spark_df.printSchema()
        packages_df=spark_df.select("packageNumber", "documents")
        packages_df.printSchema()
        packages_df=packages_df.withColumn("documents", explode(col("documents"))).select(
        col("packageNumber"),col("documents.documentId").alias("documentId"))
        packages_df=packages_df.withColumn("projectid",lit(ProjectId))
        packages_df.printSchema()
        packages_df.show(truncate=False)
        # Coalesce to 1 or a few partitions (e.g., 1 file or n files)
        df = packages_df.coalesce(2)
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
        DATETIME_FORMAT)

        partition_str = f"{project_id_partition}/{get_partition_str_mi(partition_date)}"
        partitioned_s3_key = (
            parquet_data_path
            + "/"
            + name
            + "/"
            + partition_str
            )
        # Write partitioned data to S3
        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            #root_df_rf.select(f"{root_name}"),
            dynamic_frame,
            bucket_name,
            partitioned_s3_key,
            function_name,
            typecast_cols_to_string = True
            )
        
        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {row_tag}")

                #sample_s3_key = sample_data_path + "/" + name + "/"
        sample_s3_key = "s3://" + bucket_name + "/" + sample_data_path + "/" + name

        # Write sample data to S3 without partitioning
    
        sample_data = (
            #root_df_rf.select(f"{root_name}")
            dynamic_frame
            .toDF()
            .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
        )  # Adjust the fraction as needed
    
        logger.info(f"Selected sample data {source_name}")

        save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)
    
        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {temp_deletion_path}")
    
    logger.info(f"Deleting folder path {temp_deletion_path}")
    s3_client.delete_folder_from_s3(temp_deletion_path)  
   # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {temp_deletion_path}")
    logger.info(f"Deleting folder path {temp_deletion_path}")
    s3_client.delete_folder_from_s3(temp_deletion_path)  
    job.commit()        

except Exception as e:

    print("Error --> " + str(e))
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {temp_deletion_path}")
    logger.info(f"Deleting folder path {temp_deletion_path}")
    s3_client.delete_folder_from_s3(temp_deletion_path)  
    sys.exit(1)  # Exit with failure code            


