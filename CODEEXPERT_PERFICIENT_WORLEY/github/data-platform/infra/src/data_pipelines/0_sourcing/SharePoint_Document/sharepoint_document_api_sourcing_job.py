import sys
import json
import base64
import boto3
import re
import requests
from io import BytesIO
from pyspark.sql.functions import explode, flatten, col
from datetime import datetime, timezone

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from worley_helper.utils.logger import get_logger
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, TIMESTAMP_FORMAT_WITH_UTC
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.helpers import get_partition_str_mi, standardize_columns, write_glue_df_to_s3_with_specific_file_name





# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.sql('set spark.sql.caseSensitive=true')

# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "metadata_table_name","batch_run_start_time"]
)

# Init the logger
logger = get_logger(__name__)


source_name = args.get("source_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
batch_run_start_time=args["batch_run_start_time"]


# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#extract_api"

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)



export_config = metadata


############## Load SharePointList Export API Config ################
#Job parameters
bucket_name = export_config['job_parameter']['bucket_name']
raw_data_location = export_config['job_parameter']['temp_path']
relationalized_data_path = export_config['job_parameter']['temp_path']
parquet_data_path = export_config['job_parameter']['output_s3']
sample_data_path = export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(
    export_config['job_parameter']['sampling_fraction'])
sampling_seed = export_config['job_parameter']['sampling_seed']
temp_deletion_path = export_config['job_parameter']['temp_path']
region = export_config['aws_region']

#OAuth Token parameters
resource = export_config['auth_api_parameter']['auth_body']['resource']
grant_type = export_config['auth_api_parameter']['auth_body']['grant_type']
scope = export_config['auth_api_parameter']['auth_body']['scope']
TOKEN_URL= export_config['auth_api_parameter']['endpoint']
auth_retry= export_config['auth_api_parameter']['auth_retry']



#Sharepoint API call parameters
endpoint = export_config['api_parameter']['endpoint']
titles = export_config['api_parameter']['titles']
auth_type = export_config['api_parameter']['auth_type']
folder_endpoint=export_config['api_parameter']['folder_endpoint']
File_Filter = export_config['api_parameter']['api_query_params']['filter']
derived_folder_endpoint=f"{folder_endpoint}/{File_Filter}'"


name = export_config['name']


#S3 audit details parameters
job_start_time=export_config['api_parameter']['api_body']['timestamp']
audit_foldername=export_config['api_parameter']['api_body']['auditfoldername']
#incremental_criteria_folder_location=export_config['job_parameter']['incremental_criteria_folder_location']
audit_file_name=f"{source_name}_audit.txt"
default_incremental_date=export_config['job_parameter']['default_incremental_date']
full_incremental=export_config['job_parameter']['full_incremental']


# Init the job start time
job_start_time = generate_timestamp_string(
    ).strftime(TIMESTAMP_FORMAT_WITH_UTC)
logger.info(f"==================job Start time is :{job_start_time} ============")

#Update audit file with current job datetime for next incremental run
sharepoint_audit_column=f"{source_name},{job_start_time}"  
logger.info("sharepoint_audit_data --> " + sharepoint_audit_column)
sharepoint_audit_full_file = f'{audit_foldername}/{audit_file_name}'


#############  Generate Dynamic Config for SharePointList Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(
    export_config['auth_api_parameter']['secret_key'], region))

client_id = secret_param_key.get("client_id")
client_secret = secret_param_key.get("client_secret")

export_config['auth_api_parameter']['auth_body']['client_id'] = client_id
export_config['auth_api_parameter']['auth_body']['client_secret'] = client_secret

#Metadata Parameter check
logger.info("Bucket Name -> " + bucket_name)
logger.info("raw_data_location -> " + raw_data_location)
logger.info("relationalized_data_path -> " + relationalized_data_path)
logger.info("parquet_data_path -> " + parquet_data_path)
logger.info("export_config -> " + str(export_config))



#Oauth parameters check:
logger.info("resource -> " + resource)
logger.info("grant_type -> " + grant_type)
logger.info("scope -> " + scope)
logger.info("TOKEN_URL -> " + TOKEN_URL)
logger.info("endpoint -> " + endpoint)
logger.info("folder_endpoint -> " + folder_endpoint)
logger.info("File_Filter -> " + File_Filter)

#Audit parameters
logger.info("batch_run_start_time" + batch_run_start_time)
logger.info("job_start_time" + job_start_time)
logger.info("audit_foldername" + audit_foldername)
logger.info("sharepoint_audit_column" + sharepoint_audit_column)
logger.info("audit_file_name" + audit_file_name)
logger.info("sharepoint_audit_full_file" + sharepoint_audit_full_file)
logger.info("default_incremental_date" + default_incremental_date)
logger.info("full_incremental" + full_incremental)
logger.info("derived_folder_endpoint -> " + derived_folder_endpoint)


 
kms_key_id = export_config['job_parameter']['kms_key_id']


# Method to get access token
def get_access_token():
    payload = {
        'grant_type': grant_type,
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource,
        'scope': scope  
    }
    
    response = requests.post(TOKEN_URL, data=payload)
        
    if response.status_code == 200:
        token_data = response.json()
        return token_data['access_token']
    else:
        raise Exception(f"Failed to get access token: {response.status_code} - {response.text}")

# Method to download file/ files from SharePoint
def download_file(access_token, file_url):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json;odata=verbose'
    }
        
    for _ in range(auth_retry):  # Retry no.of times based on metadata
        response = requests.get(file_url, headers=headers)
        if response.status_code == 200:
            return BytesIO(response.content)  # Return the file stream
        logger.info(f"Retrying download... Status: {response.status_code}")

    logger.error(f"Failed to download file after retries: {response.status_code} - {response.text}")
    sys.exit(1)
    
def main():
    try:
        access_token = get_access_token()
        s3_client = S3(bucket_name, region)
        
        dt = datetime.strptime(job_start_time, "%Y-%m-%d %H:%M:%S %Z")
        incremental_folder_name = f"{batch_run_start_time}_UTC"
      
        # Read the previous audit file from S3
        audit_file_obj,status=s3_client.read_s3_file(audit_foldername,audit_file_name)
        
             
        if audit_file_obj and full_incremental == 'I':
            logger.info("Audit file is available")
            previous_audit_data = audit_file_obj.split(',')
        
            source_name_audit  = previous_audit_data[0].strip()  # First column is source name in audit file
            date_threshold_str = previous_audit_data[1].strip()  # Second column contains the timestamp
            date_threshold = datetime.strptime(date_threshold_str, TIMESTAMP_FORMAT_WITH_UTC).replace(tzinfo=timezone.utc)
                
            
        elif audit_file_obj and full_incremental != 'I':
            logger.info("Incremental flag is not set to I, hence default incremental date will be picked")
            date_threshold = datetime.strptime(default_incremental_date, TIMESTAMP_FORMAT_WITH_UTC).replace(tzinfo=timezone.utc) 
        
        else:
            logger.error("Initial Load. Incremental Audit file not found")
            date_threshold = datetime.strptime(default_incremental_date, TIMESTAMP_FORMAT_WITH_UTC).replace(tzinfo=timezone.utc) 
            
        derived_folder_endpoint=f"{folder_endpoint}/{File_Filter}'"
        response = requests.get(derived_folder_endpoint, headers={'Authorization': f'Bearer {access_token}','Accept': 'application/json;odata=verbose'})
        
        
        # Check response status
        if response.status_code != 200:
            logger.info("Error:", response.status_code)
            logger.info("Response Text:", response.text)
        
        else:
            # Extract JSON data from the response
            json_string = response.content.decode('utf-8')
            json_data = json.loads(json_string)
            items = json_data.get('d', {}).get('results', [])
            
            
            # Initialize variables to find the max time of last modified file
            last_modified_time = None
        
            updated_files = []
            for item in items:
                
                current_modified_time_str = item.get('TimeLastModified')
                # Parse the string into a datetime object
                dt = datetime.fromisoformat(current_modified_time_str.replace("Z", "+00:00"))
                
                if dt > date_threshold:
                    updated_files.append({
                    'name': item.get('Name'),
                    'modified_time': dt
                    })
                    
            # If no files are updated
            if not updated_files:
                logger.info(f"No files were updated in the SharePoint site after {date_threshold_str}, hence no files were downloaded. Job completed.")
                return  # Exit the function early if there are no updated files
                        
            # Update the audit column with the modified times of fetched files
            if updated_files:
                last_modified_time = max(file['modified_time'] for file in updated_files)
                sharepoint_audit_column = f"{source_name},{last_modified_time.strftime(TIMESTAMP_FORMAT_WITH_UTC)}"
                    
                # Process and upload updated files
                for file_info in updated_files:
                                         
                    file_derived_url = f"{endpoint}/{file_info['name']}')/$value"
                    file_stream = download_file(access_token, file_derived_url)
                    input_key = f"{export_config['job_parameter']['output_s3']}/{incremental_folder_name}/{file_info['name']}"
                    # Upload to S3 after downloading the file
                    #s3_client.upload_fileobj(file_stream, bucket_name, input_key)
                    s3_client.upload_to_s3(file_stream,input_key,kms_key_id,is_gzip=False)
                    logger.info(f"Uploaded file to {bucket_name}/{input_key}")
            
            
        
            #Upload audit file to S3
            if sharepoint_audit_column:
                s3_client.upload_to_s3(sharepoint_audit_column,sharepoint_audit_full_file,kms_key_id,is_gzip=False)
                logger.info(f"Uploaded audit file for SharePoint {source_name} info to {sharepoint_audit_full_file}")
	                    
            else:
                logger.error(f"Uploading audit file to S3 failed: {e}")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
            
    

if __name__ == "__main__":
    main()

  
job.commit()


