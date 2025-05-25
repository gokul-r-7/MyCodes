import sys
import json
import base64
import boto3
import os
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.context import SparkContext
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
from pyspark.sql.functions import col, when
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, TimestampType
from awsglue.gluetypes import *
from pyspark.sql.functions import explode
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import to_timestamp,current_timestamp, from_utc_timestamp
from datetime import datetime
# custom Utilities   #
from worley_helper.utils.constants import (
    REGION,
    TIMESTAMP_FORMAT_WITH_UTC
)

# Generate access token for sharepoint api's
def generate_access_token():
    SUCCESS = True
    FAILURE = False
    token_data = {
        'client_id': client_id,
        'scope': oauth_scope,
        'client_secret': client_secret,
        'grant_type': oauth_grant_type
    }
    response = requests.post(oauth_url, data=token_data)
    response_json = response.json()
    global access_token
    access_token = response_json.get("access_token")
    if not access_token:
        logger.error("Failed to obtain access token.")
        return FAILURE
    logger.info(f"Access token generated successfully: {access_token[:10]}...")  # Log the start of the token for verification
    return SUCCESS
# Call sharepoint api's to get details
def get_sharepoint_data(url,contenttype = None, query_params = None):
    response = None
    api_status = None
    api_resp_code = None
    sharepoint_config['api_parameter']['endpoint'] = url
    sharepoint_config['api_parameter']['api_headers']['Authorization'] = f'Bearer {access_token}'
    if contenttype is not None:
      sharepoint_config['api_parameter']['api_headers']['api_response_type'] = contenttype  
    if query_params:
      sharepoint_config['api_parameter']['api_query_params']={}
      sharepoint_config['api_parameter']['api_query_params']['select'] = query_params
    else:
        sharepoint_config['api_parameter']['api_query_params'] = None
    http_client = HTTPClient(sharepoint_config)
    retry_count=0
    while(retry_count < 2):
        response, api_status, api_resp_code = http_client.run()
        if api_resp_code in (401,403,200):
            break
        else:
            retry_count = retry_count + 1
            print("API call failed. Retry initiated")
    if api_resp_code in (401,403):
        auth_status = generate_access_token()
        if auth_status:
            sharepoint_config['api_parameter']['api_headers']['Authorization'] = f'Bearer {access_token}'
            http_client = HTTPClient(sharepoint_config)
            response, api_status, api_resp_code = http_client.run()        
    return response, api_status, api_resp_code

# Get site id for a sharepoint location
def get_sharepoint_site_id(sharepointHostName, sharepointSiteName):
    # Define the SharePoint REST API URL
    url = f"https://{sharepointHostName}/_api/web/sites?$filter=Title eq '{sharepointSiteName}'"
    
    # Send request to the SharePoint REST API
    response, api_status, api_resp_code = get_sharepoint_data(url)
    
    # If the response is successful, extract the site ID
    if api_resp_code == 200:
        # Assuming the response contains a list of sites, find the relevant site
        if 'value' in response and len(response['value']) > 0:
            sharepoint_site_id = response['value'][0]['Id']
            return sharepoint_site_id, api_status, api_resp_code
        else:
            # If no site found, return None
            return None, api_status, api_resp_code
    else:
        # If API call failed, return None
        return None, api_status, api_resp_code

# Get document library id for a sharepoint location
def get_sharepoint_document_library_id(sharepoint_site_id, sharepointDocumentLibrary=None):
    # Define the SharePoint REST API URL to get document libraries (lists)
    url = f"https://{sharepoint_site_id}/_api/web/lists?$filter=BaseTemplate eq 101"
    logger.info(f'DocumentLib - {sharepointDocumentLibrary}')
    
    # Send request to the SharePoint REST API
    response, api_status, api_resp_code = get_sharepoint_data(url)
    
    if api_resp_code == 200:
        # Check if a specific document library is provided
        if sharepointDocumentLibrary is not None:
            logger.info('Check for sharepointDocumentLibrary')
            # Iterate through the lists to find the matching document library
            for docLibrary in response.get('value', []):
                if sharepointDocumentLibrary == docLibrary.get('Title'):
                    logger.info(f'Found Document Library: {sharepointDocumentLibrary}')
                    return docLibrary.get('Id'), api_status, api_resp_code
        else:
            # If no specific document library is provided, return the first document library found
            if response.get('value'):
                return response['value'][0].get('Id'), api_status, api_resp_code
    else:
        # If API call failed, return None
        return None, api_status, api_resp_code
    
    # Return None if no matching document library is found
    return None, api_status, api_resp_code

# Get folder and files list from Sharepoint Document Library
def get_sharepoint_item_list(sharepoint_site_url, sharepointDocumentLibrary, lastprocesseddate=None):
    logger.info(f"Get data for {sharepointDocumentLibrary}")
    sharepoint_item_list = []
    
    # Construct the URL for the SharePoint REST API
    if lastprocesseddate is not None:
        url = f"{sharepoint_site_url}/_api/web/lists/getByTitle('{sharepointDocumentLibrary}')/items?" \
              f"$expand=Fields&$select=Id,FileRef,FileLeafRef,ContentType,DocIcon,Modified,Created,LastModifiedDateTime&" \
              f"$filter=ContentType eq 'Document' and Modified gt '{lastprocesseddate}'"
    else:
        url = f"{sharepoint_site_url}/_api/web/lists/getByTitle('{sharepointDocumentLibrary}')/items?" \
              f"$expand=Fields&$select=Id,FileRef,FileLeafRef,ContentType,DocIcon,Modified,Created,LastModifiedDateTime&" \
              f"$filter=ContentType eq 'Document'"

    page_number = 1
    print(page_number)
    
    # Send the request to the SharePoint REST API
    response, api_status, api_resp_code = get_sharepoint_data(url)
    
    if api_resp_code == 200:
        # Extract the list of files and folders
        sharepoint_item_list = extract_files_folders_list(sharepoint_item_list, response)
        
        # Check if there's a next page of data
        next_page = response.get('d', {}).get('NextPageUrl')
        while next_page:
            url = next_page
            page_number += 1
            print(f'Page Number -> {page_number}')
            
            # Send the request for the next page of items
            response, api_status, api_resp_code = get_sharepoint_data(url)
            if api_resp_code == 200:
                # Extract files and folders from the next page
                sharepoint_item_list = extract_files_folders_list(sharepoint_item_list, response)
                next_page = response.get('d', {}).get('NextPageUrl')
    else:
        logger.error(f"API call failed. Exiting")
        sys.exit(1)
    
    return sharepoint_item_list

# Get list of files and folders from Sharepoint response
def extract_files_folders_list(sharepointData_list, jsondata):
    sharepoint_data = jsondata
    # weburl_replace_string = api_sharepoint_library_prefix
    weburl_replace_string = api_sharepoint_library_prefix
    for item in sharepoint_data.get('value'):
        file_fields = item.get("fields")
        folder_path = item.get("webUrl")
        # Decode the URL
        import urllib.parse
        folder_path = urllib.parse.unquote_plus(folder_path)
        folder_path = folder_path.replace(weburl_replace_string, "")
        if(file_fields.get("ContentType") != 'Folder'):
            file_name = file_fields.get("LinkFilename")
            folder_path = folder_path.replace(file_name, "")
            
            # Standard metadata
            sharepoint_item = {
                'id': f'{item.get("id")}',
                'webUrl': f'{item.get("webUrl")}',
                'name': f'{file_fields.get("LinkFilename")}',
                'folder_path': f'{folder_path}',
                'createdDateTime': f'{item.get("createdDateTime")}',
                'lastModifiedDateTime': f'{item.get("lastModifiedDateTime")}',
                'ContentType': f'{file_fields.get("ContentType")}',
                'DocIcon': f'{file_fields.get("DocIcon")}',
                'file_size': f'{item.get("size") if item.get("size") else ""}',
                'file_status': 'new'  # Default status for new files
            }
            
            # Extract all custom metadata fields from SharePoint V1 API
            custom_metadata = {}
            # Iterate through all field properties to capture custom metadata
            if file_fields:
                for field_key, field_value in file_fields.items():
                    # Skip standard fields already captured
                    if field_key not in ['ContentType', 'LinkFilename', 'DocIcon', 'Modified']:
                        # Add all other fields as custom metadata
                        custom_metadata[field_key] = field_value
            
            # Add custom metadata to the sharepoint_item
            sharepoint_item['custom_metadata'] = custom_metadata
            
            # For identifying if file is new or updated
            # Will be set properly in the database check step
            sharepoint_item['is_updated'] = False
            
            sharepointData_list.append(sharepoint_item)
    return sharepointData_list


def identify_new_vs_updated_deleted_files(file_list):
    """
    Check if files exist in the database and mark them as new, updated, or deleted.
    
    Args:
        file_list: List of files with metadata from SharePoint
    
    Returns:
        Tuple: (processed_files_list, current_file_ids)
    """
    # Get list of existing files from PostgreSQL
    pg_jdbc_url = f"jdbc:postgresql://{rds_cluster}"
    pg_username = secret_rds_param_key['username']
    pg_password = secret_rds_param_key['password']
    dbtable_metadata = f'"{rds_schema}".{rds_sharepoint_files_metadata}'
    
    # Create a temporary view of existing files
    existing_files_df = spark.read \
        .format("jdbc") \
        .option("url", pg_jdbc_url) \
        .option("dbtable", dbtable_metadata) \
        .option("user", pg_username) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Convert to dictionary for easier lookup
    existing_files = {}
    current_file_ids = []
    
    if existing_files_df.count() > 0:
        existing_files_rows = existing_files_df.select("sharepoint_fileid", "sharepoint_weburl", "status").collect()
        for row in existing_files_rows:
            existing_files[row["sharepoint_fileid"]] = {"url": row["sharepoint_weburl"], "status": row.get("status")}
    
    # Process each file in the current SharePoint response
    for file in file_list:
        file_id = file["id"]
        current_file_ids.append(file_id)
        
        if file_id in existing_files and existing_files[file_id].get("status") != "deleted":
            # File exists in database and wasn't previously deleted
            file["file_status"] = "updated"
            file["is_updated"] = True
        elif file_id in existing_files and existing_files[file_id].get("status") == "deleted":
            # File exists in database but was previously marked as deleted (now restored)
            file["file_status"] = "restored"
            file["is_updated"] = True
        else:
            # File doesn't exist in database - it's new
            file["file_status"] = "new"
            file["is_updated"] = False
    
    # Now identify deleted files (in database but not in current response)
    deleted_files_list = []
    
    for file_id, file_info in existing_files.items():
        if file_id not in current_file_ids and file_info.get("status") != "deleted":
            # This is a deleted file
            deleted_file = {
                "id": file_id,
                "webUrl": file_info["url"],
                "file_status": "deleted",
                "is_updated": False
            }
            deleted_files_list.append(deleted_file)
    
    # Combine current files with deleted files
    processed_files = file_list + deleted_files_list
    
    return processed_files, current_file_ids

# need to make sure PostgreSQL table has the appropriate columns to store this new metadata:
# -- Example schema addition for the PostgreSQL table (execute separately)
# ALTER TABLE "schema".rds_sharepoint_files_metadata 
# ADD COLUMN IF NOT EXISTS metadata_json TEXT,
# ADD COLUMN IF NOT EXISTS status VARCHAR(10),
# ADD COLUMN IF NOT EXISTS is_updated BOOLEAN;

# Function to filter by files based on filters
def filter_files(filelist, file_filters,folder_filters):
    logger.info(f"Total number of files available before applying filter ---> {len(filelist)}")
    temp_files = filelist
    filelist = []
    filtered_files = []
    incremental_datetimestamp=file_filters.get('incremental_datetimestamp') if file_filters.get('incremental_datetimestamp') is not None else file_filters.get('default_filter_by_modified_date')
    file_extenstion_filter=file_filters.get('file_extension_filters') if file_filters.get('file_extension_filters') is not None else None
    filter_by_file_extension_flag=file_filters.get('filter_by_file_extension_flag')
    file_by_modified_date_flag=file_filters.get('file_by_modified_date_flag')
    incremental_datetimestamp = datetime.strptime(incremental_datetimestamp, "%Y-%m-%d %H:%M:%S %Z")
    
    #Filter files by Path
    if folder_filters is not None:
        temp_filtered_files = []
        for file in temp_files:
            folder_path=file.get('folder_path')
            path_parts = folder_path.strip('/').split('/')
            if match_folder_filter(folder_filters, path_parts):
                temp_filtered_files.append(file)
        logger.info(f'Total number of files filtered by folder path ---> {len(temp_filtered_files)}')
        print(f'Total number of files filtered by folder path ---> {len(temp_filtered_files)}')
        #Update filelist with filtered files by extension
        filtered_files = temp_filtered_files
        temp_files = temp_filtered_files
    else:
      filtered_files = temp_files
    #Filter files by extension
    if filter_by_file_extension_flag == 'y':
      temp_filtered_files = []
      for file in temp_files:
        file_extension = get_file_extension(file["name"])
        if file_extension in file_extenstion_filter:
            temp_filtered_files.append(file)
      logger.info(f'Total number of files filtered by extensions ---> {len(temp_filtered_files)}')
      print(f'Total number of files filtered by extensions ---> {len(temp_filtered_files)}')

      #Update filelist with filtered files by extension
      filtered_files = temp_filtered_files
      temp_files = temp_filtered_files
    else:
      filtered_files = temp_files

    if file_by_modified_date_flag == 'y': 
      temp_filtered_files = []
      logger.info('Filtering files for files with modified date')
      for file in temp_files:
        lastModifiedDateTime = datetime.strptime(file["lastModifiedDateTime"], "%Y-%m-%dT%H:%M:%SZ")
        if lastModifiedDateTime >= incremental_datetimestamp:
            temp_filtered_files.append(file)
      #Update filelist with filtered files by extension
      filtered_files=temp_filtered_files
      temp_files = temp_filtered_files
      logger.info(f'Total number of files available after incremental date filter---> {len(temp_filtered_files)}')
      print(f'Total number of files available after incremental date filter---> {len(temp_filtered_files)}')
    else:
      filtered_files = temp_files
    return filtered_files

# Function to filter by folder name
def match_folder_filter(folder_filters, path_parts):
    if not folder_filters:
        return True  # No more filters to apply, so it's a match

    if not path_parts:
        return False  # Path is shorter than the filters, no match

    for folder_filter in folder_filters:
        filter_by_foldername = folder_filter.get("filter_by_foldername")
        filter_name = folder_filter.get("name")
        if filter_by_foldername == "n":
            # "n" means to consider all files at this level
            if match_folder_filter(folder_filter.get("folder_filters", None), path_parts[1:]):
                return True

        elif filter_name == path_parts[0]:
            # Recurse into nested filters with the next part of the path
            if match_folder_filter(folder_filter.get("folder_filters", None), path_parts[1:]):
                return True
    return False  # No match found

# Function to get file extension
def get_file_extension(filename):
    return filename[filename.rfind('.'):]



# Function download files from Sharepoint
def download_sharepoint_files(filelist, sharepoint_site_url, access_token, s3_client, s3_root_folder, kms_key_id, job_start_time):
    # Convert job start time to "%Y%m%d_%H%M%S"
    dt = datetime.strptime(job_start_time, "%Y-%m-%d %H:%M:%S %Z")
    incremental_folder_name = dt.strftime("%Y%m%d_%H%M%S") + "_UTC"
    
    for file_info in filelist:
        fileName = file_info['name']
        fileId = file_info['id']
        folderPath = file_info['folder_path']
        
        # Construct the SharePoint REST API URL for downloading the file content
        url = f"{sharepoint_site_url}/_api/web/lists/getByTitle('{sharepointDocumentLibrary}')/items({fileId})/File/$value"
        
        # Define the S3 file path
        s3_file_name = f'{s3_root_folder}/{incremental_folder_name}/{folderPath}{fileName}'
        
        # Define the headers with the Bearer token for authentication
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"  # Adding Content-Type header
        }
        
        retry_count = 0
        while retry_count < 2:
            try:
                # Make the GET request to download the file content
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    break
                elif response.status_code in (401, 403):
                    # If unauthorized, refresh the access token
                    auth_status = generate_access_token()
                    headers["Authorization"] = f"Bearer {auth_status}"  # Update the token in the header
            except requests.exceptions.RequestException as e:
                print(f"Download of file {fileName} failed. Retrying...")
            
            retry_count += 1
        
        if response.status_code == 200:
            # File downloaded successfully
            file_info['s3_FileName'] = s3_file_name
            
            # Upload to S3
            if s3_client.upload_to_s3(response.content, s3_file_name, kms_key_id, is_gzip=False):
                logger.info(f"Uploaded file {s3_file_name}")
                success = True
            else:
                logger.error("Failed to upload file to S3")
                success = False
                logger.error(f"Error downloading file {fileName} in {folderPath}")
                sys.exit(1)  # Exit with failure code
        else:
            logger.error(f"Failed to download file {fileName} in {folderPath}")
        
        logger.info(f'File download completed for {s3_file_name}')
    
    return filelist


# GenAI Notification Webhook API start here ---

def get_curent_job_folder_uri():
    """Generate the source folder with current batch
       folder name.
       curent batch folder name - UTC formated job start time
       source Folder - handled through metadata

    Returns:
        str: complete current batch run folder uri
        str: current batch run folder name
    """
    dt = datetime.strptime(job_start_time, "%Y-%m-%d %H:%M:%S %Z")
    incremental_folder_name = dt.strftime("%Y%m%d_%H%M%S") + "_UTC"
    current_s3_folder_uri = f"{s3_root_folder}/{incremental_folder_name}"
    return current_s3_folder_uri, incremental_folder_name

# Gen AI webhook API processor
def invoke_webhook_notification_process(
        job_run_id,
        metadata_table_name,
        source_name,
        ddb_notfication_system_id,
        notf_ddb_metatype,
        s3_output_folder_uri,
        s3_formated_folder_name,
        job_start_time,
        genai_project,
        no_of_files=1
        ):
    
    SUCCESS = True
    FAILURE = False
    try:
        ddb = DynamoDB(metadata_table_name=metadata_table_name,
                       default_region=REGION)
        webhook_config = ddb.get_metadata_from_ddb(
            source_system_id=ddb_notfication_system_id,
            metadata_type=notf_ddb_metatype)
        secret_params = json.loads(get_secret(
            webhook_config['api_parameter']['secret_key'], REGION))
        ssl_verify=webhook_config['api_parameter']['api_ssl_verify']
        if ssl_verify == False:
            logger.info(f'SSL verify flag is set to false.')
        else:
            logger.info(f'SSL verify is true. Download ssl pem certificate from secret manager {ssl_verify}.')
            ssl_cert=get_secret(ssl_verify, REGION)
            ca_bundle_path = '/tmp/custom-ca-bundle.pem'
            with open(ca_bundle_path,'w') as file:
                file.write(ssl_cert)
            webhook_config['api_parameter']['api_ssl_verify']=ca_bundle_path
            logger.info(f"SSL verify updated with CA bundle path")
        webhook_auth_token = secret_params["Authorization"]
        webhook_config["api_parameter"]["api_headers"]["Authorization"] = webhook_auth_token

        webhook_config["api_parameter"]["api_body"][
            "requestNumber"] = job_run_id
        webhook_config["api_parameter"]["api_body"][
            "folderPath"] = s3_output_folder_uri
        webhook_config["api_parameter"]["api_body"][
            "timestamp"] = job_start_time
        webhook_config["api_parameter"]["api_body"][
            "filesCount"] = f"{no_of_files}"
        webhook_config["api_parameter"]["api_body"][
            "folderName"] = source_name
        webhook_config["api_parameter"]["api_body"][
            "subFolderName"] = s3_formated_folder_name
        webhook_config["api_parameter"]["api_body"]["project"] = genai_project
        httpclient = HTTPClient(webhook_config)
        api_response, success, api_status_code = httpclient.run()
        # TODO need to update in audit file
        if not success:
            return FAILURE, None
        
        return SUCCESS, api_response
    except Exception as e:
        logger.error(
            f"Failure during call of webhook for {source_name} with error {e}"
        )
        return FAILURE, None
# Gen AI webhook API processor ends ---

# Init the logger
logger = get_logger(__name__)


# Init the job start time
job_start_time = generate_timestamp_string(
    ).strftime(TIMESTAMP_FORMAT_WITH_UTC)
logger.info(f"==================job Start time is :{job_start_time} ============")

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name"]
)

source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
access_token = None

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)
    
logger.info(f" Metadata Response :{metadata}")

sharepoint_config = metadata
logger.info(f" Sharepoint Export Config :{sharepoint_config}")



#Assign variables
region = sharepoint_config['aws_region']
oauth_endpoint_method=sharepoint_config['oauth_parameter']['oauth_endpoint_method']
oauth_endpoint_prefix=sharepoint_config['oauth_parameter']['oauth_endpoint_prefix']
oauth_endpoint_suffix=sharepoint_config['oauth_parameter']['oauth_endpoint_suffix']
oauth_scope=sharepoint_config['oauth_parameter']['oauth_scope']
oauth_grant_type=sharepoint_config['oauth_parameter']['oauth_grant_type']

# Constructing the URL for SharePoint REST API to get items from the document library
# sharepoint_site_url = f"{sharepoint_url_prefix}/sites/{sharepointSiteName}/_api/web/lists/getByTitle('{sharepointDocumentLibrary}')/items"
sharepoint_site_url = f"{sharepoint_url_prefix}/sites/{sharepointSiteName}"

sharepoint_url_prefix =sharepoint_config['job_parameter']['sharepoint_url_prefix'] #https://worleyparsons.sharepoint.com/
sharepointHostName=sharepoint_config['job_parameter']['sharepointHostName']
sharepointSiteName=sharepoint_config['job_parameter']['sharepointSiteName']
sharepointDocumentLibrary=sharepoint_config['job_parameter']['sharepointDocumentLibrary']
s3_bucket_name=sharepoint_config['job_parameter']['bucket_name']
s3_root_folder=sharepoint_config['job_parameter']['S3_folder']
incremental_criteria_folder_location=sharepoint_config['job_parameter']['incremental_criteria_folder_location']
full_incremental=sharepoint_config['job_parameter']['full_incremental']
file_filters=sharepoint_config['job_parameter']['file_filters']
folder_filters=sharepoint_config['job_parameter']['folder_filters']
kms_key_id=sharepoint_config['job_parameter']['kms_key_id']
rds_cluster=sharepoint_config['job_parameter']['rds_cluster']
rds_schema=sharepoint_config['job_parameter']['rds_schema']
rds_sharepoint_files_metadata=sharepoint_config['job_parameter']['rds_sharepoint_files_metadata']
rds_job_audit=sharepoint_config['job_parameter']['rds_job_audit']
api_sharepoint_library_prefix = sharepoint_config['job_parameter']['api_sharepoint_library_prefix']
# Init connection to S3 bucket
s3_client = S3(s3_bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(sharepoint_config['oauth_parameter']['secret_key'], region))
azure_tenant_id=secret_param_key.get('azure_tenant_id')
client_id=secret_param_key.get('client_id')
client_secret=secret_param_key.get('client_secret')
oauth_url = f'{oauth_endpoint_prefix}{azure_tenant_id}{oauth_endpoint_suffix}'

# Fetch RDS Secrets from secrets manager
secret_rds_param_key = json.loads(get_secret(sharepoint_config['job_parameter']['secret_key_rds'], region))


sharepoint_file_list = []
sharepoint_folder_list = []
sharepoint_item_list = []


# Main logic to fetch and upload files from SharePoint to S3
try:
    incremental_criteria_folder_location=f'{incremental_criteria_folder_location}/{function_name}_audit'
    incremental_file_name=f"{source_name}_{function_name}_audit.txt"
    if sharepointDocumentLibrary == sharepointDocumentLibrary:
        content,status=s3_client.read_s3_file(incremental_criteria_folder_location,incremental_file_name)
        # Incremental logic will be applied only if the audit file is available & full_incremental is set to I
        if status and full_incremental == 'I':
            incremental_datetimestamp=content.split(',')[2]
            file_filters["incremental_datetimestamp"]=incremental_datetimestamp
            incremental_dt=datetime.strptime(incremental_datetimestamp, "%Y-%m-%d %H:%M:%S %Z")
            lastprocesseddate = incremental_dt.strftime("%Y-%m-%d")
            logger.info("incremental_datetimestamp --> " + incremental_datetimestamp)
        else:
            logger.error("Initial Load. Incremental file not found")
            incremental_datetimestamp=None
            lastprocesseddate=None
            file_filters["incremental_datetimestamp"]=None
    
    # Generate access token for sharepoint             
    token_status=generate_access_token()

    # Call share point api if the token is generated successfully.
    if token_status:
        logger.info(f"Token generated successfully.")
        sharepoint_site_id,api_status, api_resp_code = get_sharepoint_site_id(sharepointHostName,sharepointSiteName)
    else:
        logger.error(f"Token generation failed.")
        sys.exit(1)  # Exit with failure code

    # If sharepoint_site is available, continue with data extraction
    if sharepoint_site_id is not None:
        logger.info(f"Sharepoint Site Id for {sharepointSiteName} is {sharepoint_site_id}")

        sharepointlist=get_sharepoint_item_list(sharepoint_site_id,sharepointDocumentLibrary,lastprocesseddate)
    else:
        logger.error(f"Sharepoint Site ID not available. Validate the sharepoint site name {sharepointSiteName}")
        sys.exit(1)  # Exit with failure code

    logger.info(f'Number of files found:  {len(sharepointlist)}')
    sharepointlist.sort(key=lambda x: x['folder_path'])
    #logger.info(sharepointlist)
    logger.info(f'File filters applied -> {file_filters}')

    #Filter files if available for file extension and incremental date
    filtered_file_list=filter_files(sharepointlist, file_filters,folder_filters)
    logger.info(f'Number of files found after applying filter:  {len(filtered_file_list)}')

    # Download files if available.
    numberOfAvailableFiles=len(filtered_file_list)
    if numberOfAvailableFiles > 0:
        logger.info(f"Starting file downloads")
       
        downloaded_filelist = download_sharepoint_files(
        filtered_file_list,
        sharepoint_site_url,
        access_token,
        s3_client,
        s3_root_folder,
        kms_key_id,
        job_start_time
        )

        # Identify new, updated, and deleted files
        processed_files, current_file_ids = identify_new_vs_updated_deleted_files(downloaded_filelist)
        df_filedata = spark.createDataFrame(processed_files)

        df_filedata = df_filedata.withColumn("jobid", lit(job_run_id))
        df_filedata = df_filedata.withColumn("created_date", from_utc_timestamp(current_timestamp(), "UTC"))
        df_filedata = (df_filedata
              .withColumnRenamed("name", "file_name")
              .withColumnRenamed("folder_path", "sharepoint_folderpath")
              .withColumnRenamed("id", "sharepoint_fileid")
              .withColumnRenamed("webUrl", "sharepoint_weburl")
              .withColumnRenamed("createdDateTime", "sharepoint_createdDateTime")
              .withColumnRenamed("lastModifiedDateTime", "sharepoint_lastModifiedDateTime")
              .withColumnRenamed("file_status", "status")
              .withColumnRenamed("is_updated", "is_updated"))
        
        columns_to_drop = ["driveId", "folderPath","id","lastModifiedDateTime","name","parentFolderId",
                           "parentFolderName","siteId","ContentType","DocIcon"]
        df_filedata = df_filedata.drop(*columns_to_drop)
        df_filedata = df_filedata.withColumn("sharepoint_createddatetime", to_timestamp("sharepoint_createddatetime", "yyyy-MM-dd'T'HH:mm:ssX"))
        df_filedata = df_filedata.withColumn("sharepoint_lastModifiedDateTime", to_timestamp("sharepoint_lastModifiedDateTime", "yyyy-MM-dd'T'HH:mm:ssX"))
        
        # Convert the custom_metadata dictionary to a JSON string
        from pyspark.sql.functions import to_json, struct
        columns_to_select = [col for col in df_filedata.columns if col != "custom_metadata"]
        df_filedata = df_filedata.withColumn("metadata_json", to_json(struct("custom_metadata")))
        df_filedata = df_filedata.drop("custom_metadata")

        #Recon number of files available to download and files downloaded
        numberOfFileDownloaded=df_filedata.filter(df_filedata["s3_FileName"].isNotNull()).count()
        if numberOfAvailableFiles == numberOfFileDownloaded:
            logger.info(f"File available and downloaded matches. Recon successful")
        else:
            logger.info(f"File available and downloaded don't matches. Recon failed")
            logger.error(f"Job failed due to recon mismatch")
            sys.exit(1)

        # Update audit file with current job datetime for next incremental run
        sharepoint_audit_column=f"{source_name},{function_name},{job_start_time}"  
        logger.info("sharepoint_audit_data --> " + sharepoint_audit_column)
        sharepoint_audit_full_file = f'{incremental_criteria_folder_location}/{incremental_file_name}'


        # Create a DataFrame using literals
        df_audit = spark.createDataFrame(
        [(job_run_id, source_name, function_name, numberOfAvailableFiles)],
        ["jobid", "source_name", "function_name", "file_count"]
        )
        df_audit = df_audit.withColumn("created_date", from_utc_timestamp(current_timestamp(), "UTC"))


        # Update RDS with audit details
        pg_jdbc_url = f"jdbc:postgresql://{rds_cluster}"
        pg_username = secret_rds_param_key['username']
        pg_password = secret_rds_param_key['password']
        dbtable_metadata=f'"{rds_schema}".{rds_sharepoint_files_metadata}'
        dbtable_audit=f'"{rds_schema}".{rds_job_audit}'
        

        # Write the data to PostgreSQL
        df_filedata.write \
        .format("jdbc") \
        .option("url", pg_jdbc_url) \
        .option("dbtable",dbtable_metadata) \
        .option("user", pg_username) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

        df_audit.write \
        .format("jdbc") \
        .option("url", pg_jdbc_url) \
        .option("dbtable", dbtable_audit) \
        .option("user", pg_username) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

        if sharepoint_audit_column:
            if s3_client.upload_to_s3(sharepoint_audit_column,sharepoint_audit_full_file,kms_key_id,is_gzip=False):
                logger.info(f"Uploaded audit file for sharepoint {source_name} & document library {function_name} info to {sharepoint_audit_full_file}")
                success = True
            else:
                logger.error("Uploading audit file  to S3 {sharepoint_audit_full_file} failed")
                success = False

        if "notification_parameter" in sharepoint_config["job_parameter"]:
            notification_config = sharepoint_config["job_parameter"][
                "notification_parameter"]
            genai_project = sharepoint_config["job_parameter"]["genai_project_name"]
            notf_ddb_system_id = notification_config["SourceSystemId"]
            notf_ddb_metatype = notification_config["MetadataType"]
            s3_output_path_uri, batch_current_folder_name = get_curent_job_folder_uri()
            s3_output_absolute_folder_uri = f"s3://{s3_bucket_name}/{s3_output_path_uri}"
        success, response = invoke_webhook_notification_process(
            job_run_id,
            metadata_table_name,
            source_name,
            notf_ddb_system_id,
            notf_ddb_metatype,
            s3_output_absolute_folder_uri,
            batch_current_folder_name,
            job_start_time,
            genai_project,
            numberOfAvailableFiles
        )
        if not success:
            logger.error(
                            f"Notification for Ingestion Job for Sharepoint failed"
                        )
                        # TODO need to send sns for failure
            sys.exit(1)
        else:
            logger.info(
                            f"Notification for Ingestion Job for Sharepoint success"
                        ) 
        
        logger.info("Completed the SharePoint to S3 file transfer task.")

    else:
        logger.info(f"No files are available to download")
    job.commit()
except Exception as e:
       logger.error("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code
