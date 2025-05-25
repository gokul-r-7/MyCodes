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
from pyspark.sql.functions import col, when, to_json, col, expr,coalesce
import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, TimestampType
from awsglue.gluetypes import *
from pyspark.sql.functions import explode
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import to_timestamp,current_timestamp, from_utc_timestamp
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
from psycopg2 import sql

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
        'resource': oauth_resource,
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

# Get folder and files list from Sharepoint Document Library
def get_sharepoint_item_list(sharepointsite_name,document_library_name,sharepointDocumentLibraryQueryParam):
    logger.info(f"Get data for {document_library_name} in {sharepointsite_name} with param {sharepointDocumentLibraryQueryParam}")
    sharepoint_item_list=[]
    sharepoint_url = f"https://{sharepointsite_name}/_api/web/lists/getbytitle('{document_library_name}')/items?{sharepointDocumentLibraryQueryParam}"    
    page_number=1
    while sharepoint_url:
        url=sharepoint_url
        logger.info(f"Requesting page {page_number}: {url}")
        sharepoint_url=None
        response, api_status, api_resp_code = get_sharepoint_data(url)
        if api_resp_code == 200:
            # get file's and folder's list
            sharepoint_item_list.extend([item for item in response.get("value", []) if item.get("FSObjType") == 0])
            sharepoint_url=response.get('odata.nextLink')
            page_number += 1
        else:
            logger.error(f"API call failed. Exiting")
            sys.exit(1)
    return sharepoint_item_list

# Get list of files and folders from Sharepoint response
def clean_sharepoint_list(sharepointData_list,api_fields_to_remove):
    current_s3_folder_uri, incremental_folder_name=get_curent_job_folder_uri()
    weburl_replace_string = api_sharepoint_library_prefix
    # convert job start time to "%Y%m%d_%H%M%S"
    dt = datetime.strptime(job_start_time, "%Y-%m-%d %H:%M:%S %Z")
    incremental_folder_name = dt.strftime("%Y%m%d_%H%M%S") + "_UTC"
    for item in sharepointData_list:
        file_name=item.get("FileLeafRef")
        relative_file_path=item.get("FileRef")
        item["source_id"]=source_id
        item["file_name"]=file_name
        item["s3_filename"]=f"{current_s3_folder_uri}{relative_file_path.replace(weburl_replace_string,'')}"
        item["sharepoint_folderpath"]=relative_file_path.replace(file_name,"")
        item["sharepoint_fileid"]=item["Id"]
        item["sharepoint_createddatetime"]=item["Created"]
        item["sharepoint_lastmodifieddatetime"]=item["Modified"]
        item["sharepoint_weburl"]=f"https://{sharepointsiteurl}/_api/web/GetFileByServerRelativeUrl('{relative_file_path}')/$value"

    # Cleaned list with unwanted fields removed
    fields_to_remove=["FileLeafRef","FileRef","Id","ID","Created","Modified","odata.type","odata.id", "odata.etag"
                            ,"odata.editLink","FileSystemObjectType","ServerRedirectedEmbedUri","ServerRedirectedEmbedUrl","AuthorId",
                            "EditorId","OData__CopySource","CheckoutUserId","OData__UIVersionString","GUID","SharedWithUsersId","SharedWithDetails"]
    if api_fields_to_remove:
        fields_to_remove.extend(api_fields_to_remove)
    sharepointData_list = [
    {key: value for key, value in item.items() if key not in fields_to_remove}
    for item in sharepointData_list
    ]
    return sharepointData_list

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
            folder_path=file.get('sharepoint_folderpath')
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
        file_extension = get_file_extension(file["file_name"])
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
        lastModifiedDateTime = datetime.strptime(file["sharepoint_lastmodifieddatetime"], "%Y-%m-%dT%H:%M:%SZ")
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

# Function to get file extension
def get_file_extension(filename):
    return filename[filename.rfind('.'):]

def group_custom_fields(file_list,core_fields,is_custom_fields_req):
    modified_file_list = []
    for file in file_list:
        new_file = {key: file[key] for key in core_fields if key in file}  # Keep core fields
        custom_fields = {key: file[key] for key in file if key not in core_fields}  # Collect extra fields
        if custom_fields and is_custom_fields_req:
            new_file["customfields"] = json.dumps(custom_fields)
        modified_file_list.append(new_file)
    return modified_file_list

# Function download files from Sharepoint
def download_sharepoint_files(filelist):
    file_upload_count=0
    for file_info in filelist:
        fileName = file_info['s3_filename']
        url = file_info['sharepoint_weburl']
        logger.info(f"File download started for  {fileName} with url {url}")
        headers = {"Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",  # Adding Content-Type header
        }
        retry_count = 0
        while(retry_count < 2):
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    break
                elif response.status_code  in (401,403):
                    auth_status = generate_access_token()
            except requests.exceptions.RequestException as e:
                print("Download of file failed. Retrying")
            retry_count=retry_count+1
        if response:
          if s3_client.upload_to_s3(response.content,fileName,kms_key_id,is_gzip=False):
            logger.info(f"Uploaded file to {fileName} succesfully")
            file_upload_count=file_upload_count+1
            success = True
          else:
            logger.error("Failed to upload file to S3")
            success = False
            logger.info(f"Error downloading file {fileName} from {url}")
            sys.exit(1)  # Exit with failure code
        logger.info(f'File download completed for {fileName}')
    return filelist,file_upload_count

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
    current_s3_folder_uri = f"{s3_root_folder}/{incremental_folder_name}/"
    return current_s3_folder_uri, incremental_folder_name

# cdc check
def check_file_cdc(df_delta_filedata,df_full_filedata,df_history_filedata):
    key_cols=["file_name", "sharepoint_folderpath"]
    #Set detla as new or update
    delta_joined = df_delta_filedata.join(
        df_history_filedata.select(*key_cols).distinct(),
        on=key_cols,
        how="left_semi"
    )
    df_delta_updates = delta_joined.withColumn("status", lit("update"))

    df_delta_new = df_delta_filedata.join(
        df_history_filedata.select(*key_cols).distinct(),
        on=key_cols,
        how="left_anti"
    ).withColumn("status", lit("new"))

    df_delta_filedata=df_delta_updates.unionByName(df_delta_new)

    # Identify deleted files (in history but missing in current full snapshot)
    df_deleted_filedata = df_history_filedata.join(
        df_full_filedata.select(*key_cols).distinct(),
        on=key_cols,
        how="left_anti"
    )

    df_deleted_filedata = df_deleted_filedata.withColumn("status", lit("delete"))
    df_deleted_filedata_updated = df_deleted_filedata.withColumn("jobid", lit(job_run_id))
    df_deleted_filedata_updated = df_deleted_filedata_updated.withColumn("created_date", from_utc_timestamp(current_timestamp(), "UTC"))

    # Add deleted records to delta dataframe
    df_delta_filedata=df_delta_filedata.unionByName(df_deleted_filedata_updated)

    return df_delta_filedata,df_deleted_filedata
# GenAI Notification Webhook API start here ---
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
oauth_resource=sharepoint_config['oauth_parameter']['oauth_resource']
oauth_grant_type=sharepoint_config['oauth_parameter']['oauth_grant_type']
sharepointsiteurl=sharepoint_config['job_parameter']['sharepointsiteurl']
sharepointDocumentLibrary=sharepoint_config['job_parameter']['sharepointDocumentLibrary']
sharepointDocumentLibraryQueryParam=sharepoint_config['job_parameter']['sharepointDocumentLibraryQueryParam']
s3_bucket_name=sharepoint_config['job_parameter']['bucket_name']
s3_root_folder=sharepoint_config['job_parameter']['S3_folder']
incremental_criteria_folder_location=sharepoint_config['job_parameter']['incremental_criteria_folder_location']
full_incremental=sharepoint_config['job_parameter']['full_incremental']
file_filters=sharepoint_config['job_parameter']['file_filters']
kms_key_id=sharepoint_config['job_parameter']['kms_key_id']
rds_cluster=sharepoint_config['job_parameter']['rds_cluster']
rds_schema=sharepoint_config['job_parameter']['rds_schema']
rds_sharepoint_files_metadata=sharepoint_config['job_parameter']['rds_sharepoint_files_metadata']
rds_job_audit=sharepoint_config['job_parameter']['rds_job_audit']
is_custom_fields_req=sharepoint_config['job_parameter']['is_custom_fields_req']
is_cdc_required = sharepoint_config['job_parameter'].get('is_cdc_required', False)
is_sharepointcloud = sharepoint_config['job_parameter'].get('is_sharepointcloud', True)
api_response_fields_to_remove=sharepoint_config['job_parameter'].get('api_fields_to_remove',None)
api_sharepoint_library_prefix = sharepoint_config['job_parameter']['api_sharepoint_library_prefix']
source_id=f"{source_name}_{function_name}"
folder_filters=sharepoint_config['job_parameter']['folder_filters']
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

    #Init Postgres Details
    pg_jdbc_url = f"jdbc:postgresql://{rds_cluster}"
    pg_username = secret_rds_param_key['username']
    pg_password = secret_rds_param_key['password']
    dbtable_metadata=f'"{rds_schema}".{rds_sharepoint_files_metadata}'
    dbtable_audit=f'"{rds_schema}".{rds_job_audit}'

    if sharepointDocumentLibrary is not None:
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
    else:
        logger.error(f"Token generation failed.")
        sys.exit(1)  # Exit with failure code
    
    # If sharepoint_site is available, continue with data extraction
    logger.info(f"Getting items from Sharepoint Site Id  {sharepointsiteurl} for list {sharepointDocumentLibrary}")
    sharepoint_item_list=get_sharepoint_item_list(sharepointsiteurl,sharepointDocumentLibrary,sharepointDocumentLibraryQueryParam)

    # Clean up sharepointlist results
    #list of fields to be removed
    logger.info(f"Clean up file informations from api response and construct the file path")
    sharepoint_item_list=clean_sharepoint_list(sharepoint_item_list,api_response_fields_to_remove)
    
    #Handle custom fields if required
    core_fields=['source_id','file_name','sharepoint_folderpath','sharepoint_fileid','sharepoint_weburl','sharepoint_createddatetime','sharepoint_lastmodifieddatetime','s3_filename']
    sharepoint_item_list=group_custom_fields(sharepoint_item_list,core_fields,is_custom_fields_req)

    #Sort file list by id
    sharepoint_item_list.sort(key=lambda x: x['sharepoint_fileid'])

    # Create a new variable to hold full list
    if is_cdc_required:
        sharepoint_item_fulllist=sharepoint_item_list 
    logger.info(f'Number of files found:  {len(sharepoint_item_list)}')
    
    #Filter for delta files
    logger.info(f'File filters applied -> {file_filters}')

    #Filter files if available for file extension and incremental date
    delta_filelist=filter_files(sharepoint_item_list,file_filters,folder_filters)
    logger.info(f'Number of files found after applying filter:  {len(delta_filelist)}')


    # Download files if available.
    numberOfAvailableFiles=len(delta_filelist)
    if numberOfAvailableFiles > 0:
        logger.info(f"Starting file downloads")
        downloaded_filelist,total_file_uploaded=download_sharepoint_files(delta_filelist)
        if numberOfAvailableFiles == total_file_uploaded:
            logger.info(f"File available and downloaded matches. Recon successful")
        else:
            logger.info(f"File available and downloaded don't matches. Recon failed")
            logger.error(f"Job failed due to recon mismatch")
            sys.exit(1)
        # Convert Delta list into Dataframe for further processing
        df_delta_filedata = spark.createDataFrame(downloaded_filelist)
        df_delta_filedata = df_delta_filedata.withColumn("jobid",lit(job_run_id))
        df_delta_filedata = df_delta_filedata.withColumn("created_date", from_utc_timestamp(current_timestamp(), "UTC"))
        df_delta_filedata = df_delta_filedata.withColumn("sharepoint_createddatetime", to_timestamp("sharepoint_createddatetime", "yyyy-MM-dd'T'HH:mm:ssX"))
        df_delta_filedata = df_delta_filedata.withColumn("sharepoint_lastmodifieddatetime", to_timestamp("sharepoint_lastmodifieddatetime", "yyyy-MM-dd'T'HH:mm:ssX"))
    else: # Create a empty df
        from pyspark.sql.types import TimestampType
        schema = StructType([
                StructField("jobid", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("s3_filename", StringType(), True),
                StructField("sharepoint_folderpath", StringType(), True),
                StructField("sharepoint_fileid", StringType(), True),
                StructField("sharepoint_createddatetime", TimestampType(), True),
                StructField("sharepoint_lastmodifieddatetime", TimestampType(), True),
                StructField("sharepoint_weburl", StringType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("customfields", StringType(), True),
                StructField("source_id", StringType(), True)
            ])
        df_delta_filedata = spark.createDataFrame([], schema)
    if is_cdc_required:
        if len(sharepoint_item_fulllist) > 0:
            df_full_filedata=spark.createDataFrame(sharepoint_item_fulllist)
        else:
            df_full_filedata=df_delta_filedata #If size of full is 0, then delta is also 0 and init with a empty dataframe
        # Retrieve historical data
        history_query = f"(SELECT * FROM {dbtable_metadata} WHERE source_id='{source_id}' and status not in ('delete','inactive')) AS history_table"
        df_history_filedata = spark.read \
            .format("jdbc") \
            .option("url", pg_jdbc_url) \
            .option("dbtable", history_query) \
            .option("user", pg_username) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        #Check for CDC details
        df_delta_filedata,df_deleted_filedata=check_file_cdc(df_delta_filedata,df_full_filedata,df_history_filedata)
        df_delta_filedata = df_delta_filedata.cache()
        df_delta_update=df_delta_filedata.filter(df_delta_filedata["status"] == "update").cache()
        if df_deleted_filedata.count() > 0 or df_delta_update.count() > 0:
            host_port, dbname = rds_cluster.split('/')
            aurora_host,aurora_port=host_port.split(':')
            schema,table=dbtable_metadata.split('.')
            schema = schema.replace('"', '') 
            conn = psycopg2.connect(
                    host=aurora_host,
                    port=aurora_port,
                    dbname=dbname,
                    user=pg_username,
                    password=pg_password
                )
                # SQL statement to update status
            update_stmt = sql.SQL('''
                UPDATE {table}
                SET status = 'inactive'
                WHERE file_name = %s AND sharepoint_folderpath = %s AND source_id = %s and status in ('new','update')
            ''').format(table=sql.Identifier(schema,table))
            # Collect values from DataFrame
            update_data = df_delta_update.select("file_name", "sharepoint_folderpath", "source_id").collect()
            delete_data = df_deleted_filedata.select("file_name", "sharepoint_folderpath", "source_id").collect()

            # Convert collected data into list of tuples
            update_keys = [(row["file_name"], row["sharepoint_folderpath"], row["source_id"]) for row in update_data]
            delete_keys = [(row["file_name"], row["sharepoint_folderpath"], row["source_id"]) for row in delete_data]

            # Create conn cur for postgres
            cur = conn.cursor()

            # Batch update for df_delta_data
            if update_keys:
                cur.executemany(update_stmt, update_keys)

            # Batch update for df_deleted_data
            if delete_keys:
                cur.executemany(update_stmt, delete_keys)

            # Commit changes
            conn.commit()

            # Clean up
            cur.close()
            conn.close()


    if df_delta_filedata.count() > 0:

        # Create a DataFrame using literals
        df_audit = spark.createDataFrame(
        [(job_run_id, source_name, function_name, numberOfAvailableFiles)],
        ["jobid", "source_name", "function_name", "file_count"]
        )
        df_audit = df_audit.withColumn("created_date", from_utc_timestamp(current_timestamp(), "UTC"))


        # Update RDS with audit details
        # Write the data to PostgreSQL
        df_delta_filedata.write \
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

        # Update audit file with current job datetime for next incremental run
        sharepoint_audit_column=f"{source_name},{function_name},{job_start_time}"  
        logger.info("sharepoint_audit_data --> " + sharepoint_audit_column)
        sharepoint_audit_full_file = f'{incremental_criteria_folder_location}/{incremental_file_name}'


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
