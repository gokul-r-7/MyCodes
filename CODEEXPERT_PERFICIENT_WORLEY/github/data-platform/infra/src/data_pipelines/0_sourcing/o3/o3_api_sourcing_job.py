import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

from worley_helper.utils.logger import get_logger
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, save_spark_df_to_s3_with_specific_file_name, add_masked_columns , extract_column_names
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string, generate_timestamp_iso8601_date_format_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN,TIMEZONE_UTC, AUDIT_DATE_ISO8601_FORMAT
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.metrics import CustomMetrics

import os
import argparse
import json
import requests
import base64
from datetime import datetime, timedelta
from requests.exceptions import RequestException, Timeout
import math
import csv
from typing import Union
import traceback
import time

#setting up for custom metrics 

custom_metrics = CustomMetrics(service= "O3")
tracer = custom_metrics.get_tracer()
metrics = custom_metrics.get_metrics()

# Init the logger
logger = get_logger(__name__)

# Init the start date AUDIT_DATE_ISO8601_FORMAT is audit date format
#audit_date_value = generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)
audit_date_value = generate_timestamp_iso8601_date_format_string(TIMEZONE_UTC, AUDIT_DATE_ISO8601_FORMAT)
# next_day_audit_date_value = (datetime.strptime(audit_date_value, AUDIT_DATE_COLUMN) + timedelta(days=1)).strftime(AUDIT_DATE_COLUMN)
# prev_day_audit_date_value = (datetime.strptime(audit_date_value, AUDIT_DATE_COLUMN) - timedelta(days=1)).strftime(AUDIT_DATE_COLUMN)

runId = datetime.now().strftime('%Y%m%d%H%M%S')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',"source_name", "function_name","metadata_table_name","ProjectId","EndPoint", "ApiType", "masking_metadata_table_name","database_name"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

metrics.add_dimension(name="EndPoint", value=args['EndPoint'])

start_time = time.time()

try:
    source_name = args.get("source_name")
    function_name = args.get("function_name")
    metadata_table_name = args.get("metadata_table_name")
    ProjectId = int(args.get("ProjectId"))
    EndPoint = args.get("EndPoint")
    ApiType = args.get("ApiType")
    database_name = args.get("database_name")
    masking_metadata_table_name = args.get("masking_metadata_table_name")
    endpoint_without_lash = '_'.join(EndPoint.split('/'))
    
    # Define the Sort Keys for DynamoDB Fetch
    input_keys = "raw#" + source_name + "#" + function_name
    
    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=input_keys
    )
    
    logger.info(f" Metadata Response :{metadata}")
    
    config = metadata
    httpTimeoutInterval=config['api_parameter']['api_timeout']
    bucket_name = config['job_parameter']['bucket_name']
    raw_data_location = config['job_parameter']['input_path']
    raw_data_location=f"{raw_data_location}/{str(ProjectId)}/raw_data"
    parquet_data_path = config['job_parameter']['output_s3']
    region = config['aws_region']
    incremental_default_date = config['job_parameter']['incremental_default_date']
    incremental_criteria_folder_location = config['job_parameter']['incremental_criteria_folder_location']
    kms_key_id = config['job_parameter']['kms_key_id']
    sample_data_path = config['job_parameter']['schema_output_s3']
    sampling_fraction = float(config['job_parameter']['sampling_fraction'])
    sampling_seed = config['job_parameter']['sampling_seed']
    api_pagecount = config['api_parameter']['api_pagecount']
    change_history_custom_initial_date = config['job_parameter']['change_history_default_date']
    
    input_temp_path=f"s3://{bucket_name}/{raw_data_location}"
    s3_client = S3(bucket_name, region)

    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"
except Exception as e:
    logger.error(f"Failed to initialize job variables : {traceback.format_exc()}")
    raise e

# Retrieve Page Count
def api_page_count(Host, headers, EndPoint, ProjectID, entityType, Filters):
    try:
        logger.info("first_record_page")

        if entityType is None or entityType == 0:
            url = Host + EndPoint + '/list/' + str(ProjectID) + '?requireTotalCount=true&take=1'
        else:
            url = Host + EndPoint + '/list/' + str(ProjectID) + '/' + str(entityType) + '?requireTotalCount=true&take=1&ShowComplete=true'
        if len(str(Filters).strip()) > 0:
            url += '&filter=' + Filters
        logger.info(f"Page Count URL : {url}")
        resp = requests.get(url, headers=headers, timeout=httpTimeoutInterval)
        resp_json = resp.json()
        total_count = resp_json.get('totalCount', 0)
        logger.info(total_count)

        return True, total_count
    except Exception as ex:
        logger.error(ex)
        return False, 0

def api_page_data(Host, headers, EndPoint, ProjectID, EntityType, filters :str, PageNo: int, filePath):
    try: 
        page_size= api_pagecount
        page_offset = 1
        # file_path = f"/{'_'.join(EndPoint.split('/'))}/"
        file_path = f"/{endpoint_without_lash}/"

        if EntityType is None or EntityType == 0:
            url = Host + EndPoint + '/list/'  + str(ProjectID) + '?'
            file_name = f"export_{EndPoint}_{ProjectID}_{PageNo}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        else:
            url = Host + EndPoint + '/list/'  + str(ProjectID) + '/' + str(EntityType) + '?ShowComplete=true'
            file_path = file_path + f"entityType={EntityType}/"           
            file_name = f"export_{EndPoint}_{ProjectID}_{EntityType}_{PageNo}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        
        if PageNo > 1:
            page_offset = page_size * (PageNo - 1)
            url += '&skip=' + str(page_offset) + '&take=' + str(page_size)
        else:
            url += '&take=' + str(page_size)
        if len(filters.strip()) > 0:
            url += '&filter=' + filters
        
        logger.info(f"API URL : {url}")
        resp = requests.get(url, headers=headers, timeout=httpTimeoutInterval)
        logger.info(resp)
        jsonContent = resp.json()
        filePath = filePath+file_path+file_name
        logger.info(f"File Path : {filePath}")
        if jsonContent:
            if s3_client.upload_to_s3(bytes(json.dumps(jsonContent).encode('UTF-8')),filePath,kms_key_id,is_gzip = False):
                logger.info(f"Uploaded O3 Project {EndPoint} info to {bucket_name}")
                # success = True
            else:
                logger.error("Failed to upload O3 Project error info")
                # success = False
            logger.info(f"Extraction complete for : {EndPoint}")
        return True
    except Exception as ex:
        logger.error(ex)
        return False

def get_entity_types(Host, headers):
    try: 
        url = Host + 'entity-types/list'
        resp = requests.get(url, headers=headers).json()
        if resp:
            return [i['ID'] for i in resp]
        else:
            return []
    except Exception as e:
        raise e

# Extract O3 Data
def extract_data(inst_host: str, inst_api_key:str, end_point: str,  ProjectID: int, filters, entityType, filePath, total_record_count, curr_page=None) -> Union[bool, str]:
        # success, total_record_count = api_page_count(
        #     inst_host, inst_api_key, end_point, 
        #     ProjectID, entityType, filters)

        # if total_record_count == 0: 
        #     return False, 'No Record Exists', None
        success = False

        pages = math.ceil(total_record_count / api_pagecount)
        if curr_page:
            page = curr_page
        else:
            page = 1

        while page <= pages:
            success = api_page_data(
                inst_host,
                inst_api_key,
                end_point,
                ProjectID,
                entityType,
                filters,
                page,
                filePath
            )
            if success != True:
                return False, 'Error Getting Data', page
            page += 1

        return success, "Completed", None

    # Extract O3 Data
def extract_data_type3(Host, headers, EndPoint, ProjectID, Filters, filePath):
    try:
        logger.info(f"Fetching API data for O3 Project {EndPoint} to {bucket_name}")
        url = Host + EndPoint + "/" + str(ProjectID)
        # filePath = filePath + f"/{'_'.join(EndPoint.split('/'))}/"
        # file_path = f"/{'_'.join(EndPoint.split('/'))}/"
        file_path = filePath + f"/{endpoint_without_lash}/"
        if len(str(Filters).strip()) > 0:
            url += '?filter=' + Filters

        logger.info(f" Type 3 api url : {url}")
        resp = requests.get(url, headers=headers, timeout=httpTimeoutInterval)
        jsonContent = resp.json()
        if jsonContent:
            file_name = f"export_{EndPoint}_{ProjectID}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            file_name = file_name.replace('/','_')
            file_path = file_path+file_name
            logger.info(f"File path for {EndPoint} : {file_path}")


            if s3_client.upload_to_s3(bytes(json.dumps(jsonContent).encode('UTF-8')),file_path,kms_key_id,is_gzip = False):
                logger.info(f"Uploaded O3 Project {EndPoint} info to {bucket_name}")
                # success = True
            else:
                logger.error("Failed to upload O3 Project error info")
                # success = False
            logger.info(f"Extraction complete for : {EndPoint}")
        return True, "Completed"
    except Exception as ex:
        logger.error(ex)
        return False, 'Error Getting Data'

def api_runner(config, EndPoint, ProjectId, incremental_filter, entityType, raw_data_location):
    retry_count = 0
    retry_page_count = 0
    success = False
    curr_page = None
    total_record_count = -1
    
    if ApiType in [TYPE1,TYPE2]:
        while retry_page_count < config['api_parameter']['api_retry']:
            logger.info(f"API Page Count retry count : {retry_page_count}")
            success, total_record_count = api_page_count(
                        config['api_parameter']['api_host'], config['api_parameter']['api_headers'], EndPoint, 
                        ProjectId, entityType, incremental_filter)
            logger.info(f"Success : {success} , Record_Count : {total_record_count}")

            if success:
                logger.info(f"Record count of Data from O3 for project {ProjectId} and endpoint {EndPoint} completed successfully.")
                break

            retry_page_count += 1

        metrics.add_metric(name="total_record_count", unit="Count", value=total_record_count)
    
        if (success == False):
            return False, total_record_count
        else:
            if total_record_count == 0:
                return True, total_record_count
        
        
    
    while retry_count < config['api_parameter']['api_retry']:
        if ApiType in [TYPE1,TYPE2]:
            success, data, curr_page = extract_data(
                            config['api_parameter']['api_host'],
                            config['api_parameter']['api_headers'],
                            EndPoint,
                            ProjectId,
                            incremental_filter,
                            entityType,raw_data_location, total_record_count, curr_page)
        else:
            success, data = extract_data_type3(
                            config['api_parameter']['api_host'],
                            config['api_parameter']['api_headers'],
                            EndPoint,
                            ProjectId,
                            incremental_filter,
                            raw_data_location)
        if success:
            logger.info(f"Extraction of Data from O3 for project {ProjectId} and endpoint {EndPoint} completed successfully.")
            break
        else:
            #Retry API
            if curr_page is None:
                retry_count += 1
            else:
                retry_count = 0

    metrics.add_metric(name="total_record_count", unit="Count", value=total_record_count)

    return success, total_record_count

try:

    success = False
    folder_name=f"{incremental_criteria_folder_location}"
    file_name=f"{source_name}_{function_name}_{EndPoint}_{str(ProjectId)}.txt"
    content,status=s3_client.read_s3_file(folder_name,file_name)
    
    logger.info(f"Incremental existence status : {status}")
    if status:
        incremental_date_project_file=content.split(',')[4]
        incremental_from_date=incremental_date_project_file
        logger.info("incremental_date_project_file --> " + incremental_date_project_file)
    else:
        if EndPoint == 'change-history':
            incremental_from_date = change_history_custom_initial_date
        else:
            incremental_from_date = incremental_default_date
    
    logger.info("incremental_from_date --> " + incremental_from_date)
    if EndPoint == 'change-history':
        incremental_filter = '[["DateCreated",">=","'+ incremental_from_date + '"],["DateCreated","<","'+ audit_date_value + '"]]'
    else:
        incremental_filter = '[["DateModified",">=","'+ incremental_from_date + '"],["DateModified","<","'+ audit_date_value + '"]]'
    
    if (ApiType == TYPE1) | (ApiType == TYPE3):
        entity_types = None
    else:
        entity_types = get_entity_types(config['api_parameter']['api_host'],config['api_parameter']['api_headers'])
        entity_types.sort()
        logger.info(f"Entity Types : {entity_types}")

    logger.info(f"Extracting data for O3 project {ProjectId} for all {EndPoint}")

    authentication_failure = False
    if entity_types:
        record_count = 0
        for entityType in entity_types:
            logger.info(f"Extracting data for O3 project {ProjectId} for all {EndPoint} Entity Type : {entityType}")
            success, rc = api_runner(config, EndPoint, ProjectId, incremental_filter, entityType, raw_data_location)
            record_count += rc
    else:
        success, record_count = api_runner(config, EndPoint, ProjectId, incremental_filter, entity_types, raw_data_location)

except Exception as e:
    logger.error(f"Code Failed at : {traceback.format_exc()}")
    raise e

try:
    logger.info(f"Success : {success} , Record_Count : {record_count}")
    if success:
        is_empty = True
        if (record_count > 0) | (record_count < 0):
            partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
            # endpoint_without_lash = '_'.join(EndPoint.split('/'))
            temp_data_path = f"{input_temp_path}/{endpoint_without_lash}/"
            logger.info(f"Fetching Data from : {temp_data_path}")
            _dyf = glueContext.create_dynamic_frame.from_options(
                            connection_type="s3",
                            connection_options={
                                "paths": [
                                    temp_data_path
                                    ],
                                    "recurse": True
                            },
                            format="json",
                            transformation_ctx=f"{EndPoint} : Dynamic Frame",
                        )
            if (ApiType == TYPE3 and _dyf.count() > 0) or (ApiType != TYPE3 and _dyf.filter(lambda x: len(x['data']) > 0).count() > 0):
                is_empty = False
        
        if not is_empty:
        
            if ApiType in [TYPE1,TYPE2]:
                _dyf = _dyf.toDF().select(F.explode('data').alias('data')).select('data.*')
                
            else :
                _dyf = _dyf.toDF().select(F.col("Settings.SundayHours").alias("SundayHours"),
                                        F.col("Settings.MondayHours").alias("MondayHours"),
                                        F.col("Settings.TuesdayHours").alias("TuesdayHours"),
                                        F.col("Settings.WednesdayHours").alias("WednesdayHours"),
                                        F.col("Settings.ThursdayHours").alias("ThursdayHours"),
                                        F.col("Settings.FridayHours").alias("FridayHours"),
                                        F.col("Settings.SaturdayHours").alias("SaturdayHours"),
                                        F.col("EntityTypeID"),
                                        F.col("UserCanAddAttachments"),
                                        F.col("UserCanUpdate"),
                                        F.col("UserCanDelete"),
                                        F.col("UserCanEditConstraints"),
                                        F.col("UserCanEditApprovals"),
                                        F.col("UserIsProjectAdmin"),
                                        F.col("UserCanDownload"),
                                        F.col("DetailFormConfigurationID"),
                                        F.explode_outer(F.col("Settings.NonWorkingDays")).alias("NonWorkingDay")
                                    ).withColumn("ProjectId", F.lit(ProjectId))

            #********* Data masking code *******
            # endpoint_without_lash = '_'.join(EndPoint.split('/'))
            
            # # Define the Sort Keys for DynamoDB Fetch
            tablename=endpoint_without_lash
            databasename=database_name
			# # Read Metadata
            ddb1 = DynamoDB(metadata_table_name=masking_metadata_table_name,default_region=REGION)
            masking_metadata = ddb1.get_masking_metadata_from_ddb(
			     databasename=databasename.lower()
			)
			
            logger.info(f" Masking Metadata Response :{masking_metadata}")

            _dyf_latest = _dyf.withColumnRenamed('Project','ProjectInfo')
            
            
            columns_to_hash = extract_column_names(masking_metadata, tablename)
            print(columns_to_hash)
            
            # # Apply the function to hash columns
            _dyf = add_masked_columns(_dyf_latest, columns_to_hash)
            # # Show the resulting dataframe
            _dyf.show(truncate=False)
            _dyf.printSchema()

            _dyf = _dyf.withColumnRenamed('Project','ProjectInfo')
            dynamic_frame = DynamicFrame.fromDF(_dyf, glueContext, "dynamic_frame")
            partition_str = f"{get_partition_str_mi(partition_date)}"
            # endpoint_without_lash = '_'.join(EndPoint.split('/'))
            schema_key = f"{parquet_data_path}/{endpoint_without_lash}/Project={ProjectId}/{partition_str}"
            logger.info(f"Uploading {EndPoint} data for ProjectId = {ProjectId} to {schema_key}")
            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                dynamic_frame,
                bucket_name,
                schema_key,
                EndPoint,
                typecast_cols_to_string = True
            )

            logger.info(f"Selected sample data {source_name}_{ProjectId}_{EndPoint}")

            sample_s3_key = sample_data_path + "/" + endpoint_without_lash

            logger.info(f"Sampling Path : {sample_s3_key}")

            # Write sample data to S3 without partitioning
            if _dyf.limit(51).count() < 50:
                sampling_fraction = float(1.0)

            sample_data = (_dyf.sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed))
            sample_data_dynf = DynamicFrame.fromDF(sample_data, glueContext, "sample dynamic_frame").coalesce(1)

            write_glue_df_to_s3_with_specific_file_name(glueContext, sample_data_dynf, bucket_name, sample_s3_key, EndPoint, handle_voids=True, typecast_cols_to_string = True)
        else:
            logger.info(f"No Data received for {EndPoint} for Project : {ProjectId}")
    else:
        logger.error(f"Failed to Read {EndPoint} data for Project : {ProjectId} from location {input_temp_path}")
        raise Exception
    

    o3_project_audit_object=f"{incremental_criteria_folder_location}/{source_name}_{function_name}_{EndPoint}_{str(ProjectId)}.txt"
    logger.info("o3_project_audit_object --> " + o3_project_audit_object)
    
    
    logger.info("audit_date_value --> " + audit_date_value)
    
    project_audit_column=f"{str(ProjectId)},{source_name},{function_name},{EndPoint},{audit_date_value}"  
    logger.info("project_audit_column --> " + project_audit_column)

    if project_audit_column:
        if s3_client.upload_to_s3(project_audit_column,o3_project_audit_object,kms_key_id,is_gzip=False):
            logger.info(f"Uploaded O3 Project info for {EndPoint} to {bucket_name}")
            success = True
        else:
            logger.error(f"O3 Document {EndPoint} API was successful but the gzip content could not be uploaded to S3")
            success = False
    else:
        logger.error("Failed to fetch O3 Document Register API payload")
        success = False

    input_temp_path_to_delete = s3_client.get_folder_path_from_s3(f"{input_temp_path}/{endpoint_without_lash}")
    logger.info(f"Deleting folder path {input_temp_path_to_delete}")
    s3_client.delete_folder_from_s3(input_temp_path_to_delete)

    job.commit()

    end_time = time.time()
    execution_time = end_time - start_time

    metrics.add_metric(name="execution_time", unit="Seconds", value=execution_time)

except Exception as e:
    logger.error(f"Code Failed at : {traceback.format_exc()}")

    input_temp_path_to_delete = s3_client.get_folder_path_from_s3(f"{input_temp_path}/{EndPoint}")
    logger.info(f"Deleting folder path {input_temp_path_to_delete}")
    s3_client.delete_folder_from_s3(input_temp_path_to_delete)
    raise e