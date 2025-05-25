import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import get_secret, S3, DynamoDB
import pytz
from dateutil.parser import parse
from dateutil.tz import gettz
import boto3
import logging
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from worley_helper.utils.helpers import write_glue_df_to_s3_with_specific_file_name, get_partition_str_mi,save_spark_df_to_s3_with_specific_file_name
import json
import base64
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import traceback
from pyspark.sql import types as t

# custom Utilities   #
from worley_helper.utils.constants import (
    DATE_FORMAT,
    TIMEZONE_SYDNEY,
    REGION,
    TIMESTAMP_FORMAT,
    DATETIME_FORMAT,
)
from worley_helper.utils.helpers import get_partition_str_mi
from worley_helper.utils.api_helpers import call_api
from worley_helper.utils.aws import DynamoDB , SecretsManager
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.helpers import get_partition_str_mi, create_comma_separated_string


# Init the logger
logger = get_logger(__name__)


args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name","project_id", "data_type"]
    )



logger.info(args)

source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
project_id = args.get("project_id")
data_type = args.get("data_type", None)
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
global_data_types = ["global"]
project_id_partition = f"Project={project_id}"
if data_type and (data_type in global_data_types):
    project_id_partition = None
logger.info(f" Project ID :{project_id}")
partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)

logger.info("==================Setting up the Spark and Glue Context ============")

# Set up the spark context and glue context
conf = SparkConf()
conf.set(f"spark.rpc.message.maxSize", "512")

# Create a GlueContext
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# need seperate bookmark for each project to avoid conflict when versioncommitexception
job.init(args["JOB_NAME"] + source_name + function_name + project_id, args)

def retrieveIdsFromProject(glueContext,parquet_path,function_name,project_id,columnname):
    unique_object_ids=[]
    
    logger.info(f"Reading {columnname} for {function_name} in Project from {parquet_path}")
    project_dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [parquet_path]
                                },
                                format="parquet",
                                transformation_ctx = f"{function_name}_project_{project_id}"
                                )

    selected_columns = [f"{columnname}"]

    project_dynamic_frame=project_dynamic_frame.select_fields(selected_columns)
    df = project_dynamic_frame.toDF()
    df.printSchema()
    df.show()
    
    df_rdd=df.rdd
            
    # Extract the unique IDs from the RDD
    
    unique_object_ids = df_rdd.map(lambda row: row[0]).distinct().collect()
    
    if unique_object_ids is None or not unique_object_ids:
        object_ids = None
    else:
        object_ids = ','.join(unique_object_ids)
        unique_object_ids_quoted = [f"'{str(item)}'" for item in unique_object_ids]
        object_ids = ','.join(unique_object_ids_quoted)
    return object_ids

def retrieveSpreadIdsFromProject(glueContext,parquet_path,function_name,project_id,columnname):
    print(parquet_path)
    
    project_dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [parquet_path]
                                },
                                format="parquet",
                                transformation_ctx = f"{function_name}_project_{project_id} : Dynamic Frame"
                                )
    selected_columns = [f"{columnname}"]
    
    project_dynamic_frame=project_dynamic_frame.select_fields(selected_columns)
    df = project_dynamic_frame.toDF()
    df.show()
            
    #convert df into rdd
    df_rdd=df.rdd
            
    # Extract the unique IDs from the RDD
    
    unique_object_ids = df_rdd.map(lambda row: row[0]).distinct().collect()
    unique_object_ids.sort()
    if not unique_object_ids:
        unique_object_ids = []
    return unique_object_ids

def export_api_run(api_type):
    success = False
    if api_type== EXPORT:
        api_call_attempt = 1
        logger.info(p6_export_config)
        http_client = HTTPClient(p6_export_config)
        while (api_call_attempt < api_retry_count):
            response, api_status, api_resp_code = http_client.run()
            if api_resp_code == 200:
                break
            else:
                print(f"API failed. Attempt {api_call_attempt}")
                api_call_attempt=api_call_attempt+1
        #logger.info(f"API Status: {api_status}, API Response Code: {api_resp_code}")
        print(f"API Status: {api_status}, API Response Code: {api_resp_code}")
        if api_resp_code != 200:
            logger.info("Glue job failed as response code is not 200")
            sys.exit(1)
        success = False
        raw_object_name = f"{raw_data_path}/{function_name}.json"
        kms_key_id = p6_export_config['job_parameter']['kms_key_id']
        if response:
            if s3_client.upload_to_s3(bytes(json.dumps(response).encode('UTF-8')),raw_object_name,kms_key_id,is_gzip=False):
                logger.info(f"Uploaded P6 {function_name} info to {bucket_name}/{raw_object_name}")
                success = True
            else:
                logger.error("P6 Export API was successful but the json content could not be uploaded to S3")
                success = False
        else:
            logger.error("Failed to fetch Oracle P6 Export API payload")
            success = False
    elif api_type== SPREAD:
        batch_size=p6_export_config['api_parameter']['batch_size']
        ids=object_ids
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            comma_separated_ids = create_comma_separated_string(batch)
            p6_export_config['api_parameter']['endpoint']=endpoint.format(comma_separated_ids)
            logger.info(p6_export_config['api_parameter']['endpoint'])
            api_call_attempt = 1
            http_client = HTTPClient(p6_export_config)
            print(p6_export_config['api_parameter']['endpoint'])

            while (api_call_attempt < api_retry_count):
                response, api_status, api_resp_code = http_client.run()
                # break
                if api_resp_code == 200:
                    break
                else:
                    print(f"API failed. Attempt {api_call_attempt}")
                    api_call_attempt=api_call_attempt+1
            logger.info(f"API Status: {api_status}, API Response Code: {api_resp_code}")
            if api_resp_code != 200:
                print("Glue job failed as response code is not 200")
                print(response)
                sys.exit(2)
            success = False
            raw_object_name = f"{raw_data_path}/{function_name}_{i}.json"
            kms_key_id = p6_export_config['job_parameter']['kms_key_id']
            if response:
                if s3_client.upload_to_s3(bytes(json.dumps(response).encode('UTF-8')),raw_object_name,kms_key_id,is_gzip=False):
                    logger.info(f"Uploaded P6 {function_name} info to {bucket_name}/{raw_object_name}")
                    success = True
                else:
                    logger.error("P6 Export API was successful but the json content could not be uploaded to S3")
                    success = False
            else:
                logger.error("Failed to fetch Oracle P6 Export API payload")
                success = False
    return success

try:
    input_keys = "api#" + source_name + "#" + function_name

    SPREAD = 'spread'
    EXPORT = 'export'

    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    logger.info(source_name)
    logger.info(input_keys)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=input_keys
    )
    p6_export_config = metadata

    #############  Load Oracle P6 Config ################
    bucket_name = p6_export_config['job_parameter']['bucket_name']
    bucket_data_source_prefix = p6_export_config['job_parameter']['bucket_data_source_prefix']
    temp_path = p6_export_config['job_parameter']['temp_path']
    #raw_data_path = bucket_data_source_prefix + temp_path + project_id_partition
    #parquet_data_path = bucket_data_source_prefix + project_id_partition
    global_parquet_path = bucket_data_source_prefix + 'project'
    if project_id_partition:
        project_parquet_data_path = global_parquet_path + '/' + project_id_partition
    else:
        project_parquet_data_path = global_parquet_path
    
    sampling_fraction = float(p6_export_config['job_parameter']['sampling_fraction'])
    sampling_seed = p6_export_config['job_parameter']['sampling_seed']
    region = p6_export_config['aws_region']
    api_retry_count=p6_export_config['api_parameter']['api_retry']
    Fields=p6_export_config['api_parameter']['Fields']
    raw_project_id_partition = ""
    if project_id_partition:
        raw_project_id_partition = f"{project_id_partition}/"
    raw_data_path = f"{bucket_data_source_prefix}{temp_path}{raw_project_id_partition}{function_name}"
    extract_type = p6_export_config['api_parameter']['extract_type']

    secret_param_key = json.loads(get_secret(p6_export_config['auth_api_parameter']['secret_key'], region))
    p6_export_config['auth_api_parameter']['auth_headers']['AuthToken'] = secret_param_key.get("oracle_p6_oauth_secret")

    object_path = p6_export_config['job_parameter'].get('object_path', '') or ''
    object_column = p6_export_config['job_parameter'].get('object_column', None)
    api_project_filters = p6_export_config['api_parameter'].get('api_project_filters', None)
    api_global_filters = p6_export_config['api_parameter'].get('api_global_filters', None)
    api_type = int(p6_export_config['api_parameter']['api_type'])

    if extract_type == SPREAD:
        IncludeCumulative=p6_export_config['api_parameter']['IncludeCumulative']
        PeriodType=p6_export_config['api_parameter']['PeriodType']

    s3_client = S3(bucket_name, region)

    if len(object_path) > 0:
        object_parquet_data_path = bucket_data_source_prefix + object_path
        if project_id_partition:
            object_parquet_data_path = object_parquet_data_path + project_id_partition
    else:
        object_parquet_data_path = bucket_data_source_prefix + 'project'
        if project_id_partition:
            object_parquet_data_path = object_parquet_data_path + '/' + project_id_partition

    base_end_point = f"{p6_export_config['api_parameter']['endpoint']}{function_name}"

except Exception as e:
    logger.error("Failed to set Parameters")
    logger.error(traceback.format_exc(e))
    raise e

try:
    is_ready_api = False
    if api_type == 1:
        endpoint = base_end_point+f"?Fields={Fields}"
        parquet_data_folder = f'{function_name}/'
        is_ready_api = True
    elif api_type == 2:
        if data_type == 'project':
            parquet_data_folder = f'project_{function_name}/'
            object_ids = retrieveIdsFromProject(glueContext,f"s3://{bucket_name}/{object_parquet_data_path}/",function_name,project_id,object_column)
            endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(object_ids)
            if not object_ids:
                is_ready_api = False
            else:
                is_ready_api = True
        else:
            parquet_data_folder = f'{function_name}/'
            endpoint = base_end_point+f"?Fields={Fields}" + api_global_filters
            is_ready_api = True
    elif api_type == 3:
        parquet_data_folder = f'{function_name}/'
        endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(project_id)
        is_ready_api = True
    elif api_type == 4:
        parquet_data_folder = f'{function_name}/'
        object_ids = retrieveIdsFromProject(glueContext,f"s3://{bucket_name}/{object_parquet_data_path}/",function_name,project_id,object_column)
        endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(object_ids)
        if not object_ids:
            is_ready_api = False
        else:
            is_ready_api = True
    elif api_type == 5:
        parquet_data_folder = f'project_{function_name}/'
        endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(project_id)
        is_ready_api = True
    elif api_type == 6:
        parquet_data_folder = f'project_{function_name}/'
        object_ids = retrieveIdsFromProject(glueContext,f"s3://{bucket_name}/{object_parquet_data_path}/",function_name,project_id,object_column)
        endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(object_ids)
        if not object_ids:
            is_ready_api = False
        else:
            is_ready_api = True
    elif api_type == 7:
        parquet_data_folder = f'project_{function_name}/'
        object_ids = retrieveIdsFromProject(glueContext,f"s3://{bucket_name}/{object_parquet_data_path}/",function_name,project_id,object_column)
        endpoint = base_end_point+f"?Fields={Fields}" + api_project_filters.format(object_ids, object_ids)
        if not object_ids:
            is_ready_api = False
        else:
            is_ready_api = True                                                                                               
    else:
        parquet_data_folder = f'project_{function_name}/'
        endpoint = base_end_point + api_project_filters.format(Fields, PeriodType, IncludeCumulative, {})
        object_ids = retrieveSpreadIdsFromProject(glueContext,f"s3://{bucket_name}/{object_parquet_data_path}/",function_name,project_id,object_column)
        if not object_ids:
            is_ready_api = False
        else:
            is_ready_api = True
    if not is_ready_api:
        logger.info(f"No api projectsids to retrieve for {function_name} and project - {project_id} so stopping here and exiting gracefully..")
        job.commit()
        os._exit(0)                                      

    p6_export_config['api_parameter']['endpoint'] = endpoint
    parquet_data_path = bucket_data_source_prefix + parquet_data_folder
    if project_id_partition:
        parquet_data_path = parquet_data_path + project_id_partition
        
    # parquet_data_path = bucket_data_source_prefix + parquet_data_folder + project_id_partition
    if parquet_data_folder == '':
        sample_data_path = p6_export_config['job_parameter']['schema_output_s3']+ function_name
    else:
        sample_data_path = p6_export_config['job_parameter']['schema_output_s3']+ parquet_data_folder
    success = export_api_run(extract_type)

    if success:
        logger.info(f"s3://{bucket_name}/{raw_data_path}/")    
        
        dyn_frame = glueContext.create_dynamic_frame.from_options(
                        connection_type="s3",
                        connection_options={
                            "paths": [
                                f"s3://{bucket_name}/{raw_data_path}/"
                                ],
                                "recurse": True,
                                'groupFiles': 'inPartition', 'groupSize': '542048576'
                        },
                        format="json",
                        format_options={"jsonPath": "$[*]","multiline":True},
                        transformation_ctx=f"{function_name}_{project_id} : Dynamic Frame",
            
                    )

        # Convert DynamicFrame to DataFrame
        df_spark = dyn_frame.toDF()
  
        df_spark.printSchema()
    
        # Add project column to DataFrame only if it's related to project data
        if data_type == 'project':
            df_spark = df_spark.withColumn("project_id", lit(project_id))

        if function_name == 'resourceAssignmentSpread':
            
            if df_spark.select('Period').schema.fields[0].dataType.elementType == t.NullType():
                df_spark = df_spark.withColumn("Period", coalesce(col("Period"), array(struct())))
            
            period_cols = df_spark.select(explode_outer('Period').alias('Period')).select('Period.*').columns
            df_spark = (
                df_spark.withColumn("exploded_period", explode_outer("Period"))
                .select(
                    "ResourceAssignmentObjectId",
                    "StartDate",
                    "EndDate",
                    "PeriodType",
                    "project_id",
                    *[col(f"exploded_period.{field_name}").alias(f"Period_{field_name}") 
                    for field_name in period_cols]
                )
            )
            df_spark.printSchema()
        elif function_name == 'activitySpread':

            if df_spark.select('Period').schema.fields[0].dataType.elementType == t.NullType():
                df_spark = df_spark.withColumn("Period", coalesce(col("Period"), array(struct())))

            period_cols = df_spark.select(explode_outer('Period').alias('Period')).select('Period.*').columns
            df_spark = (
                df_spark.withColumn("exploded_period", explode_outer("Period"))
                .select(
                    "ActivityId",
                    "ActivityObjectId",
                    "StartDate",
                    "EndDate",
                    "PeriodType",
                    "project_id",
                    *[col(f"exploded_period.{field_name}").alias(f"Period_{field_name}") 
                    for field_name in period_cols]
                )
            )
            df_spark.printSchema()
        
        #checking df count
        df_spark = df_spark.persist()
        record_count = df_spark.count()
        logger.info(f"record count for {function_name} : {record_count}")
        
        # Convert back to DynamicFrame
        dyn_frame = DynamicFrame.fromDF(df_spark, glueContext, "dyn_frame")
        
        logger.info(f"Uploading {function_name} data for ProjectId = {project_id} to {parquet_data_path}")

        parquet_data_path=parquet_data_path + '/' + get_partition_str_mi(partition_date)
        success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                dyn_frame,
                bucket_name,
                parquet_data_path,
                function_name,
                typecast_cols_to_string = True
            )
        print(f"success -> {success}")
        
        sample_s3_key = sample_data_path

        logger.info(f"Sampling Path : {sample_s3_key}")

        # Write sample data to S3 without partitioning
        sample_data = (df_spark.sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed))
        sample_data_dynf = DynamicFrame.fromDF(sample_data, glueContext, "sample dynamic_frame").coalesce(1)

        write_glue_df_to_s3_with_specific_file_name(glueContext, sample_data_dynf, bucket_name, sample_s3_key, function_name,typecast_cols_to_string = True)
        
        logger.info(f"Deleting folder path {raw_data_path}")
        s3_client.delete_folder_from_s3(raw_data_path)

    job.commit()
except Exception as e:
    logger.error(f"Script failed unexpectedly with error", e)
    logger.error(traceback.format_exc(e))
    logger.info(f"Deleting folder path {raw_data_path}")
    s3_client.delete_folder_from_s3(raw_data_path)
    raise e