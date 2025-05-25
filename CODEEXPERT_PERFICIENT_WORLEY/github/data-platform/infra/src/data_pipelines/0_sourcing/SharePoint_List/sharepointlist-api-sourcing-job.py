import sys
import json
import base64
import boto3
import re
from pyspark.sql.functions import explode, flatten, col

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from worley_helper.utils.logger import get_logger
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.helpers import get_partition_str_mi, standardize_columns, write_glue_df_to_s3_with_specific_file_name

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.sql('set spark.sql.caseSensitive=true')

# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "metadata_table_name"]
)

print(args)

source_name = args.get("source_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]


# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#extract_api"

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

export_config = metadata
logger.info(f" SharePointList Export Config :{export_config}")

############## Load SharePointList Export API Config ################
bucket_name = export_config['job_parameter']['bucket_name']
raw_data_location = export_config['job_parameter']['temp_path']

relationalized_data_path = export_config['job_parameter']['temp_path']

parquet_data_path = export_config['job_parameter']['output_s3']
region = export_config['aws_region']
endpoint = export_config['api_parameter']['endpoint']
titles = export_config['api_parameter']['titles']
sample_data_path = export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(
    export_config['job_parameter']['sampling_fraction'])
sampling_seed = export_config['job_parameter']['sampling_seed']
name = export_config['name']
auth_type = export_config['api_parameter']['auth_type']
temp_deletion_path = export_config['job_parameter']['temp_path']

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
print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("export_config -> " + str(export_config))


success = False

kms_key_id = export_config['job_parameter']['kms_key_id']


try:
   for idx in titles:
    print("idx-->", idx)

    # Initialize pagination
    api_endpoint = f"{endpoint}GetByTitle('{idx}')/items"
    idx=re.sub(r" +", "_", idx)
    print("Endpoint---->",api_endpoint)
 
    all_items = []  # To collect all items from paginated responses
    object_name = f"{raw_data_location}/{idx}/response_data.json"
    # Fetch all items using pagination        
    while api_endpoint:
        export_config['api_parameter']['endpoint'] = api_endpoint
        http_client = HTTPClient(export_config)
        response, api_status, api_resp_code = http_client.run()

        logger.info(f"[{idx}] API Status: {api_status}, Code: {api_resp_code}")
        print(f"[{idx}] Raw response:")
        

        if response is not None:
            if "d" in response:
                logger.info(f"[{idx}] Found 'd' in response. Keys in 'd': {list(response['d'].keys())}")
                if "results" in response["d"]:
                    items = response["d"]["results"]
                    logger.info(f"[{idx}] Retrieved {len(items)} records from this page.")
                    all_items.extend(items)

                    # Pagination
                    api_endpoint = response["d"].get("__next")
                    logger.info(f"[{idx}] Next page link: {api_endpoint}")
                else:
                    logger.warning(f"[{idx}] 'results' not found in response['d']")
                    break
            else:
                logger.warning(f"[{idx}] 'd' not in response.")
                break
        else:
            logger.warning(f"[{idx}] Empty or null response received.")
            break

    # After collecting all items
    print(f"[{idx}] Total collected items: {len(all_items)}")

    # Upload only if we have valid data
    if all_items:
        logger.info(f"[{idx}] Preparing to upload {len(all_items)} items to S3.")
        
        #Write response bytes in NDJSON (newline-delimited JSON) format
        response_bytes = "\n".join(json.dumps(record) for record in all_items).encode("utf-8")


        try:
            if s3_client.upload_to_s3(response_bytes, object_name, kms_key_id, is_gzip=False):
                logger.info(f"[{idx}] Successfully uploaded to S3 at {bucket_name}/{object_name}")
                success = True
            else:
                logger.error(f"[{idx}] Upload to S3 failed.")
                success = False
        except Exception as e:
            logger.error(f"[{idx}] Exception during upload: {str(e)}")
            sys.exit(1)
    else:
        logger.error(f"[{idx}] No data collected — nothing uploaded to S3.")
        success = False
    


    if success:
      # Load whole XML file into a DynamicFrame
        mapped_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    f"s3://{bucket_name}/{object_name}/"
                    ],
                    "recurse": True
                },
                format="json",
                format_options={
                    "multiline": False,},
                )
                
                
        logger.info("DynamicFrame created")  
        mapped_df.printSchema()
        mapped_df.show(5)
        mapped_df=mapped_df.toDF()
        
        #mapped_df = mapped_df.select(explode("record").alias(""))
        
        mapped_df = DynamicFrame.fromDF(mapped_df, glueContext, "dynamic_frame")
        meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
          )
        repartitioned_df = mapped_df.repartition(meta_num_partitions)
        relationalized_data_path = f"{relationalized_data_path}/{idx}/relationalized_data"
        logger.info(f"Relationalized data path location is {relationalized_data_path}")

        root_df_rf_relationalize = Relationalize.apply(
            frame=repartitioned_df,
            staging_path=relationalized_data_path,
            name=f"SharePointList",
            transformation_ctx=f"relationalize__SharePointList"
          )
        logger.info("Relationalize applied")
        
        print(root_df_rf_relationalize)  # This should print the dictionary's contents
       
        print(root_df_rf_relationalize.keys())  # This should print the dictionary's keys
        
        # Convert the relationalized output to individual tables (DynamicFrames)
        tables = {}
        for name_df in root_df_rf_relationalize.keys():
            if name_df != '':
                print("processing the tag --> " + name_df)
                tables[name_df] = root_df_rf_relationalize.select(name_df).toDF()   
        
        print(tables)
            
        for table_name, df in tables.items():
            print(f"Table Name: {table_name}")
            df.printSchema()
        # Access specific tables if needed
        if 'SharePointList' in tables:
            main_df = tables['SharePointList']
        else:
            main_df = None
            
        main_df.printSchema()
        
        for col in main_df.columns:
            main_df = main_df.withColumnRenamed(col, standardize_columns(col))
                
        main_df=main_df.drop("ID")
        main_df.printSchema()
        dynamic_frame = DynamicFrame.fromDF(main_df, glueContext, "dynamic_frame")
        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
        
        partition_str = f"{get_partition_str_mi(partition_date)}"
        partitioned_s3_key = (
            parquet_data_path
            + "/"
            + idx
            +"/"
            + partition_str
            )
   
   
        # Write partitioned data to S3
        success = write_glue_df_to_s3_with_specific_file_name(
          glueContext,
          dynamic_frame,

            bucket_name,
          partitioned_s3_key,
          idx
        )
     

        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data SharePointList {idx}")
     
        sample_s3_key = sample_data_path + "/" + idx + "/"
        #write samle data to s3 without partitioning
     
        sample_data = (
         dynamic_frame
         .toDF()
         .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
         )
         #adjust the fraction as needed
        logger.info(f"Selected sample data {source_name}")
        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
            bucket_name,
            sample_s3_key,
            idx
            )
    
        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data SharePointList {idx}")
        # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of GlueJob
        
        temp_path=f"s3://{bucket_name}/{temp_deletion_path}/{idx}"
        print("temp path ->" + temp_path)
        logger.info(f"Delete staging data after relationalizing - {temp_path}")
        temp_path = s3_client.get_folder_path_from_s3(temp_path)
        logger.info(f"Deleting folder path {temp_path}")
        s3_client.delete_folder_from_s3(temp_path)

    else:
        logger.error("Failed to fetch Export API data")    
   logger.info(f"for loop ended")               
   job.commit()
except Exception as e:
    print("Error --> " + str(e))
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    temp_path=f"s3://{bucket_name}/{temp_deletion_path}"
    print("temp path ->" + temp_path)
    temp_path = s3_client.get_folder_path_from_s3(temp_path)
    logger.info(f"Delete staging data after relationalizing - {temp_path}")
    logger.info(f"Deleting folder path {temp_path}")
    s3_client.delete_folder_from_s3(temp_path)  
    sys.exit(1)  # Exit with failure code  