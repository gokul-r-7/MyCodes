import sys
import json
import base64
import boto3
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize,Map
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, save_spark_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import col, when, lit
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
from awsglue.gluetypes import StructType,ArrayType

#Add instance name to dynamic frame
def AddInstanceNameCol(r):  
    r["projectInstance"] = instance_name
    return r 

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

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name", "endpoint_host","ProjectId","instance_name"]
)

project_id=args.get("ProjectId")
source_name = args.get("source_name")
function_name = args.get("function_name")
instance_name = args.get("instance_name")
metadata_table_name = args.get("metadata_table_name")
endpoint_host=args.get("endpoint_host")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
print("Project ID ->"+ project_id)

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)
    
logger.info(f" Metadata Response :{metadata}")

aconex_export_config = metadata
logger.info(f" Aconex Export Config :{aconex_export_config}")

#############  Load Aconex Export API Config ################
bucket_name = aconex_export_config['job_parameter']['bucket_name']
raw_data_location = aconex_export_config['job_parameter']['input_path']
org_name = aconex_export_config['job_parameter']['org_name']
relationalized_data_path = aconex_export_config['job_parameter']['input_path']
relationalized_data_path = f"{relationalized_data_path}/{project_id}/relationalized_data"

parquet_data_path = aconex_export_config['job_parameter']['output_s3']
row_tag = aconex_export_config['job_parameter']['row_tag']

region = aconex_export_config['aws_region']
endpoint_suffix=aconex_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_export_config['api_parameter']['endpoint_prefix']
aconex_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint = aconex_export_config['api_parameter']['endpoint']
sample_data_path = aconex_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_export_config['job_parameter']['sampling_seed']
auth_type = aconex_export_config['api_parameter']['auth_type']
name=aconex_export_config['name']
temp_deletion_path = aconex_export_config['job_parameter']['input_path']
temp_deletion_path = f"{temp_deletion_path}/{project_id}"

# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_export_config['api_parameter']['secret_key'], region))

username=secret_param_key.get("username")
password=secret_param_key.get("password")

authData = f"{username}:{password}"
base64AuthData = base64.b64encode(authData.encode()).decode()

aconex_export_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"


print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("row_tag -> " + row_tag)
print("aconex_export_config -> " + str(aconex_export_config))
print("endpoint_suffix -> " + str(endpoint_suffix))
print("endpoint_prefix -> " + str(endpoint_prefix))

#######################################################


success = False
kms_key_id = aconex_export_config['job_parameter']['kms_key_id']


try:
    if project_id==project_id:
      endpoint_url = endpoint + "projects/" +project_id+"/directory"
      print("endpoint_url --> " + endpoint_url)
      aconex_export_config['api_parameter']['endpoint']=f"{endpoint_url}"
      http_client = HTTPClient(aconex_export_config)
      User_project_response, api_status,status_code = http_client.run()
      if "403" in str(status_code):
        print("Skipping the loop for endpoint_project_url --> " + endpoint_url)
        logger.info("Skipping the loop for endpoint_project_url --> " + endpoint_url)
        #continue  # Skip the current iteration and continue with the next one
        os._exit(0)
      print(User_project_response)
      if User_project_response:
        object_name = f"{raw_data_location}/{project_id}/data.xml" 
        if s3_client.upload_to_s3(User_project_response,object_name,kms_key_id,is_gzip=True):
          logger.info(f"Uploaded mail response info to {bucket_name}/{object_name}")
          success = True
        else:
          logger.error("Aconex Export API was successful but the gzip content could not be uploaded to S3")
          success = False 
          root=ET.fromstring(User_project_response)
      else:
        logger.error("Failed to fetch response")
        success= False
    else:
      logger.error("ProjectId not matched")    
except Exception as e:
       print("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code
try:
  if success:
    # Load whole XML file into a DynamicFrame
    root_df= glueContext.create_dynamic_frame.from_options(
       connection_type="s3",
       connection_options={
                "paths": [
                    f"s3://{bucket_name}/{raw_data_location}/{project_id}/"
                    ],
                    "recurse": True
                },
                format="xml",
                format_options={"rowTag":row_tag},
                transformation_ctx=f"{function_name}"
                )
    logger.info("DynamicFrame created") 
    # root_df.printSchema()
    root_df= root_df.resolveChoice(specs=[("ProjectFax", "cast:string")]).resolveChoice(specs=[("ProjectPhone", "cast:string")]).resolveChoice(specs=[("Mobile", "cast:string")])           
    # root_df.printSchema()

    # Get the schema from the DynamicFrame and collect the fields with data types
    fields_info = collect_schema_fields(root_df.schema())

    #Convert all columns to string data type except struct and array
    for field_name, data_type in fields_info:
        if data_type not in ['StructType', 'ArrayType','NullType']:
            root_df = root_df.resolveChoice(specs=[(field_name, 'cast:string')])

	  # Add aconex instance name to data
    # root_df = Map.apply(frame = root_df, f = AddInstanceNameCol)

  
    meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
          )
    repartitioned_df = root_df.repartition(meta_num_partitions)
    logger.info(f"Relationalized data path location is {relationalized_data_path}")
    root_df_rf1 = Relationalize.apply(
      frame=repartitioned_df,
      staging_path=relationalized_data_path,
      name=f"{row_tag}",
      transformation_ctx=f"relationalize__{function_name}"
          )
    logger.info("Relationalize applied")
    root_dynamic_frame= root_df_rf1.select(f"{row_tag}")
    spark_df=root_dynamic_frame.toDF()
    spark_df = spark_df.withColumn("projectid",lit(project_id))
    spark_df = spark_df.withColumn("instance_name", lit(instance_name).cast("string"))
    spark_df.printSchema()
    spark_df.show(truncate=False)     
    root_df_rf = DynamicFrame.fromDF(spark_df, glueContext, "root_df_rf")                                        
    partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
    partition_str = f"{get_partition_str_mi(partition_date)}"
    partitioned_s3_key = (
            parquet_data_path
            + "/"
            + name
            +"/"
            +"Instance="
            + instance_name
            +"/"
            +"Project="
            +project_id
            +"/"
            + partition_str
            )
   
   
    # Write partitioned data to S3
    success = write_glue_df_to_s3_with_specific_file_name(
          glueContext,
          root_df_rf,
          bucket_name,
          partitioned_s3_key,
          function_name
        )     
    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {row_tag}")

    sample_s3_key = "s3://" + bucket_name + "/" + sample_data_path + "/" + name

        #write samle data to s3 without partitioning
    sample_data = (
         #root_df_rf.select(f"{root_name}")
      root_df_rf
         .toDF()
         .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
         )
        #adjust the fraction as needed
    logger.info(f"Selected sample data {source_name}")

    # sample_data = sample_data.repartition(1)

    # success = write_glue_df_to_s3_with_specific_file_name(
    #         glueContext,
    #         DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
    #         bucket_name,
    #         sample_s3_key,
    #         function_name
    #         )

    save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)
    
    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")
        # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of GlueJob
    logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

    #temp_path=f"s3://{bucket_name}/{temp_deletion_path}/"
    print("temp path ->" + temp_deletion_path)
    temp_path = s3_client.get_folder_path_from_s3(temp_deletion_path)
    logger.info(f"Deleting folder path {temp_deletion_path}")
    s3_client.delete_folder_from_s3(temp_deletion_path)
  else:
    logger.error("Failed to fetch Export API data")
    sys.exit(1)  # Exit with failure code
      
  job.commit()
except Exception as e:

    print("Error --> " + str(e))
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {temp_deletion_path}")
    logger.info(f"Deleting folder path {temp_deletion_path}")
    s3_client.delete_folder_from_s3(temp_deletion_path)  
    sys.exit(1)  # Exit with failure code  
