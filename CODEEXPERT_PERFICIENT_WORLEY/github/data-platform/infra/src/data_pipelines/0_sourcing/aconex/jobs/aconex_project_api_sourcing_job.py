import sys
import json
import base64
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize, Map
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name,save_spark_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
import xml.etree.ElementTree as ET
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType

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
    sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name","endpoint_host","instance_name"]
)

source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
endpoint_host = args["endpoint_host"]
instance_name=args["instance_name"]

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

aconex_project_export_config = metadata
logger.info(f" Aconex Workflow Export Config :{aconex_project_export_config}")

#############  Load Aconex Workflow Export API Config ################
bucket_name = aconex_project_export_config['job_parameter']['bucket_name']
raw_data_location = aconex_project_export_config['job_parameter']['input_path']
relationalized_data_path = aconex_project_export_config['job_parameter']['temp_output_path']
parquet_data_path = aconex_project_export_config['job_parameter']['output_s3']
row_tag = aconex_project_export_config['job_parameter']['row_tag']
root_name = aconex_project_export_config['job_parameter']['root_tag']
region = aconex_project_export_config['aws_region']
incremental_default_date = aconex_project_export_config['job_parameter']['incremental_default_date']
page_size=aconex_project_export_config['page_size']
incremental_criteria_folder_location = aconex_project_export_config['job_parameter']['incremental_criteria_folder_location']
endpoint_suffix=aconex_project_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_project_export_config['api_parameter']['endpoint_prefix']
aconex_project_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint_url=aconex_project_export_config['api_parameter']['endpoint']
sample_data_path = aconex_project_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_project_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_project_export_config['job_parameter']['sampling_seed']
name = aconex_project_export_config['name']
bucket_data_source_prefix = aconex_project_export_config['job_parameter']['bucket_data_source_prefix']
orgFilter=aconex_project_export_config['org_filter']

auth_type = aconex_project_export_config['api_parameter']['auth_type']

#############  Generate Dynamic Config for Aconex Workflow Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_project_export_config['api_parameter']['secret_key'], region))

Username = secret_param_key.get("username")
Password = secret_param_key.get("password")

authData = f"{Username}:{Password}"
base64AuthData = base64.b64encode(authData.encode()).decode()

aconex_project_export_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"
raw_data_location=f"{raw_data_location}{instance_name}/"
relationalized_data_path=f"{relationalized_data_path}{instance_name}/"
print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("row_tag -> " + row_tag)
print("aconex_project_export_config -> " + str(aconex_project_export_config))
print("incremental_default_date -> " + str(incremental_default_date))
print("incremental_criteria_folder_location -> " + str(incremental_criteria_folder_location))
print("endpoint_suffix -> " + str(endpoint_suffix))
print("endpoint_prefix -> " + str(endpoint_prefix))

#######################################################


kms_key_id = aconex_project_export_config['job_parameter']['kms_key_id']
success=False

try:

    http_client = HTTPClient(aconex_project_export_config)
    response, api_status, status_code = http_client.run()
    object_name = f"{raw_data_location}{name}.xml"
    print("object_name --> " + object_name)
    if response:
        if s3_client.upload_to_s3(response,object_name,kms_key_id,is_gzip=False):
            logger.info(f"Uploaded Aconex Project info to {bucket_name}/{object_name}")
            success = True
        else:
            logger.error("Aconex Project Export API was successful but the gzip content could not be uploaded to S3")
            success = False
    else:
        logger.error("Failed to fetch Aconex Project Export API payload")
        success = False

except Exception as e:
       print("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code



if success:

    # Load whole XML file into a DynamicFrame
    root_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [
                f"s3://{bucket_name}/{raw_data_location}"
                ],
                "recurse": True
        },
        format="xml",
        format_options={"rowTag": row_tag},
        transformation_ctx=f"{function_name}",
    )

    logger.info("Dataframe created")

    # Get the schema from the DynamicFrame and collect the fields with data types
    fields_info = collect_schema_fields(root_df.schema())

    #Convert all columns to string data type except struct and array
    for field_name, data_type in fields_info:
        if data_type not in ['StructType', 'ArrayType','NullType']:
            root_df = root_df.resolveChoice(specs=[(field_name, 'cast:string')])
    
    # Add aconex instance name to data
    root_df = Map.apply(frame = root_df, f = AddInstanceNameCol) 

    meta_num_partitions = (
        200  # Adjust this value based on your data size and cluster resources
    )

    repartitioned_df = root_df.repartition(meta_num_partitions)

    logger.info(f"Relationalized data path location is {relationalized_data_path}")

    root_df_rf = Relationalize.apply(
        frame=repartitioned_df,
        staging_path=relationalized_data_path,
        name=f"{root_name}",
        transformation_ctx=f"relationalize__{function_name}",
    )

    logger.info("Relationalize applied")

    partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)

    partition_str = f"{get_partition_str_mi(partition_date)}"

    partitioned_s3_key = (
        parquet_data_path
        + "/"
        + name
        + "/"
        +"Instance="
        + instance_name
        +"/"
        + partition_str
        )

    # Write partitioned data to S3
    success = write_glue_df_to_s3_with_specific_file_name(
        glueContext,
        root_df_rf.select(f"{root_name}"),
        bucket_name,
        partitioned_s3_key,
        function_name
        )

    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {row_tag}")

    sample_s3_key = "s3://" + bucket_name + "/"+ sample_data_path + "/" + name
    
    # Write sample data to S3 without partitioning
    root_df_rf.select(f"{root_name}").show()
    sample_data = (
        root_df_rf.select(f"{root_name}")
        .toDF()
        .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
    )  # Adjust the fraction as needed

    logger.info(f"Selected sample data {source_name}")

    #check for void columns and remove it
    columns_to_drop = [field.name for field in sample_data.schema if str(field.dataType) == "NullType()"]

    # Create a new DataFrame without VoidType columns
    sample_data = sample_data.drop(*columns_to_drop)

    #sample_data = sample_data.repartition(1)

    # success = write_glue_df_to_s3_with_specific_file_name(
    #     glueContext,
    #     DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
    #     bucket_name,
    #     sample_s3_key,
    #     function_name
    # )

    save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)

    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")

    #list the org to be used for filtering worley project
    org_to_be_filtered=[org["orgid"] for org in orgFilter]

    logger.info(f"List of valid Worley owner orgs -> {org_to_be_filtered}")
        
    selected_column_df = root_df_rf.select(f"{root_name}").toDF()
    selected_column_df.printSchema()
    selected_column_df.show(truncate=False)

    selected_column_df=selected_column_df.where(col("ProjectOwnerOrganizationId").isin(org_to_be_filtered))
    
    project_column_selection = selected_column_df.select(col("ProjectId").cast("string").alias("ProjectId")).filter(col("_AccessLevel") != "ARCHIVE").dropDuplicates()
    project_column_selection.printSchema()
    project_column_selection.show(truncate=False)

    project_info_object=f"{bucket_data_source_prefix}/project_ids/{instance_name}"
    print("project_info_object --> " + project_info_object)
    

    s3_path = f"s3://{bucket_name}/{project_info_object}"
    
    deletion_bucket = f"s3://{bucket_name}/{raw_data_location}"

    temp_path_deletion = s3_client.get_folder_path_from_s3(deletion_bucket)
    logger.info(f"Deleting folder path {temp_path_deletion}")
    s3_client.delete_folder_from_s3(temp_path_deletion)

    #Write the DataFrame to S3 in text format
    project_column_selection.coalesce(1).write.mode("overwrite").text(s3_path)
    
else:
    logger.error("Failed to fetch Export API data")
    sys.exit(1)  # Exit with failure code

# Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

temp_path_deletion=f"s3://{bucket_name}/{relationalized_data_path}"

temp_path_deletion = s3_client.get_folder_path_from_s3(temp_path_deletion)
logger.info(f"Deleting folder path {temp_path_deletion}")
s3_client.delete_folder_from_s3(temp_path_deletion)

job.commit()
