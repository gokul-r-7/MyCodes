import sys
import json
import base64
import os
import boto3
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
import xml.etree.ElementTree as ET
from pyspark.sql.functions import lit
from pyspark.sql.functions import flatten, explode, col, when, coalesce, struct, to_json, explode_outer, collect_list
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import ArrayType, StructType, StructField
from awsglue.gluetypes import StructType, ArrayType, BooleanType, StringType


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

# Init the start date
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
    sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name","endpoint_host","ProjectId"])

source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
endpoint_host = args["endpoint_host"]
ProjectId = args["ProjectId"]
print("ProjectId --> " + ProjectId)

project_id_partition = f"Project={ProjectId}"

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)
    
logger.info(f" Metadata Response :{metadata}")

aconex_docregister_export_config = metadata
logger.info(f" Aconex Document Register Export Config :{aconex_docregister_export_config}")

#############  Load Aconex Document Register Export API Config ################
bucket_name = aconex_docregister_export_config['job_parameter']['bucket_name']
raw_data_location = aconex_docregister_export_config['job_parameter']['input_path']
raw_data_location=f"{raw_data_location}/{str(ProjectId)}/raw_data"
relationalized_data_path = aconex_docregister_export_config['job_parameter']['input_path']
relationalized_data_path = f"{relationalized_data_path}/{str(ProjectId)}/relationalized_data"
parquet_data_path = aconex_docregister_export_config['job_parameter']['output_s3']
root_name = aconex_docregister_export_config['job_parameter']['root_tag']
region = aconex_docregister_export_config['aws_region']
incremental_default_date = aconex_docregister_export_config['job_parameter']['incremental_default_date']
page_size=aconex_docregister_export_config['page_size']
incremental_criteria_folder_location = aconex_docregister_export_config['job_parameter']['incremental_criteria_folder_location']
endpoint_suffix=aconex_docregister_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_docregister_export_config['api_parameter']['endpoint_prefix']
aconex_docregister_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint_url=aconex_docregister_export_config['api_parameter']['endpoint'] 
sample_data_path = aconex_docregister_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_docregister_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_docregister_export_config['job_parameter']['sampling_seed']
allAttributes = aconex_docregister_export_config['job_parameter']['allAttributes']
sources = aconex_docregister_export_config['job_parameter']['sources']
full_incr = aconex_docregister_export_config['job_parameter']['full_incr']
api_retry_count = aconex_docregister_export_config['api_parameter']['api_retry']





input_temp_path=f"s3://{bucket_name}/{raw_data_location}"

auth_type = aconex_docregister_export_config['api_parameter']['auth_type']

#############  Generate Dynamic Config for Aconex Document Register Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_docregister_export_config['api_parameter']['secret_key'], region))

Username = secret_param_key.get("username")
Password = secret_param_key.get("password")

authData = f"{Username}:{Password}"
base64AuthData = base64.b64encode(authData.encode()).decode()



api_timeout = "1000"
timeout=int(api_timeout)
authHeaders_force = {
    "Authorization": f"Basic {base64AuthData}",
    "Content-Type": "application/xml",
}



aconex_docregister_export_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"

print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("aconex_docregister_export_config -> " + str(aconex_docregister_export_config))
print("incremental_default_date -> " + str(incremental_default_date))
print("incremental_criteria_folder_location -> " + str(incremental_criteria_folder_location))

#######################################################
def resolve_attribute_type_names(attribute_struct):
    attribute_type_names = attribute_struct.getField("AttributeTypeNames")
    if isinstance(attribute_type_names.dataType, ArrayType):
        return attribute_type_names.getField("AttributeTypeName").cast("array<string>")
    else:
        return attribute_type_names.alias("attribute_type_name")

def process_attribute(df, attribute_name, attribute_type):
    attribute_struct = struct(
        df[attribute_name].getField("AttributeType").alias("AttributeType"),
        df[attribute_name].getField("AttributeTypeNames").alias("AttributeTypeNames")
    )
    df = df.withColumn(
        f"{attribute_name.lower()}_attributetypenames",
        when(
            attribute_struct["AttributeType"] == attribute_type,
            to_json(resolve_attribute_type_names(attribute_struct))
        ).otherwise(lit(None))
    )
    df = df.withColumn(
        f"{attribute_name.lower()}_attributetype",
        when(
            attribute_struct["AttributeType"] == attribute_type,
            attribute_struct["AttributeType"]
        ).otherwise(lit(None))
    )
    return df
#########################################################

success=False
kms_key_id = aconex_docregister_export_config['job_parameter']['kms_key_id']

limit=3
identifiers_str = ""
cst_attributes = ""
try:

    if ProjectId == ProjectId:
        counter = 1
        custom_identifiers = []
        unique_custom_str = ""
        folder_name=f"{incremental_criteria_folder_location}"
        file_name=f"{source_name}_{function_name}_{str(ProjectId)}.txt"
        content,status=s3_client.read_s3_file(folder_name,file_name)
        
        print(status)
        if status:
            incremental_date_project_file=content.split(',')[3]
            incremental_from_date=incremental_date_project_file
            print("incremental_date_project_file --> " + incremental_date_project_file)
        else:
            incremental_from_date=incremental_default_date
        
        print("incremental_from_date --> " + incremental_from_date)

        
        schema_endpoint_project_url = f"{endpoint_url}/{str(ProjectId)}/register/schema"
        print("schema_endpoint_project_url --> " + schema_endpoint_project_url)
        aconex_docregister_export_config['api_parameter']['endpoint']=f"{schema_endpoint_project_url}"
        http_client = HTTPClient(aconex_docregister_export_config)
        api_call_attempt=1
        while (api_call_attempt < api_retry_count):
            schema_project_xml_response, api_status, status_code = http_client.run()
            if status_code == 200:
                break
            else:
                print(f"API Attempt {api_call_attempt} failed. Retrying")
                api_call_attempt=api_call_attempt+1

        schema_obj_name = f"{raw_data_location}/schema/schema.xml"

        if "403" in str(status_code):
            print("Skipping the loop for schema_endpoint_project_url --> " + schema_endpoint_project_url)
            logger.info("Skipping the loop for schema_endpoint_project_url --> " + schema_endpoint_project_url)
            os._exit(0)  # Skip the current iteration and continue with the next one

        else: 
            print("Processing the loop for schema_endpoint_project_url --> " + schema_endpoint_project_url)
            logger.info("Processing the loop for schema_endpoint_project_url --> " + schema_endpoint_project_url)
            if s3_client.upload_to_s3(schema_project_xml_response,schema_obj_name,kms_key_id,is_gzip=False):
                logger.info(f"Uploaded Aconex Project info to {bucket_name}/{schema_obj_name}")
                success = True
            else:
                logger.error("Aconex DocumentRegister Standard Export API was successful but the gzip content could not be uploaded to S3")
                success = False
            root = ET.fromstring(schema_project_xml_response)
            links = root.findall('.//Link')

            if links:
              # Iterate over the Link elements and extract the href attribute
              for link in links:
                  href = link.get('href')
                  print(href)
                  customapi = f"https://{endpoint_host}{href}"
                  print(customapi)
                  aconex_docregister_export_config['api_parameter']['api_headers']['accept'] = f"application/vnd.aconex.document.v2+xml"
                  aconex_docregister_export_config['api_parameter']['endpoint']=f"{customapi}"
                  http_client = HTTPClient(aconex_docregister_export_config)
                  api_call_attempt=1
                  while (api_call_attempt < api_retry_count):
                    custom_xml_response, api_status, status_code = http_client.run()
                    if status_code == 200:
                        break
                    else:
                        print(f"API Attempt {api_call_attempt} failed. Retrying")
                        api_call_attempt=api_call_attempt+1

                  # Extracting the custom attributes

                  root2 = ET.fromstring(custom_xml_response)
                 
                  for project_field in root2.findall("ProjectField"):
                      identifier = project_field.get("identifier")
                      custom_identifiers.append(identifier)
                      
                  identifiers_str = ','.join(custom_identifiers)
                  print("Custom Identifiers for project( ", ProjectId, " ) are:", identifiers_str)
                    
            else:
              print("No Link elements with href attributes found in the XML data.")
              pass

            if identifiers_str is not None:
              word_list = identifiers_str.split(",")
              unique_words = set(word_list)
              unique_custom_list = list(unique_words)
              # Join the list back into a string
              unique_custom_str = ",".join(unique_custom_list)
              print("Unique attributes are: ", unique_custom_str)
              cst_attributes = unique_custom_str
            else:
              pass

            incremental_to_date = generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)
            print("incremental_to_date --> " + incremental_to_date)
            if unique_custom_str is not None and unique_custom_str != "":
              print("Custom attributes present")
              if full_incr.upper() == "I".upper():
                docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes},{unique_custom_str}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true&search_query=registered:[{str(incremental_from_date)} TO {str(incremental_to_date)}]"
                print("cst_docregister_project_url --> " + docregister_project_url)
              else:
                docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes},{unique_custom_str}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true"
                print("cst_docregister_project_url --> " + docregister_project_url)
            else:
              print("Only standard attributes present")
              if full_incr.upper() == "I".upper():
                docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true&search_query=registered:[{str(incremental_from_date)} TO {str(incremental_to_date)}]"
                print("cst_docregister_project_url --> " + docregister_project_url)
              else:
                docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true"
                print("cst_docregister_project_url --> " + docregister_project_url)
                

            aconex_docregister_export_config['api_parameter']['endpoint']=f"{docregister_project_url}"
            aconex_docregister_export_config['api_parameter']['api_headers'] = authHeaders_force
            http_client = HTTPClient(aconex_docregister_export_config)
            api_call_attempt=1
            while (api_call_attempt < api_retry_count):
                doc_page_project_xml_response, api_status, status_code = http_client.run()
                if status_code == 200:
                    break
                else:
                    print(f"API Attempt {api_call_attempt} failed. Retrying")
                    api_call_attempt=api_call_attempt+1

            if "403" in str(status_code):
                project_api_error=f"{str(ProjectId)},{docregister_project_url},{function_name},{incremental_to_date}"  
                print("project_api_error --> " + project_api_error)
                if project_api_error:
                    docuregister_project_error_obj=f"{incremental_criteria_folder_location}/{source_name}_{function_name}_{str(ProjectId)}.txt"
                    if s3_client.upload_to_s3(project_api_error,docuregister_project_error_obj,kms_key_id,is_gzip=False):
                        logger.info(f"Uploaded Aconex Project error info to {bucket_name}")
                        success = True
                    else:
                        logger.error("Failed to upload Aconex Project error info")
                        success = False
                print(f"Skipping the loop (API not enabled for Project [{ProjectId}])for docregister_project_url --> " + docregister_project_url)
                logger.info("Skipping the loop for docregister_project_url --> " + docregister_project_url)
                os._exit(0)  # Exiting

            
            else:
                root3 = ET.fromstring(doc_page_project_xml_response)
                total_pages_element = root3.get("TotalPages")
                total_results = root3.get("TotalResults")
                print("total_pages_element --> " + str(ProjectId) +
                              "-->" + str(total_pages_element)) 
                print("total_results --> " + str(ProjectId) +
                              "-->" + str(total_results))                             
                if int(total_results) == 0:
                 print(f"No results for document register api {docregister_project_url}")    
                 job.commit()
                 os._exit(0)

                
                else:

                  print("total_pages_element --> " + str(ProjectId) +
                              "-->" + str(total_pages_element))
                  
                  try:
                      while counter <= int(total_pages_element):
                                                
                          if unique_custom_str is not None and unique_custom_str != "":
                            print("Looping pages with Custom attributes present")
                            if full_incr.upper() == "I".upper():
                              docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes},{unique_custom_str}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true&search_query=registered:[{str(incremental_from_date)} TO {str(incremental_to_date)}]"
                              print("docregister_project_url --> " + docregister_project_url)
                            else:
                              docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes},{unique_custom_str}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true"
                              print("docregister_project_url --> " + docregister_project_url)
                          else:
                            print("Looping pages with only standard attributes present")
                            if full_incr.upper() == "I".upper():
                              docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true&search_query=registered:[{str(incremental_from_date)} TO {str(incremental_to_date)}]"
                              print("docregister_project_url --> " + docregister_project_url)
                            else:
                              docregister_project_url = f"{endpoint_url}/{str(ProjectId)}/register?return_fields={allAttributes}&search_type=paged&page_size={str(page_size)}&page_number={str(counter)}&sort_field=docno&sort_direction=ASC&show_document_history=true"
                              print("docregister_project_url --> " + docregister_project_url)

                          
                          std_object_name = f"{raw_data_location}/docregister_response/{str(ProjectId)}_page_{str(counter)}_{incremental_to_date}.xml"
                          print("std_object_name --> " + std_object_name)
                          aconex_docregister_export_config['api_parameter']['endpoint']=f"{docregister_project_url}"
                          http_client = HTTPClient(aconex_docregister_export_config)
                          api_call_attempt=1
                          while (api_call_attempt < api_retry_count):
                            docregister_project_url, api_status, status_code = http_client.run()
                            if status_code == 200:
                                break
                            else:
                                print(f"API Attempt {api_call_attempt} failed. Retrying")
                                api_call_attempt=api_call_attempt+1

                          if docregister_project_url:
                              if s3_client.upload_to_s3(docregister_project_url,std_object_name,kms_key_id,is_gzip=False):
                                  logger.info(f"Uploaded Aconex Project info to {bucket_name}/{std_object_name}")
                                  success = True
                              else:
                                  logger.error("Aconex DocumentRegister Standard Export API was successful but the gzip content could not be uploaded to S3")
                                  success = False
                          else:
                              logger.error("Failed to fetch Aconex DocumentRegister Standard Export API payload")
                              success = False                            
                              
                          counter += 1
                          
                      print("Loop Finished")
                  except Exception as e:
                      print("Error --> " + str(e))
                      sys.exit(1)  # Exit with failure code
except Exception as e:
       print("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code
       
        
try:                
    if success:
        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
        
        for source_key, source_value in sources.items():        # Load whole XML file into a DynamicFrame
            if source_key.upper() == "docregister_schema".upper():
                print(f"The df location loop right now is -----> {input_temp_path}/schema")
                root_df = glueContext.create_dynamic_frame.from_options(
                            connection_type="s3",
                            connection_options={
                                "paths": [
                        f"{input_temp_path}/schema/"
                                    ],
                                    "recurse": True
                            },
                            format="xml",
                            format_options={"rowTag": source_value},
                            transformation_ctx=f"schema",
                        )
                
                
                root_df.printSchema()
                root_df_df = root_df.toDF()

                #Join root_df and array_mapped_df
                multi_field_df = root_df_df.select(
                    explode("EntityCreationSchemaFields.MultiValueSchemaField").alias("multi_field")
                )
                
                # Explode EntityCreationSchemaFields.SingleValueSchemaField
                single_field_df = root_df_df.select(
                    explode("EntityCreationSchemaFields.SingleValueSchemaField").alias("single_field")
                )
                
                # Explode SearchSchemaFields.MultiValueSchemaField
                search_multi_field_df = root_df_df.select(
                    explode("SearchSchemaFields.MultiValueSchemaField").alias("search_multi_field")
                )
                
                # Explode SearchSchemaFields.SingleValueSchemaField
                search_single_field_df = root_df_df.select(
                    explode("SearchSchemaFields.SingleValueSchemaField").alias("search_single_field")
                )
                
                
                # Extract identifiers and modified field names from EntityCreationSchemaFields
                multi_field_has_modified = "multi_field.ModifiedFieldName" in multi_field_df.columns
                single_field_has_modified = "single_field.ModifiedFieldName" in single_field_df.columns
                
                # Select columns from multi_field_df
                if multi_field_has_modified:
                    multi_field_selected = multi_field_df.select(
                    col("multi_field.Identifier").alias("Identifier"),
                    col("multi_field.ModifiedFieldName").alias("ModifiedFieldName")
                    )
                else:
                    multi_field_selected = multi_field_df.select(
                    col("multi_field.Identifier").alias("Identifier"),
                    lit(None).alias("ModifiedFieldName")
                    )
                
                # Select columns from single_field_df
                if single_field_has_modified:
                    single_field_selected = single_field_df.select(
                    col("single_field.Identifier").alias("Identifier"),
                    col("single_field.ModifiedFieldName").alias("ModifiedFieldName")
                    )
                else:
                    single_field_selected = single_field_df.select(
                    col("single_field.Identifier").alias("Identifier"),
                    lit(None).alias("ModifiedFieldName")
                    )
                    
                    
                entity_creation_fields = multi_field_selected.select(
                    col("Identifier"),
                    col("ModifiedFieldName")
                ).union(
                    single_field_selected.select(
                        col("Identifier"),
                        col("ModifiedFieldName")
                    )
                )


                
                # Extract identifiers and modified field names from SearchSchemaFields
                search_multi_field_has_modified = "search_multi_field.ModifiedFieldName" in search_multi_field_df.columns
                search_single_field_has_modified = "search_single_field.ModifiedFieldName" in search_single_field_df.columns

                # Select columns from multi_field_df
                if search_multi_field_has_modified:
                    search_multi_field_selected = search_multi_field_df.select(
                    col("search_multi_field.Identifier").alias("Identifier"),
                    col("search_multi_field.ModifiedFieldName").alias("ModifiedFieldName")
                    )
                else:
                    search_multi_field_selected = search_multi_field_df.select(
                    col("search_multi_field.Identifier").alias("Identifier"),
                    lit(None).alias("ModifiedFieldName")
                    )
					
                # Select columns from single_field_df
                if search_single_field_has_modified:
                    search_single_field_selected = search_single_field_df.select(
                    col("search_single_field.Identifier").alias("Identifier"),
                    col("search_single_field.ModifiedFieldName").alias("ModifiedFieldName")
                    )
                else:
                    search_single_field_selected = search_single_field_df.select(
                    col("search_single_field.Identifier").alias("Identifier"),
                    lit(None).alias("ModifiedFieldName")
                    )
                
                search_fields = search_multi_field_selected.select(
                    col("Identifier"),
                    col("ModifiedFieldName")
                ).union(
                    search_single_field_selected.select(
                        col("Identifier"),
                        col("ModifiedFieldName")
                    )
                )
                
                # Combine the results
                result = entity_creation_fields.union(search_fields).distinct()
                result.show()
                
                selected_columns = result.withColumn("projectid",lit(ProjectId))           
                dynamic_frame = DynamicFrame.fromDF(selected_columns, glueContext, "dynamic_frame")
                
                partition_str = f"{get_partition_str_mi(partition_date)}"
                schema_key = f"{parquet_data_path}/docregister_schema/Project={ProjectId}/{partition_str}"    

                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    dynamic_frame,
                    bucket_name,
                    schema_key,
                    "docregister_schema",
                    typecast_cols_to_string = True
                )
                ###########################
            else:
                print(f"The df location loop right now is -----> {input_temp_path}/docregister_response")

                root_dyn = glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": [
                            f"{input_temp_path}/docregister_response/"
                            ],
                            "recurse": True
                    },
                    format="xml",
                    format_options={"rowTag": source_value},
                    transformation_ctx=f"{source_key}",
                )
                

                    
                print(f"schema for docregister response is: --------->")

                logger.info("Dataframe created")




                meta_num_partitions = (
                    200  # Adjust this value based on your data size and cluster resources
                )

                partition_str = f"{get_partition_str_mi(partition_date)}"
                partitioned_s3_key_cst = f"{parquet_data_path}/docregister_custom/Project={ProjectId}/{partition_str}"     
                partitioned_s3_key_std = f"{parquet_data_path}/docregister_standard/Project={ProjectId}/{partition_str}" 
                
                # Get the schema from the DynamicFrame and collect the fields with data types
                fields_info = collect_schema_fields(root_dyn.schema())

                #Schema before conversion to string

                logger.info(f"Schema before type casting all data types to string")
                root_dyn.toDF().printSchema()

                # Print the collected fields information
                #column_names = [field.name for field in root_dyn.schema().fields]
                #for column_name in column_names:
                #        root_dyn = root_dyn.resolveChoice(specs=[(column_name, "cast:string")])
                for field_name, data_type in fields_info:
                    if data_type not in ['StructType', 'ArrayType','NullType']:
                        root_dyn = root_dyn.resolveChoice(specs=[(field_name, 'cast:string')])
                logger.info(f"Schema after type casting all data types to string")     
                root_df1 = root_dyn.toDF()
                root_df1.printSchema()
                root_df = root_df1.withColumn("projectid",lit(ProjectId))
                root_df.printSchema()

                def standardizeArray():
                    from pyspark.sql import DataFrame
                    from awsglue.dynamicframe import DynamicFrame
                    from awsglue.gluetypes import Field,StructType,ArrayType,StringType,IntegerType
                    schema = StructType([ 
                        Field("Attribute1", StructType([
                        Field("AttributeType", StringType()),
                        Field("AttributeTypeNames", StructType([
                        Field("AttributeTypeName", ArrayType(StringType({}), {}))]
                            ))
                        ])),
                        Field("Attribute2", StructType([
                        Field("AttributeType", StringType()),
                        Field("AttributeTypeNames", StructType([
                        Field("AttributeTypeName", ArrayType(StringType({}), {}))]
                            ))
                        ])),
                        Field("Attribute3", StructType([
                        Field("AttributeType", StringType()),
                        Field("AttributeTypeNames", StructType([
                        Field("AttributeTypeName", ArrayType(StringType({}), {}))]
                            ))
                        ])),
                        Field("Attribute4", StructType([
                        Field("AttributeType", StringType()),
                        Field("AttributeTypeNames", StructType([
                        Field("AttributeTypeName", ArrayType(StringType({}), {}))]
                            ))
                        ])),
                        Field("_DocumentId", StringType({}), {})
                        ]
                        )
                    # Use this schema in your create_dynamic_frame call
                    array_mapped_df = glueContext.create_dynamic_frame.from_options(
                        connection_type="s3",
                        connection_options={
                        "paths": [
                            f"{input_temp_path}/docregister_response/"
                        ],
                        "recurse": True
                        },
                        format="xml",
                        format_options={
                            "rowTag": source_value,
                            "withSchema": json.dumps(schema.jsonValue())
                        },
                        transformation_ctx=f"{source_key}",
                        )   
                    #array_mapped_df=array_mapped_df.resolveChoice(specs=[("Attribute1.AttributeTypeNames.AttributeTypeName", "cast:string")]).resolveChoice(
                    #    specs=[("Attribute2.AttributeTypeNames.AttributeTypeName", "cast:string")])
                    return array_mapped_df.toDF()
                
                array_df=standardizeArray()
                print("Schema of array converted dataframe")
                array_df.printSchema()

                # Drop columns Attribute1, Attribute2,Attribute3 and Attribute4
                root_df=root_df.drop("Attribute1","Attribute2","Attribute3","Attribute4")
                print(f"Join Stndard DF and Array DF")
                root_df=root_df.join(array_df,on="_DocumentId", how="left")
                print("Updated DF after joining Standard and Array DF")
                root_df.printSchema()

                if cst_attributes is not None and cst_attributes != "":
                    print("Separating custom and standard attributes")
                    cst_key_attributes = f"{cst_attributes},_DocumentId,Revision,VersionNumber,DocumentNumber,DateModified,projectid"
                    existing_cols = set(root_df.columns)
                    print(f"existing cols are: {existing_cols}")
                    selected_cols = [col for col in cst_key_attributes.split(',') if col in existing_cols]
                    print(f"selected cols are {selected_cols}")

                    custom_df = root_df.select(selected_cols)
                    excluded_cols = ['_DocumentId', 'Revision', 'VersionNumber', 'DocumentNumber', 'DateModified', 'projectid']

                    # Filter out the excluded columns
                    cols_to_merge = [col for col in selected_cols if col not in excluded_cols]


                    # Select the desired columns and the custom_Attributes struct
                    custom_df.printSchema()
                    print(f"exluded cols are{excluded_cols}")
                    custom_df2 = custom_df.select(
                        *excluded_cols,
                        to_json(struct(*[col(c) for c in cols_to_merge])).alias('custom_attributes_json')
                    )

                    custom_df2.show()

                    print("-------------")
                    custom_df2.printSchema()
                    print("-------------")


                    cst_cols = [col for col in cst_attributes.split(',') if col in existing_cols]

                    standard_df = root_df.drop(*cst_cols)
                    standard_df.printSchema()

                    columns_to_select = list(standard_df.columns)
                    attributes = [
                        ("Attribute1", "ATTRIBUTE1"),
                        ("Attribute2", "ATTRIBUTE2"),
                        ("Attribute3", "ATTRIBUTE3"),
                        ("Attribute4", "ATTRIBUTE4")
                    ]
                    columns_to_drop = []
                    
                    #Block to dump attribute array data as json 
                    for attribute_name, attribute_type in attributes:
                        if attribute_name in standard_df.columns:
                            df = process_attribute(standard_df, attribute_name, attribute_type)
                            columns_to_select.append(f"{attribute_name.lower()}_attributetypenames")
                            columns_to_select.append(f"{attribute_name.lower()}_attributetype")
                            standard_df = df.select(columns_to_select)
                            columns_to_drop.append(attribute_name)


                    #Removing the duplicate attribute columns
                    for column in set(columns_to_drop):
                        if column in standard_df.columns:
                            standard_df = standard_df.drop(column)

                    #Converting void type columns to string for parquet reading
                    for field in standard_df.schema:
                        col_name = field.name
                        col_type = field.dataType
                        
                        if str(col_type) == "NullType()":
                            standard_df = standard_df.withColumn(col_name, when(col(col_name).isNull(), lit("")).otherwise(col(col_name).cast("string")))
                            print(f"Column '{col_name}' converted from void to string.")
                        else:
                            print(f"Column '{col_name}' is of type {col_type} (skipped).")

                    standard_df.printSchema()
                    df_count = standard_df.count()
                    print("standard_df count --> " + str(df_count))
                    distinct_count = standard_df.select("_DocumentId").distinct().count()
                    print("distinct_count column --> " + str(distinct_count))

                    dynamic_frame_custom = DynamicFrame.fromDF(custom_df2, glueContext, "dynamic_frame")
                    dynamic_frame_standard = DynamicFrame.fromDF(standard_df, glueContext, "dynamic_frame")
                    
                    column_names = [field.name for field in dynamic_frame_standard.schema().fields]

                    for column_name in column_names:
                        dynamic_frame_standard = dynamic_frame_standard.resolveChoice(specs=[(column_name, "cast:string")])
                    
                    column_names_cst = [field.name for field in dynamic_frame_custom.schema().fields]

                    for column_name_cst in column_names_cst:
                        dynamic_frame_custom = dynamic_frame_custom.resolveChoice(specs=[(column_name_cst, "cast:string")])

                    print("The final schema of standard response is")
                    dynamic_frame_standard.printSchema()
                    
                    # Write custom partitioned data to S3
                    success = write_glue_df_to_s3_with_specific_file_name(
                        glueContext,
                        dynamic_frame_custom,
                        bucket_name,
                        partitioned_s3_key_cst,
                        function_name,
                        typecast_cols_to_string = True
                        )
                    
                    # Write standard partitioned data to S3
                    success = write_glue_df_to_s3_with_specific_file_name(
                        glueContext,
                        dynamic_frame_standard,
                        bucket_name,
                        partitioned_s3_key_std,
                        function_name,
                        typecast_cols_to_string = True
                        )
                        
                    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data docregister response")
                    
                    sample_s3_key_cst = "s3://" + bucket_name + "/" + sample_data_path + "/" + "docregister_custom"
                    sample_s3_key_std = "s3://" + bucket_name + "/" + sample_data_path + "/" + "docregister_standard"


                    # Write sample data to S3 without partitioning
                    sample_data_cst = (
                        dynamic_frame_custom
                        .toDF()
                        .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
                    )

                    #sample_data_cst = sample_data_cst.repartition(1)


                    # Write sample data to S3 without partitioning
                    sample_data_std = (
                        dynamic_frame_standard
                        .toDF()
                        .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
                    )  # Adjust the fraction as needed
                    
                    logger.info(f"Selected sample data for standard and custom")

                    #sample_data_std = sample_data_std.repartition(1)
                    
                    # success = write_glue_df_to_s3_with_specific_file_name(
                    #     glueContext,
                    #     DynamicFrame.fromDF(sample_data_cst, glueContext, "sample_data"),
                    #     bucket_name,
                    #     sample_s3_key_cst,
                    #     "docregister_custom"
                    # )

                    save_spark_df_to_s3_with_specific_file_name(
                                    sample_data_cst, sample_s3_key_cst)



                    # success = write_glue_df_to_s3_with_specific_file_name(
                    #     glueContext,
                    #     DynamicFrame.fromDF(sample_data_std, glueContext, "sample_data"),
                    #     bucket_name,
                    #     sample_s3_key_std,
                    #     "docregister_standard"
                    # )

                    save_spark_df_to_s3_with_specific_file_name(
                                    sample_data_std, sample_s3_key_std)

                    
                    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data: standard and custom")      

        ###########################

                else:
                    print("Creating df for standard attributes only")
                    columns_to_select = list(root_df.columns)

                    columns_to_drop = []

                    attributes = [
                        ("Attribute1", "ATTRIBUTE1"),
                        ("Attribute2", "ATTRIBUTE2"),
                        ("Attribute3", "ATTRIBUTE3"),
                        ("Attribute4", "ATTRIBUTE4")
                    ]
                    #Block to dump attribute array data as json 
                    for attribute_name, attribute_type in attributes:
                        if attribute_name in root_df.columns:
                            df = process_attribute(root_df, attribute_name, attribute_type)
                            columns_to_select.append(f"{attribute_name.lower()}_attributetypenames")
                            columns_to_select.append(f"{attribute_name.lower()}_attributetype")
                            root_df = df.select(columns_to_select)
                            columns_to_drop.append(attribute_name)

                    #Removing the duplicate attribute columns
                    for column in set(columns_to_drop):
                        if column in root_df.columns:
                            root_df = root_df.drop(column)

                    root_df.printSchema()


                    #Converting void type columns to string for parquet reading
                    for field in root_df.schema:
                        col_name = field.name
                        col_type = field.dataType
                        
                        if str(col_type) == "NullType()":
                            root_df = root_df.withColumn(col_name, when(col(col_name).isNull(), lit("")).otherwise(col(col_name).cast("string")))
                            print(f"Column '{col_name}' converted from void to string.")
                        else:
                            print(f"Column '{col_name}' is of type {col_type} (skipped).")

                    root_df.printSchema()

                    df_count = root_df.count()
                    print("standard_df count --> " + str(df_count))
                    distinct_count = root_df.select("_DocumentId").distinct().count()
                    print("distinct_count column --> " + str(distinct_count))

                    dynamic_frame_standard = DynamicFrame.fromDF(root_df, glueContext, "dynamic_frame")
                    
                    column_names = [field.name for field in dynamic_frame_standard.schema().fields]

                    for column_name in column_names:
                        dynamic_frame_standard = dynamic_frame_standard.resolveChoice(specs=[(column_name, "cast:string")])

                    print("The final schema of standard response is")
                    dynamic_frame_standard.printSchema()


                    success = write_glue_df_to_s3_with_specific_file_name(
                        glueContext,
                        dynamic_frame_standard,
                        bucket_name,
                        partitioned_s3_key_std,
                        function_name,
                        typecast_cols_to_string = True
                        )
                    
                    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the standard attributes main data ")
                    
                    sample_s3_key_std = "s3://" + bucket_name + "/" + sample_data_path + "/" + "docregister_standard"
                    
                    
                    sample_data_std = (
                        dynamic_frame_standard
                        .toDF()
                        .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
                    )
                    
                    logger.info(f"Selected sample data for standard attribute data")

                    # sample_data_std = sample_data_std.repartition(1)

                    # success = write_glue_df_to_s3_with_specific_file_name(
                    #     glueContext,
                    #     DynamicFrame.fromDF(sample_data_std, glueContext, "sample_data"),
                    #     bucket_name,
                    #     sample_s3_key_std,
                    #     "docregister_standard"
                    # )

                    save_spark_df_to_s3_with_specific_file_name(
                                    sample_data_std, sample_s3_key_std)
                    
                    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data: standard")      
            
                
            
    else:
        logger.error("Failed to fetch Export API data")
        sys.exit(1)  # Exit with failure code

    docuregister_project_audit_object=f"{incremental_criteria_folder_location}/{source_name}_{function_name}_{str(ProjectId)}.txt"
    print("DocReg_project_audit_object --> " + docuregister_project_audit_object)
                
                
    print("audit_date_value --> " + audit_date_value)
                
    project_audit_column=f"{str(ProjectId)},{source_name},{function_name},{audit_date_value}"  
    print("project_audit_column --> " + project_audit_column)

    if project_audit_column:
        if s3_client.upload_to_s3(project_audit_column,docuregister_project_audit_object,kms_key_id,is_gzip=False):
            logger.info(f"Uploaded Aconex Project info to {bucket_name}")
            success = True
        else:
            logger.error("Aconex Document Register API was successful but the audit could not be uploaded to S3")
            success = False
    else:
        logger.error("Failed to fetch Aconex Document Register Audit payload")
        success = False
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

    staging_path = s3_client.get_folder_path_from_s3(relationalized_data_path)
    logger.info(f"Deleting folder path {staging_path}")
    s3_client.delete_folder_from_s3(staging_path)  

    input_temp_path = s3_client.get_folder_path_from_s3(input_temp_path)
    logger.info(f"Deleting folder path {input_temp_path}")
    s3_client.delete_folder_from_s3(input_temp_path) 

    job.commit()


except Exception as e:

    print("Error --> " + str(e))
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    staging_path = s3_client.get_folder_path_from_s3(relationalized_data_path)
    logger.info(f"Delete staging data after relationalizing - {staging_path}")
    s3_client.delete_folder_from_s3(staging_path) 
    logger.info(f"Deleting folder path {input_temp_path}")
    s3_client.delete_folder_from_s3(input_temp_path)  
    sys.exit(1)  # Exit with failure code    
