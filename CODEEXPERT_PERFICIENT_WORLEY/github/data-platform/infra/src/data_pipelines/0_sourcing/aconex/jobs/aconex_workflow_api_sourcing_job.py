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

      
def apply_dataframe_conditional_logic(
    df: DataFrame,
    id_col: str,
    main_col: str,
    array_col: str,
    new_col: str
) -> DataFrame:
    """
    Apply conditional logic to create a new column based on the presence and values of existing columns.
    
    :param df: The input DataFrame.
    :param id_col: The column name for the 'id' condition.
    :param main_col: The column name for the 'main' value.
    :param array_col: The column name for the 'array' value.
    :param new_col: The name of the new column to create.
    :return: DataFrame with the new column added.
    """
    if main_col in df.columns:
        expr = when(
            col(id_col).isNull(), 
            when(col(main_col).isNotNull(), col(main_col)).otherwise(col(array_col))
        ).otherwise(col(array_col))
    else:
        expr = col(array_col)
    
    return df.withColumn(new_col, expr)


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

aconex_workflow_export_config = metadata
logger.info(f" Aconex Workflow Export Config :{aconex_workflow_export_config}")

#############  Load Aconex Workflow Export API Config ################
bucket_name = aconex_workflow_export_config['job_parameter']['bucket_name']
raw_data_location = aconex_workflow_export_config['job_parameter']['input_path']
raw_data_location = f"{raw_data_location}/{ProjectId}/raw_data"
project_id_partition = f"Project={ProjectId}"
relationalized_data_path = aconex_workflow_export_config['job_parameter']['input_path']
relationalized_data_path = f"{relationalized_data_path}/{ProjectId}/relationalized_data"
parquet_data_path = aconex_workflow_export_config['job_parameter']['output_s3']
row_tag = aconex_workflow_export_config['job_parameter']['row_tag']
root_name = aconex_workflow_export_config['job_parameter']['root_tag']
region = aconex_workflow_export_config['aws_region']
incremental_default_date = aconex_workflow_export_config['job_parameter']['incremental_default_date']
page_size=aconex_workflow_export_config['page_size']
incremental_criteria_folder_location = aconex_workflow_export_config['job_parameter']['incremental_criteria_folder_location']
endpoint_suffix=aconex_workflow_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_workflow_export_config['api_parameter']['endpoint_prefix']
aconex_workflow_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint_url=aconex_workflow_export_config['api_parameter']['endpoint'] 
sample_data_path = aconex_workflow_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_workflow_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_workflow_export_config['job_parameter']['sampling_seed']
name = aconex_workflow_export_config['name']
full_incr = aconex_workflow_export_config['job_parameter']['full_incr']
temp_deletion_path = aconex_workflow_export_config['job_parameter']['input_path']
temp_deletion_path = f"{temp_deletion_path}/{ProjectId}"
api_retry_count = aconex_workflow_export_config['api_parameter']['api_retry']
print("temp_deletion_path " + temp_deletion_path)


auth_type = aconex_workflow_export_config['api_parameter']['auth_type']

#############  Generate Dynamic Config for Aconex Workflow Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_workflow_export_config['api_parameter']['secret_key'], region))

Username = secret_param_key.get("username")
Password = secret_param_key.get("password")

authData = f"{Username}:{Password}"
base64AuthData = base64.b64encode(authData.encode()).decode()

aconex_workflow_export_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"

print("Bucket Name -> " + bucket_name)
print("raw_data_location -> " + raw_data_location)
print("relationalized_data_path -> " + relationalized_data_path)
print("parquet_data_path -> " + parquet_data_path)
print("row_tag -> " + row_tag)
print("root_name -> " + root_name)
print("aconex_workflow_export_config -> " + str(aconex_workflow_export_config))
print("incremental_default_date -> " + str(incremental_default_date))
print("incremental_criteria_folder_location -> " + str(incremental_criteria_folder_location))
print("endpoint_suffix -> " + str(endpoint_suffix))
print("endpoint_prefix -> " + str(endpoint_prefix))
print("full_incr --> " + full_incr)

#######################################################


success=False
kms_key_id = aconex_workflow_export_config['job_parameter']['kms_key_id']

try:
    if ProjectId == ProjectId:
        counter = 1
        folder_name=f"{incremental_criteria_folder_location}"
        file_name=f"{source_name}_{function_name}_{ProjectId}.txt"
        content,status=s3_client.read_s3_file(folder_name,file_name)
        
        print(status)
        if status:
            incremental_date_project_file=content.split(',')[3]
            incremental_date=incremental_date_project_file
            print("incremental_date_project_file --> " + incremental_date_project_file)
        else:
            incremental_date=incremental_default_date
        
        print("incremental_date --> " + incremental_date)
        
        if full_incr.upper() == "I".upper():
            endpoint_project_url = endpoint_url + "/" + ProjectId + "/workflows?page_size=" + str(page_size) + "&page_number=" + str(counter) + "&updated_after=" + incremental_date
        else:
            endpoint_project_url = endpoint_url + "/" + ProjectId + "/workflows?page_size=" + str(page_size) + "&page_number=" + str(counter) 
            
        print("endpoint_project_url --> " + endpoint_project_url)
        aconex_workflow_export_config['api_parameter']['endpoint']=f"{endpoint_project_url}"
        http_client = HTTPClient(aconex_workflow_export_config)
        workflow_project_xml_response, api_status,status_code = http_client.run()
        
        if "403" in str(status_code):
            print("Skipping the loop for endpoint_project_url --> " + endpoint_project_url)
            logger.info("Skipping the loop for endpoint_project_url --> " + endpoint_project_url)
            #continue  # Skip the current iteration and continue with the next one
            job.commit()
            os._exit(0)
        else: 
            print("Processing the loop for endpoint_project_url --> " + endpoint_project_url)
            logger.info("Processing the loop for endpoint_project_url --> " + endpoint_project_url)
            root = ET.fromstring(workflow_project_xml_response)
            total_pages_element = root.get("TotalPages")
            totol_records = root.get("TotalResults")
            print("total_pages_element --> " + ProjectId +
                          "-->" + str(total_pages_element))
            print("totol_records --> " + ProjectId +
                          "-->" + str(totol_records))
                          
            if int(totol_records) == 0:
                print("Total records present for " + ProjectId + " is 0 " + "so exiting successfully, nothing to process.")
                job.commit()
                os._exit(0)
            
            try:
                while counter <= int(total_pages_element):
                    
                    if full_incr.upper() == "I".upper():
                        workflow_paginated_project_url = endpoint_url + "/" + ProjectId + "/workflows?page_size=" + str(page_size) +   "&page_number=" + str(counter) + "&updated_after=" + incremental_date
                    else:
                         workflow_paginated_project_url = endpoint_url + "/" + ProjectId + "/workflows?page_size=" + str(page_size) +   "&page_number=" + str(counter) 
                         
                    print("workflow_paginated_project_url --> " +
                                  workflow_paginated_project_url)
                    
                    object_name = f"{raw_data_location}/{ProjectId}_page_{str(counter)}_{incremental_date}.xml"
                    print("object_name --> " + object_name)
                    aconex_workflow_export_config['api_parameter']['endpoint']=f"{workflow_paginated_project_url}"
                    http_client = HTTPClient(aconex_workflow_export_config)
                    api_call_attempt=1
                    while (api_call_attempt < api_retry_count):
                        workflow_paginated_project_xml_response, api_status,status_code = http_client.run()
                        if status_code == 200:
                            break
                        else:
                            print(f"API Attempt {api_call_attempt} failed. Retrying")
                            api_call_attempt=api_call_attempt+1         
                            counter=counter+1
                    
                    if workflow_paginated_project_xml_response:
                        if s3_client.upload_to_s3(workflow_paginated_project_xml_response,object_name,kms_key_id,is_gzip=False):
                            logger.info(f"Uploaded Aconex Workflow Project info to {bucket_name}/{object_name}")
                            success = True
                        else:
                            logger.error("Aconex Workflow Export API was successful but the gzip content could not be uploaded to S3")
                            success = False
                    else:
                        logger.error("Failed to fetch Oracle Aconex Workflow Export API payload")
                        success = False  
                        
                    counter += 1
                    
                print("Loop Finished")
            except Exception as e:
                print("Error --> " + str(e))
                sys.exit(1)  # Exit with failure code
                
        
except Exception as e:
       print("Error --> " + str(e))
       sys.exit(1)  # Exit with failure code


# Function to check if a column exists in the DataFrame schema
def column_exists(df: DataFrame, column_path: str) -> bool:
    try:
        fields = column_path.split(".")
        schema = df.schema
        for field in fields:
            schema = schema[field].dataType
        return True
    except KeyError:
        return False
    
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

try:
    if success:
        
        # Load whole XML file into a DynamicFrame
        mapped_df_orig = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    f"s3://{bucket_name}/{raw_data_location}/"
                    ],
                    "recurse": True
            },
            format="xml",
            format_options={"rowTag": f"{row_tag}"},
            transformation_ctx=f"{function_name}"
        )
        
        logger.info("Dataframe created")
        
        mapped_df= mapped_df_orig.resolveChoice(specs=[("FileSize", "cast:string")]).resolveChoice(specs=[("DocumentRevision", "cast:string")])

        # Get the schema from the DynamicFrame and collect the fields with data types
        fields_info = collect_schema_fields(mapped_df.schema())

        #Convert all columns to string data type except struct and array
        for field_name, data_type in fields_info:
            if data_type not in ['StructType', 'ArrayType','NullType']:
                mapped_df = mapped_df.resolveChoice(specs=[(field_name, 'cast:string')])
        logger.info(f"Schema after type casting all data types to string")
        
        spark_df = mapped_df.toDF()
        spark_df.printSchema()

        #Standardize Assignees
        def standardizeArray() -> DataFrame:
            from pyspark.sql import DataFrame
            from awsglue.dynamicframe import DynamicFrame
            from awsglue.gluetypes import Field,StructType,ArrayType,StringType,IntegerType
            schema = StructType([ 
                        Field("Assignees", StructType([
                            Field("Assignee",ArrayType(StructType([
                                    Field("Name", StringType()),
                                    Field("OrganizationId", StringType()),
                                    Field("OrganizationName", StringType()),
                                    Field("UserId", StringType())
                                    ])
                                ))
                            ])),
                        Field("_WorkflowId", StringType({}), {})])
            # Use this schema in your create_dynamic_frame call
            array_mapped_df = glueContext.create_dynamic_frame.from_options(
                        connection_type="s3",
                        connection_options={
                        "paths": [
                            f"s3://{bucket_name}/{raw_data_location}/"
                        ],
                        "recurse": True
                        },
                        format="xml",
                        format_options={
                            "rowTag": f"{row_tag}",
                            "withSchema": json.dumps(schema.jsonValue())
                        },
                        transformation_ctx=f"{function_name}"
                        )   
            array_mapped_df=array_mapped_df.resolveChoice(specs=[("Attribute1.AttributeTypeNames.AttributeTypeName", "cast:string")]).resolveChoice(
                         specs=[("Attribute2.AttributeTypeNames.AttributeTypeName", "cast:string")])
            return array_mapped_df.toDF()
                
        array_df=standardizeArray()
        print("Schema of array converted dataframe")
        array_df.printSchema()

        # Drop columns Attribute1, Attribute2,Attribute3 and Attribute4
        spark_df=spark_df.drop("Assignees")
        print(f"Join Stndard DF and Array DF")
        spark_df=spark_df.join(array_df,on="_WorkflowId", how="left")

        print('Updated schema after standardize Assignees')
        spark_df.printSchema()
        
        # Step 2: Check if the column exists before selecting and exploding
        if column_exists(spark_df, "Assignees.Assignee"):
            
            exploded_df = spark_df.select(col("Assignees.Assignee").alias("AssigneeArray"), "_WorkflowId")
            exploded_df = exploded_df.withColumn("ExplodedAssignees", explode(col("AssigneeArray"))).drop("AssigneeArray").drop("Assignees_Assignee")
        else:
            from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
            schema = StructType([
                StructField("_WorkflowId", StringType(), True),
                StructField("ExplodedAssignees", StructType([
                    StructField("Name", StringType(), True),
                    StructField("OrganizationId", IntegerType(), True),
                    StructField("OrganizationName", StringType(), True),
                    StructField("UserId", IntegerType(), True)
                ]), True)
                ])
            exploded_df = spark.createDataFrame([], schema)

        
        exploded_df.printSchema()
        
        # Step 3: Select the required columns to create assignees_df
        assignees_df = exploded_df.select("_WorkflowId","ExplodedAssignees.Name","ExplodedAssignees.OrganizationId","ExplodedAssignees.OrganizationName","ExplodedAssignees.UserId") 
        
        assignees_df.printSchema()
        
        meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
        )
    
        repartitioned_df = mapped_df.repartition(meta_num_partitions)

        logger.info(f"Relationalized data path location is {relationalized_data_path}")
    
        root_df_rf_relationalize = Relationalize.apply(
            frame=repartitioned_df,
            staging_path=relationalized_data_path,
            name=f"{root_name}",
            options={
                "format_options": {
                "rowTag": f"{row_tag}"  # Example of an XML format option
                }
            },
            transformation_ctx=f"relationalize__{function_name}",
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
        if 'Workflow' in tables:
            workflow_df = tables['Workflow']
        else:
            workflow_df = None
        
        
        #workflow_df = rename_columns(workflow_df1)
        
        
        column_name = 'Assignees.Assignee.struct.Name'
        workflow_df.printSchema()
#        if column_name in workflow_df.columns:
        if column_name in workflow_df.columns:
            workflow_df = workflow_df.withColumnRenamed("Assignees.Assignee.struct.Name", "Assignee_main_Name") \
               .withColumnRenamed("Assignees.Assignee.struct.OrganizationName", "Assignee_main_OrganizationName") \
               .withColumnRenamed("Assignees.Assignee.struct.OrganizationId", "Assignee_main_OrganizationId") \
               .withColumnRenamed("Assignees.Assignee.struct.UserId", "Assignee_main_UserId")
        else:
            workflow_df = workflow_df.withColumnRenamed("Assignees.Assignee.Name", "Assignee_main_Name") \
               .withColumnRenamed("Assignees.Assignee.OrganizationName", "Assignee_main_OrganizationName") \
               .withColumnRenamed("Assignees.Assignee.OrganizationId", "Assignee_main_OrganizationId") \
               .withColumnRenamed("Assignees.Assignee.UserId", "Assignee_main_UserId")

         # Columns you want to rename or add with null values if they don't exist
        columns_to_rename = [
            ("Name", "Assignee_array_Name","String"),
            ("OrganizationId", "Assignee_array_OrganizationId","Int"),
            ("OrganizationName", "Assignee_array_OrganizationName","String"),
            ("UserId", "Assignee_array_UserId","Int"),
            ("_WorkflowId","id","long")
        ]
        
        # Iterate through columns and handle missing columns
        for src_col, dest_col, datatype in columns_to_rename:
            if src_col in assignees_df.columns:
                assignees_df = assignees_df.withColumnRenamed(src_col, dest_col)
            else:
                assignees_df = assignees_df.withColumn(dest_col, lit(None))
        
        # Perform the join on the root_id field
        combined_df = workflow_df.join(assignees_df, workflow_df._WorkflowId == assignees_df.id,"left")
        combined_df.printSchema()
        combined_df.show(5)
        combined_df = combined_df.withColumn("Assignee_name",when(col("id").isNull(), col("Assignee_array_Name")).otherwise(col("Assignee_array_Name")))
        combined_df = combined_df.withColumn("Assignee_OrganizationName",when(col("id").isNull(), col("Assignee_array_OrganizationName")).otherwise(col("Assignee_array_OrganizationName")))
        combined_df = combined_df.withColumn("Assignee_OrganizationId",when(col("id").isNull(), col("Assignee_array_OrganizationId")).otherwise(col("Assignee_array_OrganizationId")))
        combined_df = combined_df.withColumn("Assignee_UserId",when(col("id").isNull(), col("Assignee_array_UserId")).otherwise(col("Assignee_array_UserId")))
        combined_df = combined_df.withColumn("projectid",lit(ProjectId))
        combined_df = combined_df.drop("id").drop("Assignee_main_Name").drop("Assignee_main_OrganizationName").drop("Assignee_main_OrganizationId").drop("Assignee_main_UserId").drop("Assignee_array_Name").drop("Assignee_array_OrganizationName").drop("Assignee_array_OrganizationId").drop("Assignee_array_UserId")

        # Show the combined DataFrame
        combined_df.printSchema()
        combined_df_count = combined_df.count()
        print("combined_df count --> " + str(combined_df_count))
        distinct_count = combined_df.select("_WorkflowId").distinct().count()
        print("distinct_count column --> " + str(distinct_count))
        print(combined_df.columns)
        
        dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "dynamic_frame")
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

        column_names = [field.name for field in dynamic_frame.schema().fields]
        for column_name in column_names:
                        dynamic_frame = dynamic_frame.resolveChoice(specs=[(column_name, "cast:string")])     

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

        # sample_data = sample_data.repartition(1)
    
        # success = write_glue_df_to_s3_with_specific_file_name(
        #     glueContext,
        #     DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
        #     #dynamic_frame,
        #     bucket_name,
        #     sample_s3_key,
        #     function_name
        # )

        save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)
    
        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")
        
        
        workflow_project_audit_object=f"{incremental_criteria_folder_location}/{source_name}_{function_name}_{ProjectId}.txt"
        print("workflow_project_audit_object --> " + workflow_project_audit_object)
            
        
        print("audit_date_value --> " + audit_date_value)
        
        project_audit_column=f"{ProjectId},{source_name},{function_name},{audit_date_value}"  
        print("project_audit_column --> " + project_audit_column)
        if project_audit_column:
            if s3_client.upload_to_s3(project_audit_column,workflow_project_audit_object,kms_key_id,is_gzip=False):
                logger.info(f"Uploaded Aconex Workflow Project info to {bucket_name}/{object_name}")
                success = True
            else:
                logger.error("Aconex Workflow Export API was successful but the gzip content could not be uploaded to S3")
                success = False
        else:
            logger.error("Failed to fetch Aconex Workflow Export API payload")
            success = False
    
    else:
        logger.error("Failed to fetch Export API data")
        sys.exit(1)  # Exit with failure code
    
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
