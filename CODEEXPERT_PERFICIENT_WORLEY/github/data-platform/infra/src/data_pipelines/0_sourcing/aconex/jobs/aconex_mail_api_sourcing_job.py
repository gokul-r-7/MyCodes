import sys
import json
import base64
import boto3
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.context import SparkContext
import xml.etree.ElementTree as ET
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, save_spark_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import (
    TIMEZONE_SYDNEY,
    DATETIME_FORMAT,
    REGION, DATE_FORMAT,
    AUDIT_DATE_COLUMN,
    TIMEZONE_UTC,
    TIMESTAMP_FORMAT
)
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
from pyspark.sql.functions import col, when
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import explode,struct,to_json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import datetime
import pytz

FOLDER_TIMESTAMP_FORMAT: str = "%Y-%m-%d-%H-%M-%S"

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
    sys.argv, ["JOB_NAME", "ProjectId", "source_name", "function_name","metadata_table_name", "endpoint_host"]
)

project_id=args.get("ProjectId")
source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
endpoint_host=args.get("endpoint_host")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
today = datetime.datetime.now(pytz.UTC)
current_batch_run_time = today.strftime(TIMESTAMP_FORMAT)
print("Project ID ->"+project_id)

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
project_id_partition = f"Project={project_id}"
relationalized_data_path = aconex_export_config['job_parameter']['input_path']
relationalized_data_path = f"{relationalized_data_path}/{project_id}/relationalized_data"
parquet_data_path = aconex_export_config['job_parameter']['output_s3']
row_tag = aconex_export_config['job_parameter']['row_tag']
root_name=aconex_export_config['job_parameter']['root_tag']
region = aconex_export_config['aws_region']
incremental_default_date = aconex_export_config['job_parameter']['incremental_default_date']
page_size=aconex_export_config['page_size']
incremental_criteria_folder_location = aconex_export_config['job_parameter']['incremental_criteria_folder_location']
endpoint_suffix=aconex_export_config['api_parameter']['endpoint_suffix']
endpoint_prefix=aconex_export_config['api_parameter']['endpoint_prefix']
aconex_export_config['api_parameter']['endpoint'] = f"{endpoint_prefix}{endpoint_host}{endpoint_suffix}"
endpoint = aconex_export_config['api_parameter']['endpoint']
sample_data_path = aconex_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(aconex_export_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_export_config['job_parameter']['sampling_seed']
return_fields=aconex_export_config['return_fields']
auth_type = aconex_export_config['api_parameter']['auth_type']
mail_box=aconex_export_config['mail_box']
name=aconex_export_config['name']
full_incr = aconex_export_config['job_parameter']['full_incr']
temp_deletion_path = aconex_export_config['job_parameter']['input_path']
temp_deletion_path = f"{temp_deletion_path}/{project_id}"
api_retry_count = aconex_export_config['api_parameter']['api_retry']

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
print("incremental_default_date -> " + incremental_default_date)
print("incremental_criteria_folder_location -> " + str(incremental_criteria_folder_location))
print("endpoint_suffix -> " + str(endpoint_suffix))
print("endpoint_prefix -> " + str(endpoint_prefix))

#######################################################


success = False
kms_key_id = aconex_export_config['job_parameter']['kms_key_id']
isDataProcessed = False

try:
  if project_id==project_id:
    folder_name=f"{incremental_criteria_folder_location}"
    print(folder_name)
    file_name=f"{source_name}_{function_name}_{project_id}.txt"
    print(file_name)
    content,status=s3_client.read_s3_file(folder_name,file_name)
        
    #print("line o 123 status= "+status)
    if status:
      incremental_date_project_file=content.split(',')[3]
      incremental_from_date=incremental_date_project_file
      incremental_to_date=generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)
      print("incremental_date_project_file --> " + incremental_date_project_file)
    else:
        incremental_from_date=incremental_default_date
        incremental_to_date=generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)
        
    print("incremental_date --> " + incremental_from_date+ " TO "+ incremental_to_date)
      
    for box in mail_box:
      counter=1
      if full_incr.upper() == "I".upper():
          endpoint_mail_url= endpoint + "/" + project_id + "/" +"mail?search_type=paged&page_size="+str(page_size)+"&mail_box="+box+"&return_fields="+ return_fields +"&page_number="+str(counter)+"&sort_field=sentdate&search_query=sentdate:["+incremental_from_date+" TO "+incremental_to_date+"]"
      else:
          endpoint_mail_url= endpoint + "/" + project_id + "/" +"mail?search_type=paged&page_size="+str(page_size)+"&mail_box="+box+"&return_fields="+ return_fields +"&page_number="+str(counter)+"&sort_field=sentdate" 
            
      print("endpoint_project_url --> " + endpoint_mail_url)
      aconex_export_config['api_parameter']['endpoint']=f"{endpoint_mail_url}"
      http_client = HTTPClient(aconex_export_config)
      mail_response, api_status,status_code = http_client.run()
      #print(mail_response)
        
      if "403" in str(status_code):
        print("Skipping the loop for endpoint_project_url --> " + endpoint_mail_url)
        logger.info("Skipping the loop for endpoint_project_url --> " + endpoint_mail_url)
        #continue  # Skip the current iteration and continue with the next one
        os._exit(0)
      else: 
        root=ET.fromstring(mail_response)
        total_pages=root.get("TotalPages")
        print("Total number of pages for "+project_id+"-->"+total_pages)
        total_results=root.get("TotalResults")
        if total_results=="0":
            break
      try:
          
        while counter<=int(total_pages):
          if full_incr.upper() == "I".upper():
            mail_paginated_url= endpoint + "/" + project_id + "/" +"mail?search_type=paged&page_size="+str(page_size)+"&mail_box="+box+"&return_fields="+ return_fields +"&page_number="+str(counter)+"&sort_field=sentdate&search_query=sentdate:["+incremental_from_date+" TO "+incremental_to_date+"]"
          else:
            mail_paginated_url= endpoint + "/" + project_id + "/" +"mail?search_type=paged&page_size="+str(page_size)+"&mail_box="+box+"&return_fields="+ return_fields +"&page_number="+str(counter)+"&sort_field=sentdate" 
            
          print(mail_paginated_url)
                    
          object_name = f"{raw_data_location}/{project_id}/{box}_page_{str(counter)}_{incremental_from_date}TO{incremental_to_date}.xml"
          
                    
          aconex_export_config['api_parameter']['endpoint']=f"{mail_paginated_url}"
          http_client=HTTPClient(aconex_export_config)
          api_call_attempt=1
          while (api_call_attempt < api_retry_count):
            mail_paginated_response,api_status,status_code=http_client.run()
            if status_code == 200:
                break
            else:
                print(f"API Attempt {api_call_attempt} failed. Retrying")
                api_call_attempt=api_call_attempt+1
          counter=counter+1
          
          if mail_paginated_response:
            if s3_client.upload_to_s3(mail_paginated_response,object_name,kms_key_id,is_gzip=True):
              logger.info(f"Uploaded mail response info to {bucket_name}/{object_name}")
              success = True
            else:
              logger.error("Aconex Export API was successful but the gzip content could not be uploaded to S3")
              success = False
          else:
            logger.error("Failed to fetch response")
            success= False
            
        print("Loop finished")
      except Exception as e:
          print("Error --> " + str(e))
          sys.exit(1) #exit with failure code

      # Function to collect all the column names
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
          
      # Function to check if a column exists in the DataFrame schema
      def column_exists(df: DataFrame, column_path: str) -> bool:
        try:
          fields = column_path.split(".")
          schema = df.schema
          for field in fields:
            schema = schema[field].dataType
            print("Schema-->",schema)
          return True
        except KeyError:
          return False
        except TypeError:
            return False
      if success:
       
        # Load whole XML file into a DynamicFrame
        mapped_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    f"s3://{bucket_name}/{raw_data_location}/{project_id}/"
                    ],
                    "recurse": True
                },
                format="xml",
                format_options={"rowTag": row_tag},
                transformation_ctx=f"{function_name}"
                )
        mapped_df= mapped_df.resolveChoice(specs=[("AttachmentFileSize", "cast:string")]).resolveChoice(specs=[("AttachedDocumentCount", "cast:string")])
        
        # Get the schema from the DynamicFrame and collect the fields with data types
        fields_info = collect_schema_fields(mapped_df.schema())

        #Convert all columns to string data type except struct and array
        for field_name, data_type in fields_info:
            if data_type not in ['StructType', 'ArrayType','NullType']:
                mapped_df = mapped_df.resolveChoice(specs=[(field_name, 'cast:string')])
        logger.info(f"Schema after type casting all data types to string")
        
        mapped_df.printSchema()
        def standardizeArray() -> DataFrame:
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
            Field("ToUsers", StructType([
            Field("Recipient", ArrayType(
                    StructType([
                        Field("DistributionType", StringType({}), {}),
                        Field("FirstName", StringType({}), {}),
                        Field("LastName", StringType({}), {}),
                        Field("Name", StringType({}), {}),
                        Field("OrganizationId", StringType({}), {}),
                        Field("OrganizationName", StringType({}), {}),
                        Field("Status", StringType({}), {}),
                        Field("UserId", StringType({}), {})
                    ])
              ))
            ])),

              Field("_MailId", StringType({}), {})
              ]
            )
            # Use this schema in your create_dynamic_frame call
          array_mapped_df = glueContext.create_dynamic_frame.from_options(
          connection_type="s3",
          connection_options={
          "paths": [
            f"s3://{bucket_name}/{raw_data_location}/{project_id}/"
          ],
          "recurse": True
          },
          format="xml",
          format_options={
          "rowTag": row_tag,
          "withSchema": json.dumps(schema.jsonValue())
          },
          transformation_ctx=f"{function_name}"
          )   
          array_mapped_df=array_mapped_df.resolveChoice(specs=[("Attribute1.AttributeTypeNames.AttributeTypeName", "cast:string")]).resolveChoice(
             specs=[("Attribute2.AttributeTypeNames.AttributeTypeName", "cast:string")])
          return array_mapped_df.toDF()

        print("Create a new dynamic frame to standardize array for Attribute1, Attribute2, ToUsers")
        array_df=standardizeArray()

        print("Schema of array converted dataframe")
        array_df.printSchema()    

        logger.info("DynamicFrame created")
      

        spark_df = mapped_df.toDF()
        # Drop columns Attribute1, Attribute2 and ToUsers
        spark_df=spark_df.drop("Attribute1","Attribute2","ToUsers")
        print(f"Join Stndard DF and Array DF")
        spark_df=spark_df.join(array_df,on="_MailId", how="left")

        print("Updated DF after joining Standard and Array DF")
        spark_df.printSchema()

        # Step 2: Check if the column exists before selecting and exploding
        if column_exists(spark_df, "ToUsers.Recipient"):
            exploded_df = spark_df.select(col("ToUsers.Recipient").alias("RecipientArray"),"_MailId")
            exploded_df = exploded_df.withColumn("ExplodedRecipient", explode(col("RecipientArray"))).drop("RecipientArray")
        else:
            print("else")
            # Create an empty DataFrame with the expected schema
            
            from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

            schema = StructType([
                StructField("_MailId", StringType(), True),
                StructField("ExplodedRecipient", StructType([
                    StructField("DistributionType", StringType(), True),
                    StructField("FirstName", StringType(), True),
                    StructField("LastName", StringType(), True),
                    StructField("Name", StringType(), True),
                    StructField("OrganizationId", StringType(), True),
                    StructField("OrganizationName", StringType(), True),
                    StructField("UserId", StringType(), True),
                    StructField("Status", StringType(), True)
                ]), True)
            ])
            exploded_df = spark.createDataFrame([], schema)     
        
        exploded_df.printSchema()

        # Step 3: Select the required columns to create assignees_df
        recipient_df = exploded_df.select("_MailId","ExplodedRecipient.DistributionType","ExplodedRecipient.FirstName","ExplodedRecipient.LastName","ExplodedRecipient.Name","ExplodedRecipient.OrganizationId","ExplodedRecipient.OrganizationName","ExplodedRecipient.UserId","ExplodedRecipient.Status")
        recipient_df.printSchema()

        #Convert updated df back to dynamic frame
        mapped_df = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame")



        meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
          )
        
        repartitioned_df = mapped_df.repartition(meta_num_partitions)
        
        logger.info(f"Relationalized data path location is {relationalized_data_path}")
        
        root_df_rf_relationalize = Relationalize.apply(
            frame=repartitioned_df,
            staging_path=relationalized_data_path,
            name=f"{row_tag}",
            transformation_ctx=f"relationalize__{function_name}"
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
        if 'Mail' in tables:
            mail_df = tables['Mail']
        else:
            mail_df = None
        mail_df.printSchema()

    
        column_name = 'ToUsers.Recipient.struct.DistributionType'
        column_name2 = 'ToUsers.Recipient.DistributionType'
        if column_name in mail_df.columns:   
          mail_df = mail_df.withColumnRenamed("ToUsers.Recipient.struct.DistributionType", "Recipient_main_DistributionType") \
                        .withColumnRenamed("ToUsers.Recipient.struct.FirstName", "Recipient_main_FirstName") \
                        .withColumnRenamed("ToUsers.Recipient.struct.LastName", "Recipient_main_LastName") \
                        .withColumnRenamed("ToUsers.Recipient.struct.Name", "Recipient_main_Name")\
                        .withColumnRenamed("ToUsers.Recipient.struct.OrganizationName", "Recipient_main_OrganizationName") \
                        .withColumnRenamed("ToUsers.Recipient.struct.OrganizationId", "Recipient_main_OrganizationId") \
                        .withColumnRenamed("ToUsers.Recipient.struct.Status", "Recipient_main_Status")\
                        .withColumnRenamed("ToUsers.Recipient.struct.UserId","Recipient_main_UserId")
          mail_df=mail_df.drop("ToUsers.Recipient.array")                
        if column_name2 in mail_df.columns:
            mail_df = mail_df.withColumnRenamed("ToUsers.Recipient.DistributionType", "Recipient_main_DistributionType") \
                        .withColumnRenamed("ToUsers.Recipient.FirstName", "Recipient_main_FirstName") \
                        .withColumnRenamed("ToUsers.Recipient.LastName", "Recipient_main_LastName") \
                        .withColumnRenamed("ToUsers.Recipient.Name", "Recipient_main_Name")\
                        .withColumnRenamed("ToUsers.Recipient.OrganizationName", "Recipient_main_OrganizationName") \
                        .withColumnRenamed("ToUsers.Recipient.OrganizationId", "Recipient_main_OrganizationId") \
                        .withColumnRenamed("ToUsers.Recipient.Status", "Recipient_main_Status")\
                        .withColumnRenamed("ToUsers.Recipient.UserId","Recipient_main_UserId")
               
             
        columns_to_rename = [
            ("DistributionType", "Recipient_array_DistributionType","String"),
            ("FirstName", "Recipient_array_FirstName","String"),
            ("LastName", "Recipient_array_LastName","String"),
            ("Name", "Recipient_array_Name","String"),
            ("OrganizationName", "Recipient_array_OrganizationName","String"),
            ("OrganizationId", "Recipient_array_OrganizationId","String"),
            ("Status", "Recipient_array_Status","String"),
            ("UserId", "Recipient_array_UserId","String"),
            ("_MailId","id","String")
        ]                
        # Iterate through columns and handle missing columns
        for src_col, dest_col, datatype in columns_to_rename:
            if src_col in recipient_df.columns:
                recipient_df = recipient_df.withColumnRenamed(src_col, dest_col)
            else:
                recipient_df = recipient_df.withColumn(dest_col, lit(None))
        # Perform the join on the root_id field
        combined_df = mail_df.join(recipient_df, mail_df._MailId == recipient_df.id,"left")
        combined_df.printSchema()
        if "Recipient_main_DistributionType" in combined_df.columns:
          combined_df = combined_df.withColumn("Recipient_DistributionType",when(col("id").isNull(), col("Recipient_main_DistributionType")).otherwise(col("Recipient_array_DistributionType")))
          combined_df = combined_df.withColumn("Recipient_FirstName",when(col("id").isNull(), col("Recipient_main_FirstName")).otherwise(col("Recipient_array_FirstName")))
          combined_df = combined_df.withColumn("Recipient_LastName",when(col("id").isNull(), col("Recipient_main_LastName")).otherwise(col("Recipient_array_LastName")))
          combined_df = combined_df.withColumn("Recipient_Name",when(col("id").isNull(), col("Recipient_main_Name")).otherwise(col("Recipient_array_Name")))

          combined_df = combined_df.withColumn("Recipient_OrganizationName",when(col("id").isNull(), col("Recipient_main_OrganizationName")).otherwise(col("Recipient_array_OrganizationName")))
          combined_df = combined_df.withColumn("Recipient_OrganizationId",when(col("id").isNull(), col("Recipient_main_OrganizationId")).otherwise(col("Recipient_array_OrganizationId")))
          combined_df = combined_df.withColumn("Recipient_Status",when(col("id").isNull(), col("Recipient_main_Status")).otherwise(col("Recipient_array_Status")))
          combined_df = combined_df.withColumn("Recipient_UserId",when(col("id").isNull(), col("Recipient_main_UserId")).otherwise(col("Recipient_array_UserId")))

          combined_df = combined_df.drop("id")\
          .drop("Recipient_array_DistributionType")\
          .drop("Recipient_array_FirstName")\
          .drop("Recipient_array_LastName")\
          .drop("Recipient_array_Name")\
          .drop("Recipient_array_OrganizationName")\
          .drop("Recipient_array_OrganizationId")\
          .drop("Recipient_array_Status")\
          .drop("Recipient_array_UserId")\
          .drop("Recipient_main_DistributionType")\
          .drop("Recipient_main_FirstName")\
          .drop("Recipient_main_LastName")\
          .drop("Recipient_main_Name")\
          .drop("Recipient_main_OrganizationName")\
          .drop("Recipient_main_OrganizationId")\
          .drop("Recipient_main_Status")\
          .drop("Recipient_main_UserId")
        else:
           combined_df = combined_df.withColumnRenamed("Recipient_array_DistributionType", "Recipient_DistributionType") \
                        .withColumnRenamed("Recipient_array_FirstName", "Recipient_FirstName") \
                        .withColumnRenamed("Recipient_array_LastName", "RecipientLastName") \
                        .withColumnRenamed("Recipient_array_Name", "Recipient_Name")\
                        .withColumnRenamed("Recipient_array_OrganizationName", "Recipient_OrganizationName") \
                        .withColumnRenamed("Recipient_array_OrganizationId", "Recipient_OrganizationId") \
                        .withColumnRenamed("Recipient_array_Status", "Recipient_Status")\
                        .withColumnRenamed("Recipient_array_UserId","Recipient_UserId") 
           combined_df = combined_df.drop("ToUsers.Recipient")                

        combined_df = combined_df.withColumn("projectid",lit(project_id))
        
          
        # Show the combined DataFrame
        combined_df.printSchema()
        combined_df_count = combined_df.count()
        print("combined_df count --> " + str(combined_df_count))
        distinct_count = combined_df.select("_MailId").distinct().count()
        print("distinct_count column --> " + str(distinct_count))
                        
        
        dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "dynamic_frame")
        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
        batch_partition_path = get_partition_str_mi(partition_date)
        # To support old runs which does not have batch run from Airflow.
        # Check if there is data then use it  
        if current_batch_run_time.strip():
            batch_partition_path = get_partition_str_mi(
                input_date=current_batch_run_time,
                timezone=None,
                input_date_format=TIMESTAMP_FORMAT
            )
        partition_str = f"{batch_partition_path}"
        partitioned_s3_key = (
            parquet_data_path
            + "/"
            + row_tag
            + "_"
            + box
            +"/"
            +"Project="
            +project_id
            +"/"
            + partition_str
            )
   
        column_names = [field.name for field in dynamic_frame.schema().fields]
        for column_name in column_names:
                        dynamic_frame = dynamic_frame.resolveChoice(specs=[(column_name, "cast:string")])

        # Write partitioned data to S3
        success = write_glue_df_to_s3_with_specific_file_name(
          glueContext,
          dynamic_frame,
          #root_df_rf.select(f"{root_name}"),
            bucket_name,
          partitioned_s3_key,
          function_name,
          typecast_cols_to_string = True
        )
     

        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {root_name}")
     
        #sample_s3_key = sample_data_path + "/" + row_tag + "_" + box + "/"
        sample_s3_key = "s3://" + bucket_name + "/" + sample_data_path + "/" + row_tag + "_" + box
        

        #write samle data to s3 without partitioning
     
        sample_data = (
         #root_df_rf.select(f"{root_name}")
         dynamic_frame
         .toDF()
         .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
         )
         #adjust the fraction as needed
        logger.info(f"Selected sample data {source_name}")

        #sample_data = sample_data.repartition(1)
        save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)

        # success = write_glue_df_to_s3_with_specific_file_name(
        #     glueContext,
        #     DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
        #     bucket_name,
        #     sample_s3_key,
        #     function_name
        #     )

        logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")
        isDataProcessed=True
      else:
        logger.error("Failed to fetch Export API data")


      # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of GlueJob
      logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

      temp_path=f"s3://{bucket_name}/{temp_deletion_path}"
      print("temp path ->" + temp_path)
      temp_path = s3_client.get_folder_path_from_s3(temp_path)
      logger.info(f"Deleting folder path {temp_path}")
      s3_client.delete_folder_from_s3(temp_path)
     
      
      
    if isDataProcessed:  
      mail_project_audit_object=f"{incremental_criteria_folder_location}/{source_name}_{function_name}_{project_id}.txt"
      print("mail_project_audit_object --> " + mail_project_audit_object)
    
      audit_date_value = generate_today_date_string(TIMEZONE_SYDNEY,AUDIT_DATE_COLUMN)
      print("audit_date_value --> " + audit_date_value)
            
      project_audit_column=f"{project_id},{source_name},{function_name},{incremental_to_date},{audit_date_value}"  
      print("project_audit_column --> " + project_audit_column)
      if project_audit_column:
        if s3_client.upload_to_s3(project_audit_column,mail_project_audit_object,kms_key_id,is_gzip=False):
          logger.info(f"Uploaded Aconex mail Project info to {bucket_name}/{incremental_criteria_folder_location}")
          success = True
        else:
          logger.error("Aconex mail Export API was successful but the gzip content could not be uploaded to S3")
          success = False
      else:
        logger.error("Failed to fetch Aconex mail Export API payload")
        success = False 
      
      
      
  else:
    logger.error("Failed to fetch Aconex Export API payload")
    success = False
    
except Exception as e: 
    print("Error --> " + str(e))
    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    temp_path=f"s3://{bucket_name}/{temp_deletion_path}"
    temp_path = s3_client.get_folder_path_from_s3(temp_path)
    logger.info(f"Delete staging data after relationalizing - {temp_path}")
    logger.info(f"Deleting folder path {temp_path}")
    s3_client.delete_folder_from_s3(temp_path)  
    sys.exit(1)  # Exit with failure code
job.commit()
