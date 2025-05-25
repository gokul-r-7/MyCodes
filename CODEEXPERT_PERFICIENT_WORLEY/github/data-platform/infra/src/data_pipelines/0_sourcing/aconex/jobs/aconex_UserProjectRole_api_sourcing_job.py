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
from pyspark.sql.functions import explode, col, when, lit
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
import awsglue.gluetypes as gt
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, TimestampType
from pyspark.sql import DataFrame

#Add instance name to dynamic frame
def AddInstanceNameCol(r):  
    r["projectInstance"] = instance_name
    return r 
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
logger.info("Project ID ->"+ project_id)

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
page_size=aconex_export_config['page_size']
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


logger.info("Bucket Name -> " + bucket_name)
logger.info("raw_data_location -> " + raw_data_location)
logger.info("relationalized_data_path -> " + relationalized_data_path)
logger.info("parquet_data_path -> " + parquet_data_path)
logger.info("row_tag -> " + row_tag)
logger.info("aconex_export_config -> " + str(aconex_export_config))
logger.info("endpoint_suffix -> " + str(endpoint_suffix))
logger.info("endpoint_prefix -> " + str(endpoint_prefix))


success = False
kms_key_id = aconex_export_config['job_parameter']['kms_key_id']

schema = gt.StructType([
    gt.Field("Id", gt.StringType({}), {}),
    gt.Field("Name", gt.StringType({}), {}),
    gt.Field("NewOrgRole", gt.StringType({}), {}),
    gt.Field("OrganizationAdminRole", gt.StringType({}), {}),
    gt.Field("OwningOrganizationId", gt.StringType({}), {}),
    gt.Field("ProjectId", gt.StringType({}), {}),    
    gt.Field("Users", gt.StructType([
        gt.Field("User", gt.ArrayType(gt.StructType([
            gt.Field("Email", gt.StringType({}), {}),
            gt.Field("FirstName", gt.StringType({}), {}),
            gt.Field("LastName", gt.StringType({}), {}),
            gt.Field("MiddleName", gt.StringType({}), {}),
            gt.Field("Mobile", gt.StringType({}), {}),
            gt.Field("UserId", gt.StringType({}), {}),
            gt.Field("UserName", gt.StringType({}), {}),
            gt.Field("UserTitle", gt.StringType({}), {})
        ])))
    ]))
])

try:
    if project_id==project_id:
      counter=1
      endpoint_url = endpoint+"roles/users/projects/"+project_id
      logger.info("endpoint_url --> " + endpoint_url)
      aconex_export_config['api_parameter']['endpoint']=f"{endpoint_url}"
      http_client = HTTPClient(aconex_export_config)
      UserProjectRole_response, api_status,status_code = http_client.run()
      if status_code == 400:
          logger.info("This project does not have permission so exiting gracefully")
          os._exit(os.EX_OK)
      #logger.info(UserProjectRole_response)
      root=ET.fromstring(UserProjectRole_response)
      total_pages=root.get("TotalPages")
      logger.info("Total number of pages for "+project_id+"-->"+total_pages)
      total_results=root.get("TotalResults")
      object_name = f"{raw_data_location}/{project_id}/data.xml"
      
      try:
        while counter<=int(total_pages):
          role_url=endpoint+"roles/users/projects/"+project_id+"?page_number="+str(counter)
          object_name=f"{raw_data_location}/{project_id}/{project_id}_page_{str(counter)}data.xml"
          
          aconex_export_config['api_parameter']['endpoint']=f"{role_url}"
          http_client=HTTPClient(aconex_export_config)
          role_paginated_response,api_status,status_code=http_client.run()
          counter=counter+1
          if role_paginated_response: 
            if s3_client.upload_to_s3(role_paginated_response,object_name,kms_key_id,is_gzip=True):
              logger.info(f"Uploaded role response info to {bucket_name}/{object_name}")
              success = True
            else:
              logger.error("Aconex Export API was successful but the gzip content could not be uploaded to S3")
              success = False           
          else:
            logger.error("Failed to fetch response")
            success= False
        logger.info("Loop finished")
      except Exception as e:
          logger.error("Error --> " + str(e))
          sys.exit(1) #exit with failure code    

 
      success = True
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
                format_options={"rowTag":row_tag},
                transformation_ctx=f"{function_name}"
                )
       logger.info("DynamicFrame created")

       # Get the schema from the DynamicFrame and collect the fields with data types
       fields_info = collect_schema_fields(mapped_df.schema())

       #Convert all columns to string data type except struct and array
       for field_name, data_type in fields_info:
          print(f"field nam - {field_name} and datatype - {data_type}") 
          if data_type not in ['StructType', 'ArrayType','NullType']:
              mapped_df = mapped_df.resolveChoice(specs=[(field_name, 'cast:string')])
        
       def standardizeArray() -> DataFrame:
          from pyspark.sql import DataFrame
          from awsglue.dynamicframe import DynamicFrame
          from awsglue.gluetypes import Field,StructType,ArrayType,StringType,IntegerType
          schema = StructType([ 
            Field("Id", StringType({}), {}),
            Field("Name", StringType({}), {}),
            Field("NewOrgRole", StringType({}), {}),
            Field("OrganizationAdminRole", StringType({}), {}),
            Field("OwningOrganizationId", StringType({}), {}),
            Field("ProjectId", StringType({}), {}),
            Field("Users", StructType([
            Field("User", ArrayType(
                StructType([
                    Field("Email", StringType({}), {}),
                    Field("FirstName", StringType({}), {}),
                    Field("LastName", StringType({}), {}),
                    Field("MiddleName", StringType({}), {}),
                    Field("Mobile", StringType({}), {}),
                    Field("UserId", StringType({}), {}),
                    Field("UserName", StringType({}), {}),
                    Field("UserTitle", StringType({}), {})
                    ])
                ))
            ]))
            ])
          # Use this schema in your create_dynamic_frame call
          array_mapped_df = glueContext.create_dynamic_frame.from_options(
          connection_type="s3",
          connection_options={
          "paths": [f"s3://{bucket_name}/{raw_data_location}/{project_id}/"],
          "recurse": True
          },
          format="xml",
          format_options={"rowTag": row_tag,"withSchema": json.dumps(schema.jsonValue())},
          transformation_ctx=f"{function_name}"
          )
          return array_mapped_df.toDF()
       # Add aconex instance name to data
       array_df=standardizeArray()
       print("Schema of array converted dataframe")
       array_df.printSchema()  
       # root_df = Map.apply(frame = mapped_df, f = AddInstanceNameCol)
       mapped_df.printSchema()
       spark_df = mapped_df.toDF()
       # Drop columns Name, NewOrgRole,OrganizationAdminRole and ToUsers
       spark_df=spark_df.drop("Name","NewOrgRole","OrganizationAdminRole","OwningOrganizationId","ProjectId","Users")
       print(f"Join Stndard DF and Array DF")
       spark_df=spark_df.join(array_df,on="Id", how="left")
       print("Updated DF after joining Standard and Array DF")
       spark_df.printSchema()
       
       # Step 2: Check if the column exists before selecting and exploding
       if column_exists(spark_df, "Users.User"):
           logger.info(f" is user array exist - yes")
           exploded_df = spark_df.select(col("Users.User").alias("UserArray"),"Id")
           exploded_df = exploded_df.withColumn("ExplodedUsers", explode(col("UserArray"))).drop("UserArray")
       else:
           logger.info("no users ")
           # Create an empty DataFrame with the expected schema
           from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
           emptyschema = StructType([
               StructField("Id",  StringType(), True),
               StructField("Name",  StringType(), True),
               StructField("NewOrgRole",  StringType(), True),
               StructField("OrganizationAdminRole",  StringType(), True),
               StructField("OwningOrganizationId",  StringType(), True),
               StructField("ProjectId",  StringType(), True),
               StructField("ExplodedUsers", StructType([
                   StructField("Email",  StringType(), True),
                   StructField("FirstName",  StringType(), True),
                   StructField("LastName",  StringType(), True),
                   StructField("MiddleName",  StringType(), True),
                   StructField("Mobile",  StringType(), True),
                   StructField("UserId",  StringType(), True),
                   StructField("UserName",  StringType(), True),
                   StructField("UserTitle",  StringType(), True)
                   ]))
           ])
           exploded_df = spark.createDataFrame([], emptyschema)
       print("Array exploded for users")
       
       ctx=f"{function_name}"
       array_exploded_dyf = DynamicFrame.fromDF(exploded_df, glueContext, ctx)
       array_mapped_dyf=array_exploded_dyf.resolveChoice(specs=[("ExplodedUsers.Mobile", "cast:string")]).resolveChoice(
           specs=[("Mobile", "cast:string")])
       array_mapped_df = array_mapped_dyf.toDF()
       print("resolved step")
       User_df = array_mapped_df.select("Id","ExplodedUsers.Email","ExplodedUsers.FirstName","ExplodedUsers.LastName","ExplodedUsers.MiddleName","ExplodedUsers.Mobile","ExplodedUsers.UserId","ExplodedUsers.UserName","ExplodedUsers.UserTitle")
       User_df.printSchema()
       
       #User_df = User_df.withColumn("Mobile",when(col("Mobile.long").isNotNull(), col("Mobile.long")).otherwise(col("Mobile.string")))
       
       

       User_df.printSchema()
       meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
          )
       repartitioned_df = mapped_df.repartition(meta_num_partitions)
       logger.info("repartitioned DF schema")
       repartitioned_df.printSchema()
       logger.info(f"Relationalized data path location is {relationalized_data_path}")
       root_df_rf_relationalize = Relationalize.apply(
            frame=repartitioned_df,
            staging_path=relationalized_data_path,
            name=f"{row_tag}",
            transformation_ctx=f"relationalize__{function_name}"
          )
       logger.info("Relationalize applied")
       logger.info(root_df_rf_relationalize)  # This should print the dictionary's contents
       logger.info(root_df_rf_relationalize.keys())  # This should print the dictionary's keys
        
        # Convert the relationalized output to individual tables (DynamicFrames)
       tables = {}
       for name_df in root_df_rf_relationalize.keys():
            if name_df != '':
                logger.info("processing the tag --> " + name_df)
                tables[name_df] = root_df_rf_relationalize.select(name_df).toDF()   
        
       logger.info(tables)
            
       for table_name, df in tables.items():
            logger.info(f"Table Name: {table_name}")
            df.printSchema()
        
        # Access specific tables if needed
       if 'Role' in tables:
            main_df = tables['Role']
       else:
            main_df = None
       column_name = 'Users.User.struct.Email'
       column_name2 = 'Users.User.Email'
       if column_name in main_df.columns:
           main_df=main_df.withColumnRenamed("Users.User.struct.Email","User_main_Email")\
                      .withColumnRenamed("Users.User.struct.FirstName","User_main_FirstName")\
                      .withColumnRenamed("Users.User.struct.LastName","User_main_LastName")\
                      .withColumnRenamed("Users.User.struct.MiddleName","User_main_MiddleName")\
                      .withColumnRenamed("Users.User.struct.Mobile","User_main_Mobile")\
                      .withColumnRenamed("Users.User.struct.UserId","User_main_UserId")\
                      .withColumnRenamed("Users.User.struct.UserName","User_main_UserName")\
                      .withColumnRenamed("Users.User.struct.UserTitle","User_main_UserTitle")
           main_df=main_df.drop("Users.User.array")
       if column_name2 in main_df.columns:
           main_df = main_df.withColumnRenamed("Users.User.Email","User_main_Email")\
                      .withColumnRenamed("Users.User.FirstName","User_main_FirstName")\
                      .withColumnRenamed("Users.User.LastName","User_main_LastName")\
                      .withColumnRenamed("Users.User.MiddleName","User_main_MiddleName")\
                      .withColumnRenamed("Users.User.Mobile","User_main_Mobile")\
                      .withColumnRenamed("Users.User.UserId","User_main_UserId")\
                      .withColumnRenamed("Users.User.UserName","User_main_UserName")\
                      .withColumnRenamed("Users.User.UserTitle","User_main_UserTitle")
       columns_to_rename = [
            ("Email", "User_array_Email","String"),
            ("FirstName", "User_array_FirstName","String"),
            ("MiddleName", "User_array_MiddleName","String"),
            ("LastName", "User_array_LastName","String"),
            ("UserId", "User_array_UserId","String"),
            ("UserName", "User_array_UserName","String"),
            ("UserTitle", "User_array_UserTitle","String"),
            ("Mobile", "User_array_Mobile","String"),
            ("Id","array_id","int")
        ]
                      
       # Iterate through columns and handle missing columns
       for src_col, dest_col, datatype in columns_to_rename:
            if src_col in User_df.columns:
                User_df = User_df.withColumnRenamed(src_col, dest_col)
            else:
                User_df = User_df.withColumn(dest_col, lit(None))
        # Perform the join on the root_id field
       combined_df = main_df.join(User_df, main_df.Id == User_df.array_id,"left")
       if "User_main_UserId" in combined_df.columns:
           combined_df = combined_df.withColumn("User_Email",when(col("array_id").isNull(), col("User_main_Email")).otherwise(col("User_array_Email")))
           combined_df = combined_df.withColumn("User_FirstName",when(col("array_id").isNull(), col("User_main_FirstName")).otherwise(col("User_array_FirstName")))
           combined_df = combined_df.withColumn("User_LastName",when(col("array_id").isNull(), col("User_main_LastName")).otherwise(col("User_array_LastName")))
           combined_df = combined_df.withColumn("User_MiddleName",when(col("array_id").isNull(), col("User_main_MiddleName")).otherwise(col("User_array_MiddleName")))
           combined_df = combined_df.withColumn("User_Mobile",when(col("array_id").isNull(), col("User_array_Mobile")).otherwise(col("User_array_Mobile")))
           combined_df = combined_df.withColumn("User_UserId",when(col("array_id").isNull(), col("User_main_UserId")).otherwise(col("User_array_UserId")))
           combined_df = combined_df.withColumn("User_UserName",when(col("array_id").isNull(), col("User_main_UserName")).otherwise(col("User_array_UserName")))
           combined_df = combined_df.withColumn("User_UserTitle",when(col("array_id").isNull(), col("User_main_UserTitle")).otherwise(col("User_array_UserTitle")))
           
           combined_df = combined_df.drop("array_id")\
                                .drop("User_main_Email")\
                                .drop("User_main_FirstName")\
                                .drop("User_main_LastName")\
                                .drop("User_main_MiddleName")\
                                .drop("User_main_UserId")\
                                .drop("User_main_UserName")\
                                .drop("User_main_UserTitle")\
                                .drop("Users_array")\
                                .drop("User_array_Email")\
                                .drop("User_array_FirstName")\
                                .drop("User_array_MiddleName")\
                                .drop("User_array_LastName")\
                                .drop("User_array_UserId")\
                                .drop("User_array_UserTitle")\
                                .drop("User_array_UserName")\
                                .drop("User_array_Mobile")
       else:
           combined_df = combined_df.withColumnRenamed("User_array_Email", "User_Email") \
                                .withColumnRenamed("User_array_FirstName", "User_FirstName") \
                                .withColumnRenamed("User_array_MiddleName", "User_MiddleName") \
                                .withColumnRenamed("User_array_LastName", "User_LastName")\
                                .withColumnRenamed("User_array_Mobile", "User_Mobile") \
                                .withColumnRenamed("User_array_UserId", "User_UserId") \
                                .withColumnRenamed("User_array_UserName", "User_UserName")\
                                .withColumnRenamed("User_array_UserTitle","User_UserTitle")
           combined_df = combined_df.drop("Users.User")
        # Show the combined DataFrame
       combined_df = combined_df.withColumn("projectid",lit(project_id))        
       combined_df.printSchema()
       combined_df_count = combined_df.count()
       logger.info("combined_df count --> " + str(combined_df_count))
       distinct_count = combined_df.select("User_UserId").distinct().count()
       logger.info("distinct_count column --> " + str(distinct_count))
       combined_df = combined_df.withColumn("instance_name", lit(instance_name).cast("string")) 
       dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "dynamic_frame")
       partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
       partition_str = f"{get_partition_str_mi(partition_date)}"
       partitioned_s3_key = (
            parquet_data_path + "/" + function_name
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
          dynamic_frame,
          bucket_name,
          partitioned_s3_key,
          function_name
        )     
       logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {row_tag}")

       sample_s3_key = "s3://" + bucket_name + "/" + sample_data_path + "/" + source_name
       #write samle data to s3 without partitioning
       sample_data = (
         #root_df_rf.select(f"{root_name}")
         dynamic_frame
         .toDF()
         .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
         )
       #adjust the fraction as needed
       logger.info(f"Selected sample data {source_name}")

      #  sample_data = sample_data.repartition(1)

      #  success = write_glue_df_to_s3_with_specific_file_name(
      #       glueContext,
      #       DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
      #       bucket_name,
      #       sample_s3_key,
      #       function_name
      #       )

       save_spark_df_to_s3_with_specific_file_name(
                                    sample_data, sample_s3_key)
    
       logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {row_tag}")
       # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of GlueJob
       logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

       temp_path=f"s3://{bucket_name}/{temp_deletion_path}/"
       logger.info("temp path ->" + temp_path)
       temp_path = s3_client.get_folder_path_from_s3(temp_path)
       logger.info(f"Deleting folder path {temp_path}")
       s3_client.delete_folder_from_s3(temp_path)
      else:
        logger.error("Failed to fetch API data")      
except Exception as e:
  logger.error("Error --> " + str(e))    
  # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
  temp_path=f"s3://{bucket_name}/{temp_deletion_path}/"
  temp_path = s3_client.get_folder_path_from_s3(temp_path)
  logger.info(f"Delete staging data after relationalizing - {temp_path}")
  logger.info(f"Deleting folder path {temp_path}")
  s3_client.delete_folder_from_s3(temp_path)  
  sys.exit(1)  # Exit with failure code    
job.commit()
