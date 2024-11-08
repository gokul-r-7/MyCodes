
import sys
import uuid
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pyathena import connect

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
glue_client = boto3.client('glue')
starttime = datetime.now()
start_time = starttime.strftime("%Y-%m-%d %H:%M:%S")
unique_id = str(uuid.uuid4())
job_name = "Test_Job"
job_log_database_name = "sample-datebase"
job_log_table_name = "job_log_table"
source_database_name = "sample-datebase"
source_table_name = "test_folder_1"
s3_athena_results = "s3://gokul-test-bucket-07/temporary_files/"
job_log_table_path = "s3://gokul-test-bucket-07/Job_log_table/"
job_log_partitionkey = []
conn = connect(
    s3_staging_dir = s3_athena_results,
    region_name = "us-east-1"
)
def job_lob_table_data(job_load_type,endtime,runtimeseconds,recordcount):
    job_log_data = {
    "id" : unique_id,
    "job_name" : job_name,
    "job_load_type" : job_load_type,
    "job_start_time" : start_time,
    "job_end_time" : endtime,
    "job_run_time" : runtimeseconds,
    "total_count" : recordcount,
    "created_date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    df = spark.createDataFrame([job_log_data])
    return df
    
    
def check_table_exists(database_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        # If the table exists, response will contain metadata
        return True
    except ClientError as e:
        # If the table doesn't exist, an exception is raised
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise  # Rethrow the exception if it's not a table not found error

# Usage example
exists = check_table_exists(job_log_database_name, job_log_table_name)


if exists:
    print(f"Table {job_log_table_name} exists in database {job_log_database_name}.")
else:
    print(f"Table {job_log_table_name} does not exist in database {job_log_database_name}.")
def check_load_type(database_name, table_name):
    # Read the Glue catalog table as a dynamic frame
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name
    )

    # Convert to DataFrame and check if 'LoadType' contains 'Latest13months'
    df = dynamic_frame.toDF()
    
    # Assuming 'LoadType' column is of string type
    load_type_values = df.select("job_load_type").distinct().collect()

    for row in load_type_values:
        if row['job_load_type'] == 'Latest 13 Months':
            return True
    return False

exists = check_load_type(job_log_database_name, job_log_table_name)
if exists:
    print("latest month exists")
else:
    print("latest month not exisits")

def load_data_from_athena(load_full_data=True):
    if load_full_data:
        # Read all data from Athena (Glue Catalog)
        query = 'SELECT * FROM "sample-datebase"."test_folder_1"'
        df = pd.read_sql(query, conn)

    else:
        # Read only the latest month data (Assuming a `Month` partition or similar column exists)
        # Replace `Month` with actual partition/column name for the month
        query = 'SELECT * FROM "sample-datebase"."test_folder_1" order by index desc limit 1'
        df = pd.read_sql(query, conn)
        
    return df

def write_to_s3(df, output_path,partitionkey):
    # Write the dynamic frame to S3 (Parquet format))
    write_df = DynamicFrame.fromDF(df, glueContext, "athena_table_source")
    parquet_output = glueContext.write_dynamic_frame.from_options(
    frame=write_df,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": partitionkey,
        "recurse": True,
        "overwrite": "true"
    },
    format="glueparquet",
    format_options={
        "compression": "gzip"
    }
    )
    return parquet_output
# Step 1: Check if the table exists
s3_output_path ="s3://gokul-test-bucket-07/final_output_path/"
partition_keys = ["index"]
if check_table_exists(job_log_database_name, job_log_table_name):
    
    # Step 2: Check if 'LoadType' contains 'Latest13months'
    if check_load_type(job_log_database_name, job_log_table_name):
        # If 'Latest13months' is present, load only the latest month data
        load_df = load_data_from_athena(load_full_data=False)
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.max_rows', 100) 
        load_df.head()
        record_count = load_df.shape[0]
        loadtype = "Latest 13 Months"
    else:
        # If 'Latest13months' is not present, load all data
        load_df = load_data_from_athena(load_full_data=True)
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.max_rows', 100) 
        load_df.head()
        record_count = load_df.shape[0]
        loadtype = "Latest Current Month"

    
    # Step 3: Write the loaded data to S3
    sparkloaddf = spark.createDataFrame(load_df)
    write_to_s3(sparkloaddf, s3_output_path,partition_keys)
    
else:
    months13_load_df = load_data_from_athena(load_full_data=True)
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.max_rows', 100) 
    months13_load_df.head()
    record_count = months13_load_df.shape[0]
    sparkloaddf = spark.createDataFrame(months13_load_df)
    write_to_s3(sparkloaddf, s3_output_path,partition_keys)
    loadtype = "Latest 13 Months"


endtime = datetime.now()
end_time = endtime.strftime("%Y-%m-%d %H:%M:%S")
runtime_seconds = (endtime - starttime).total_seconds()
job_log_table_df = job_lob_table_data(loadtype,endtime,runtime_seconds,record_count)
job_log_table_df.show()
write_to_s3(job_log_table_df,job_log_table_path,job_log_partitionkey)
job.commit()
