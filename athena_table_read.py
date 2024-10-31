
import boto3
import time
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Athena client
athena_client = boto3.client("athena", region_name="us-east-1")
glue_client = boto3.client("glue", region_name="us-east-1")

# Athena query configurations
database_name = "sample-datebese"
output_bucket = "s3://gokul-test-bucket-07/gluejob-test-folder/"  # S3 bucket for Athena query results
partitition_column = "index"

# SQL query to fetch data from Athena tables
query = """
    SELECT "index","first name", "last name", "company", "city" FROM "sample-datebase"."test_folder_1" limit 10;
"""  

# Execute Athena query
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": database_name},
    ResultConfiguration={"OutputLocation": output_bucket}
)

# Get query execution ID to track the query status
query_execution_id = response["QueryExecutionId"]

# Wait until query finishes
while True:
    query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status["QueryExecution"]["Status"]["State"]

    if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        break
    time.sleep(5)

#if status == "SUCCEEDED":
# Load the result into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [f"{output_bucket}{query_execution_id}.csv"]
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    }
)
df = dynamic_frame.toDF()
df.count()
df.show()


dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
# Specify S3 output path
output_path = "s3://gokul-test-bucket-07/target-folder/"
# Write the DynamicFrame to S3 as partitioned CSV files
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["index"]
    },
    format="csv",
    format_options={
        "separator": ",",
        "quoteChar": "\"",
        "withHeader": True,
         "escapeChar": "\\" 
    }
)
df.show()

    
job.commit()
