import boto3
import time
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Glue and Spark contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Athena client
athena_client = boto3.client("athena", region_name="your-region")  # Replace 'your-region' with your AWS region

# Athena query configurations
database_name = "your_source_database"
output_bucket = "s3://your-output-bucket/folder/"  # S3 bucket for Athena query results

# SQL query to fetch data from Athena tables
query = """
    SELECT * FROM your_table1
    JOIN your_table2 ON your_table1.id = your_table2.id
    WHERE your_conditions
"""  # Replace with your actual SQL query

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

if status == "SUCCEEDED":
    # Load the result into a DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"{output_bucket}{query_execution_id}.csv"]
        },
        format="csv"
    )
    
    # Write the DynamicFrame to a new Athena table
    glueContext.write_dynamic_frame.from_catalog(
        frame=dynamic_frame,
        database="your_target_database",
        table_name="your_target_table"
    )
    print("Data loaded successfully to the target Athena table.")
else:
    print("Query failed:", query_status["QueryExecution"]["Status"]["StateChangeReason"])