import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import DynamoDB
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


################################################
# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "function_name", "metadata_table_name"]
)

#source_name should be "CSV"
source_name = args.get("source_name")
#Function name can be "E3D, Assurance, Sharepoint" etc.
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]


# Define the Sort Keys for DynamoDB Fetch
input_keys = source_name + "#" + function_name + "#"
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

csv_config = metadata
logger.info(f" Config to parse CSV: {csv_config}")

#############  Load CSV parsing Config ################
bucket_name = csv_config['job_parameter']['bucket_name']
raw_files_path = csv_config['job_parameter']['raw_files_path']
quote_character = csv_config['job_parameter']['quote_character']
with_headers = csv_config['job_parameter']['with_headers']
separator = csv_config['job_parameter']['separator']
multiline = csv_config['job_parameter']['multiline']
parquet_files_path = csv_config['job_parameter']['parquet_files_path']

################################################

try:
    # List all files under the input S3 bucket folder
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_files_path)
    files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response.get('Contents', [])]
    logger.info(f"List of CSV files to be processed {files}")

    for file_path in files:
        
        if not file_path.lower().endswith(".csv"):
            logger.info(f"Skipping non CSV file {file_path}")
            continue
        
        logger.info(f"Processing file {file_path}")
        
        # Extract filename from file_path
        filename = file_path.split('/')[-1].replace(".csv", "")  # Extract filename without extension
        
        # Read the CSV file into a DataFrame
        #datasource = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiline": True, "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [file_path], "recurse": True}, transformation_ctx=args['FUNCTION_NAME'])
        datasource = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": quote_character, "withHeader": with_headers, "separator": separator, "multiline": multiline, "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [file_path], "recurse": True}, transformation_ctx=function_name)
        
        
        try:
            logger.info(f"Schema of {file_path}")
            logger.info(datasource.printSchema())

            # Check if the input DataFrame is empty
            # IMPORTANT : The count check covers scenario where the columns are present but 0 rows
            # Check with Worley if we still need to capture the table schema if there are 0 rows but columns are present.
            if datasource.count() > 0:
                # Define the output path for Parquet file
                partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
                    DATETIME_FORMAT
                )
                partition_str = f"{get_partition_str_mi(partition_date)}"
                output_path = f"s3://{bucket_name}/{parquet_files_path}{filename}.parquet/{partition_str}"
                
                # Convert the DataFrame to Parquet format
                """datasink = glueContext.write_dynamic_frame.from_options(frame=datasource, connection_type="s3", format="glueparquet", connection_options={"path": output_path, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx=function_name + 'parquet')"""
                
                # Write partitioned data to S3
                partitioned_s3_key = f"{parquet_files_path}{filename}.parquet/{partition_str}"
                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    datasource,
                    bucket_name,
                    partitioned_s3_key,
                    function_name + 'parquet'
                )
                
                logger.info(f"Parquet file has been written to the location {output_path}")

                # Write sample data to S3 without partitioning
                sample_data = (
                    datasource
                    .toDF()
                    .sample(withReplacement=False, fraction=0.5, seed=42)
                )  # Adjust the fraction as needed

                sample_data = sample_data.repartition(1)

                logger.info(f"Selected sample data {filename}")

                sample_s3_key = f"csv_sample_data/{parquet_files_path}{filename}.parquet/"

                ctx = function_name + "parquet" + "sampledata"

                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                    bucket_name,
                    sample_s3_key,
                    ctx
                )

                logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {filename}")
            else:
                logger.error(f"Input CSV file {file_path} is empty, skipping Parquet conversion, but trying to process rest of the files in the batch")
                
        except Exception as e:     
            # IMPORTANT : This happens in a scenario where the columns and rows but are absent. 0 rows and 0 columns.
            logger.error(f"Input CSV file {file_path} ran into exception {e}, skipping Parquet conversion, but trying to process rest of the files in the batch")

except Exception as e:
    logger.error(f"Error occurred while processing the Glue job: {e}")
    raise e

# Commit the job
job.commit()

