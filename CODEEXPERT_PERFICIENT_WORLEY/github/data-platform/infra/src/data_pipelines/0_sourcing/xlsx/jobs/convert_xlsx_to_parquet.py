import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from awsglue.dynamicframe import DynamicFrame
from io import BytesIO
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, read_excel_from_s3
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import DynamoDB

# Init the logger
logger = get_logger(__name__)

# Initialize AWS Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['sourceName', 'functionName', 'metadata_table_name'])

# Extract parameters

source_name = args['sourceName']
metadata_table_name = args["metadata_table_name"]
function_name = args['functionName'] 

# Define the Sort Keys for DynamoDB Fetch
input_keys = source_name + "#" + function_name + "#"
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info("Content of dynamodb")
logger.info(metadata)
bucket_name = metadata["job_parameter"]["bucket_name"]
raw_files_path = metadata["job_parameter"]["raw_files_path"]
parquet_files_path = metadata["job_parameter"]["parquet_files_path"]
csv_files_path = metadata["job_parameter"]["csv_files_path"]
            
# Function to clean and format column names
"""def clean_column_names(columns):
    cleaned_columns = []
    for col in columns:
        # Remove special characters, replace spaces with underscores
        cleaned_col = col.strip().replace(' ', '_').replace('[^\w\s]', '')
        cleaned_columns.append(cleaned_col)
    return cleaned_columns"""

# Function to convert Excel to Parquet
def convert_to_parquet(sheet_info):
    
    sheet_name = sheet_info["sheet_name"]
    xls_engine_name = sheet_info["xls_engine_name"] 
    quote_character = sheet_info["quote_character"]
    with_headers = sheet_info["with_headers"]
    separator = sheet_info["separator"]
    multiline = sheet_info["multiline"]
    header_row = sheet_info["header_row"]
    header_column = sheet_info["header_column"]
    data_start_row = sheet_info["data_start_row"]
    data_start_column = sheet_info["data_start_column"]
    specialchars_to_be_replaced_in_columnnames = sheet_info["specialchars_to_be_replaced_in_columnnames"]
    replacement_char=sheet_info["replacement_char"]
    replace_non_printable_ascii_with_underscore = sheet_info["replace_non_printable_ascii_with_underscore"]
    sampling_fraction = float(sheet_info["sampling_fraction"])
    sampling_seed = sheet_info["sampling_seed"]

    logger.info(f"Processing Excel file: {raw_files_path}{xls_filename} and sheet {sheet_name}")
    df = read_excel_from_s3(bucket_name, f"{raw_files_path}{xls_filename}", sheet_name, xls_engine_name, metadata["aws_region"])
    
    filename = xls_filename.replace(".xlsx", "")
    
    # Extract headers and clean them
    headers = df.iloc[header_row, header_column:].astype(str).tolist()
    #cleaned_headers = clean_column_names(header_row)
    cleaned_headers = headers
    
    # Set data types for all columns to string
    df = df.astype(str)
    
    # Pass dataRow position to skip empty or filtered rows
    new_df = df.iloc[data_start_row:, data_start_column:]
    new_df.columns = cleaned_headers
    
    # Drop rows and columns that are entirely empty
    new_df = new_df.dropna(how='all', axis=0)
    new_df = new_df.dropna(how='all', axis=1)
    
    # Write DataFrame to CSV
    csv_buffer = BytesIO()
    new_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    csv_file_path = f"{csv_files_path}{filename}.csv"
    
    # Upload CSV to S3
    s3 = boto3.client('s3', region_name=metadata["aws_region"])
    s3.put_object(Bucket=bucket_name, Key=csv_file_path, Body=csv_buffer.getvalue())
    
    # Create DynamicFrame from CSV
    datasource = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": quote_character, "withHeader": with_headers, "separator": separator, "multiline": multiline, "optimizePerformance": False},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [f"s3://{bucket_name}/{csv_file_path}"], "recurse": True},
        transformation_ctx="read_csv"
    )
    
    if datasource.count() > 0:
        logger.info(f"Number of rows in datasource: {datasource.count()}")
        
        # Define the output path for Parquet file
        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
        partition_str = f"{get_partition_str_mi(partition_date)}"
        output_path = f"s3://{bucket_name}/{parquet_files_path}{filename}.parquet/{partition_str}"
        # Write partitioned data to S3
        partitioned_s3_key = f"{parquet_files_path}{filename}.parquet/{partition_str}"
        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            datasource,
            bucket_name,
            partitioned_s3_key,
            function_name + 'parquet',
            csv_or_xlsx_datasource=True,
            special_chars=specialchars_to_be_replaced_in_columnnames,
            replacement_chars=replacement_char,
            replace_non_ascii=replace_non_printable_ascii_with_underscore
        )

        if success:                  
            logger.info(f"Parquet file has been written to the location {output_path}")

            # Write sample data to S3 without partitioning
            sample_data = (
                datasource
                .toDF()
                .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
            )  # Adjust the fraction as needed

            sample_data = sample_data.repartition(1)

            logger.info(f"Selected sample data {filename}")

            sample_s3_key = f"xls_sample_data/{parquet_files_path}{filename}.parquet/"

            ctx = function_name + "parquet" + "sampledata"

            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                bucket_name,
                sample_s3_key,
                ctx,
                csv_or_xlsx_datasource=True,
                special_chars=specialchars_to_be_replaced_in_columnnames,
                replacement_chars=replacement_char,
                replace_non_ascii=replace_non_printable_ascii_with_underscore
            )

            if success:
                logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {filename}")
            else:
                logger.error(f"Failed to write sample data for {filename}")
        else:
            logger.error(f"Failed to write Parquet file for {filename}")            


# Access and print relevant information
for file_info in metadata["job_parameter"]["files"]:
    xls_filename = file_info["xls_filename"]
    logger.info(f"Processing file: {xls_filename}")
    
    for sheet_info in file_info["sheets"]:
        #Set this dynamically
        sheet_info["xls_engine_name"] = metadata["job_parameter"]["xls_engine_name"]
        convert_to_parquet(sheet_info)

job.commit()
