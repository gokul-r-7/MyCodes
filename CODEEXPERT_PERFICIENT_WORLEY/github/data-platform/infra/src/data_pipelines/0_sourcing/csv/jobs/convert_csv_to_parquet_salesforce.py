import sys
import uuid
import os
import shutil
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name, read_excel_from_s3
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import DynamoDB
from botocore.exceptions import ClientError
from worley_helper.utils.helpers import S3Helper

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
    sys.argv, ["JOB_NAME", "source_name", "function_name", "metadata_table_name", "connector_file_path"]
)


# Source name can be "e3d, assurance, sharepoint, oraclegbs" etc.
source_name = args.get("source_name")

# Function name should be "csv or csv_xlsx"
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
connector_file_path = args['connector_file_path']

logger.info(f"Connector file path: {connector_file_path}")

# Init the job to use booksmarks
job.init(args["JOB_NAME"] + source_name + function_name + metadata_table_name, args)

# Define the Sort Keys for DynamoDB Fetch
input_keys = f"{function_name}#{source_name}"

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f"Metadata Response: {metadata}")

# Access and print values from metadata
bucket_name = metadata['job_parameter']['bucket_name']
raw_files_path = metadata['job_parameter']['raw_files_path']
parquet_files_path = metadata['job_parameter']['parquet_files_path']
csv_files_path = metadata['job_parameter']['csv_files_path']
sample_data_location = metadata['job_parameter']['sample_data_location']
drop_duplicate_columns = metadata['job_parameter']['drop_duplicate_columns']
drop_duplicate_rows = metadata['job_parameter']['drop_duplicate_rows']
specialchars_to_replace = metadata['job_parameter']['specialchars_to_be_replaced_in_columnnames']
replacement_char = metadata['job_parameter']['replacement_char']
replace_non_printable_ascii = metadata['job_parameter']['replace_non_printable_ascii_with_underscore']
replace_non_alphanumeric_with_underscore = metadata['job_parameter']['replace_non_alphanumeric_with_underscore']
sampling_fraction = float(metadata['job_parameter']['sampling_fraction'])
sampling_seed = metadata['job_parameter']['sampling_seed']

if connector_file_path == '""':
    logger.info(f"raw_files_path remains the same: {raw_files_path}")
else:
    raw_files_path = connector_file_path
    logger.info(f"raw_files_path changed to connector_file_path: {raw_files_path}")

logger.info(f"bucket_name: {bucket_name}")
logger.info(f"raw_files_path: {raw_files_path}")
logger.info(f"parquet_files_path: {parquet_files_path}")
logger.info(f"drop_duplicate_columns: {drop_duplicate_columns}")
logger.info(f"drop_duplicate_rows: {drop_duplicate_rows}")
logger.info(f"specialchars_to_replace: {specialchars_to_replace}")
logger.info(f"replacement_char: {replacement_char}")
logger.info(f"replace_non_printable_ascii: {replace_non_printable_ascii}")
logger.info(f"replace_non_alphanumeric_with_underscore: {replace_non_alphanumeric_with_underscore}")
logger.info(f"sampling_fraction: {sampling_fraction}")
logger.info(f"sampling_seed: {sampling_seed}")

def find_matching_file_in_s3_bucket(filename, file_prefix=0, file_suffix=0):
    try:
        # Set up the S3 client
        s3 = boto3.client('s3', region_name=metadata["aws_region"])
    
        # Get the file extension
        file_extension = os.path.splitext(filename)[1]
    
        # Get the base filename without the suffix and extension
        base_filename = filename[file_prefix:-file_suffix - len(file_extension)]
        logger.info(f"Input file base {base_filename}")
    
        # List the files in the S3 directory
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_files_path)
    
        # Iterate over the files in the S3 directory
        for obj in response.get('Contents', []):
            s3_filename = obj['Key'].split('/')[-1]
            s3_extension = os.path.splitext(s3_filename)[1]
    
            # Check if the file extension matches
            if s3_extension == file_extension:
                # Exclude the suffix, prefix, and extension from the S3 filename
                s3_base_filename = s3_filename[file_prefix:-file_suffix - len(s3_extension)]
                logger.info(f"S3 file base {s3_base_filename}")
    
                # Compare the base filenames
                if s3_base_filename == base_filename:
                    logger.info(f"Match found: {s3_filename}")
                    return s3_filename
        return None            
    
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error occurred: {e}")
        return None
    except OSError as e:
        logger.error(f"OS error occurred: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return None


def archive_files_from_metadata(metadata, s3_helper):
    job_param = metadata.get("job_parameter", {})
    archive_config = job_param.get("archive", {})

    if not archive_config.get("enabled", False):
        print("Archive is disabled in metadata. Skipping archiving.")
        return

    archive_prefix = archive_config.get("archive_prefix", "archive/")
    include_timestamp = archive_config.get("include_timestamp_in_name", False)

    csv_files = job_param.get("csv_files", [])
    source_path = job_param.get("csv_files_path", "")

    csv_filename = csv_file['filename']
    file_prefix = csv_file.get('file_prefix_length', 0)
    file_suffix = csv_file.get('file_suffix_length', 0)
    csv_tablename = csv_file['tablename']
    is_fixed_file_name = csv_file.get('is_fixed_file_name', True)

    #Find matching file on S3 for a given input file in metadata config
    if not is_fixed_file_name:
        input_filename = csv_filename
        csv_filename = find_matching_file_in_s3_bucket(input_filename, file_prefix, file_suffix)
        if not csv_filename:
            logger.info(f"No matching file found for {input_filename}")
            return
        logger.info(f"find_matching_file_in_s3_bucket matched {csv_filename} for input {input_filename}")

    file_name = csv_filename
    s3_key = os.path.join(source_path, file_name)
    local_file = os.path.join("/tmp", file_name)

    try:
        # Download file from S3 to /tmp
        print(f"Downloading {s3_key} from bucket...")
        s3_helper.s3_client.download_file(s3_helper.bucket_name, s3_key, local_file)

        # Prepare archive name with timestamp if necessary
        if include_timestamp:
            base, ext = os.path.splitext(file_name)
            timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            archived_file_name = f"{base}_{timestamp}{ext}"
        else:
            archived_file_name = file_name

        # Set the archive key (path) in S3, ensuring the correct structure
        archive_key = archive_prefix  # Just the prefix as per metadata config

        print(f"Ready to upload to {archive_key}{archived_file_name}")

        # Prepare files to upload
        with open(local_file, 'rb') as file_data:
            files_to_upload = [(archived_file_name, file_data)]
            # Call the S3 helper to upload the file
            s3_helper.cps_upload_files(files_to_upload, archive_key)

        # Optionally delete the original file after archiving
        print(f"File archived successfully. Deleting original file from {s3_key}")
        s3_helper.s3_client.delete_object(Bucket=s3_helper.bucket_name, Key=s3_key)
        print(f"Deleted original file: {s3_key}")

    except Exception as e:
        print(f"Failed to archive file {file_name}: {e}")

#Function to convert CSV to Parquet 
def convert_csv_to_parquet(csv_file, s3_helper):
    csv_filename = csv_file['filename']
    file_prefix = csv_file.get('file_prefix_length', 0)
    file_suffix = csv_file.get('file_suffix_length', 0)
    csv_tablename = csv_file['tablename']
    is_fixed_file_name = csv_file.get('is_fixed_file_name', True)

    #Find matching file on S3 for a given input file in metadata config
    if not is_fixed_file_name:
        input_filename = csv_filename
        csv_filename = find_matching_file_in_s3_bucket(input_filename, file_prefix, file_suffix)
        if not csv_filename:
            logger.info(f"No matching file found for {input_filename}")
            return
        logger.info(f"find_matching_file_in_s3_bucket matched {csv_filename} for input {input_filename}")
    
    with_header = csv_file.get('with_header', True)
    separator = csv_file.get('separator', ',')
    quote_character = csv_file.get('quote_character', '"')
    multiline = csv_file.get('multiline', True)

    logger.info(f"CSV File Details - filename: {csv_filename}, tablename: {csv_tablename}, is_fixed_file_name: {is_fixed_file_name}")
    logger.info(f"with_header: {with_header}, separator: {separator}, quote_character: {quote_character}, multiline: {multiline}")
    
    if not csv_filename.lower().endswith(".csv"):
        logger.info(f"Skipping non CSV file {csv_filename}")
        return
        
    logger.info(f"Processing file {csv_filename}")
        
    # Extract filename from file_path
    #filename = csv_filename.split('/')[-1].replace(".csv", "")  # Extract filename without extension
        
    # Read the CSV file into a DataFrame
    datasource = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": quote_character, "withHeader": with_header, "separator": separator, "multiline": multiline, "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [f"s3://{bucket_name}/{raw_files_path}{csv_filename}"], "recurse": True}, transformation_ctx=function_name)
        
    try:
        logger.info(f"Schema of {csv_filename}")
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
            output_path = f"s3://{bucket_name}/{parquet_files_path}{csv_tablename}.parquet/{partition_str}"
                
            # Convert the DataFrame to Parquet format
            # Write partitioned data to S3
            partitioned_s3_key = f"{parquet_files_path}{csv_tablename}.parquet/{partition_str}"
            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                datasource,
                bucket_name,
                partitioned_s3_key,
                function_name + 'parquet',
                csv_or_xlsx_datasource=True,
                special_chars=specialchars_to_replace,
                replacement_chars=replacement_char,
                replace_non_ascii=replace_non_printable_ascii,
                replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
            )
                
            logger.info(f"Parquet file has been written to the location {output_path}")

            # Write sample data to S3 without partitioning
            sample_data = (
                datasource
                .toDF()
                .sample(withReplacement=False, fraction=0.5, seed=42)
            )  # Adjust the fraction as needed

            sample_data = sample_data.repartition(1)

            logger.info(f"Selected sample data {csv_filename}")
            #sample_s3_key = f"{sample_data_location}/{parquet_files_path}{csv_tablename}/"
            sample_s3_key = f"{sample_data_location}/{csv_tablename}/"

            ctx = function_name + "parquet" + "sampledata"

            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                bucket_name,
                sample_s3_key,
                ctx,
                csv_or_xlsx_datasource=True,
                special_chars=specialchars_to_replace,
                replacement_chars=replacement_char,
                replace_non_ascii=replace_non_printable_ascii,
                replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
            )

            logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {csv_filename}")
        
            logger.info(f"File Started the archive if flag set true")
            archive_files_from_metadata(metadata, s3_helper)
        else:
            logger.error(f"Input CSV file {csv_filename} is empty, skipping Parquet conversion, but trying to process rest of the files in the batch")
                
    except Exception as e:     
        # IMPORTANT : This happens in a scenario where the columns and rows but are absent. 0 rows and 0 columns.
        logger.error(f"Input CSV file {csv_filename} ran into exception {e}, skipping Parquet conversion, but trying to process rest of the files in the batch")

# Function to convert Excel to Parquet
def convert_xlsx_to_parquet(xls_filename, sheet, xls_engine_name, is_fixed_file_name, file_prefix, file_suffix):
    try:
        sheet_name = sheet['sheet_name']
        tablename = sheet['tablename']
        quote_character = sheet.get('quote_character', '"')
        with_headers = sheet.get('with_headers', True)
        separator = sheet.get('separator', ',')
        multiline = sheet.get('multiline', True)
        header_row = sheet.get('header_row', 0)
        header_column = sheet.get('header_column', 0)
        data_start_row = sheet.get('data_start_row', 0)
        data_start_column = sheet.get('data_start_column', 0)
        
        logger.info(f"Sheet Details - sheet_name: {sheet_name}, tablename: {tablename}")
        logger.info(f"quote_character: {quote_character}, with_headers: {with_headers}, separator: {separator}")
        logger.info(f"multiline: {multiline}, header_row: {header_row}, header_column: {header_column}")
        logger.info(f"data_start_row: {data_start_row}, data_start_column: {data_start_column}")
        logger.info(f"Processing Excel file: {raw_files_path}{xls_filename} and sheet {sheet_name}")
        
        #Find matching file on S3 for a given input file in metadata config
        if not is_fixed_file_name:
            input_filename = xls_filename
            xls_filename = find_matching_file_in_s3_bucket(input_filename, file_prefix, file_suffix)
            if not xls_filename:
                logger.info(f"No matching file found for {input_filename}")
                return
            logger.info(f"find_matching_file_in_s3_bucket matched {xls_filename} for input {input_filename}")

        df = read_excel_from_s3(bucket_name, f"{raw_files_path}{xls_filename}", sheet_name, header_row, xls_engine_name, metadata["aws_region"])
        
        if df.empty:
            logger.error(f"Pandas could not read {xls_filename} and sheet {sheet_name}")
            return
        
        filename = xls_filename.lower().replace(".xlsx", "")
        logger.info(f"Header row {header_row}")
        logger.info(f"Header col {header_column}")
        logger.info(df.columns.tolist())

        # Set data types for all columns to string
        df = df.astype(str)
        # Pass dataRow position to skip empty or filtered rows
        new_df = df.iloc[data_start_row:, data_start_column:]
        new_df.columns = df.columns[header_column:]

        # Drop rows and columns that are entirely empty
        new_df = new_df.dropna(how='all', axis=0)
        new_df = new_df.dropna(how='all', axis=1)

        # Write DataFrame to CSV
        csv_buffer = BytesIO()
        new_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_file_path = f"{csv_files_path}{sheet_name}/{filename}.csv"
    
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
            output_path = f"s3://{bucket_name}/{parquet_files_path}{tablename}.parquet/{partition_str}"
            # Write partitioned data to S3
            partitioned_s3_key = f"{parquet_files_path}{tablename}.parquet/{partition_str}"
            
            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                datasource,
                bucket_name,
                partitioned_s3_key,
                function_name + 'parquet',
                csv_or_xlsx_datasource=True,
                special_chars=specialchars_to_replace,
                replacement_chars=replacement_char,
                replace_non_ascii=replace_non_printable_ascii,
                replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
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
    
                sample_s3_key = f"{sample_data_location}/{tablename}/"
                ctx = function_name + "parquet" + "sampledata"
    
                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                    bucket_name,
                    sample_s3_key,
                    ctx,
                    csv_or_xlsx_datasource=True,
                    special_chars=specialchars_to_replace,
                    replacement_chars=replacement_char,
                    replace_non_ascii=replace_non_printable_ascii,
                    replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
                )

                if success:
                    logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {filename}")
                else:
                    logger.error(f"Failed to write sample data for {filename}")
            else:
                logger.error(f"Failed to write Parquet file for {filename}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    
    
# Parse csv_files if present
if 'csv_files' in metadata['job_parameter']:
    csv_files = metadata['job_parameter']['csv_files']
    destination_bucket = metadata['job_parameter']['bucket_name']
    s3_key = metadata['job_parameter']['csv_files_path']
    kms_key_arn = metadata['job_parameter']['kms_key_arn']
    archive = metadata['job_parameter']['archive']
    s3_helper = S3Helper(destination_bucket, s3_key,kms_key_arn)

    for csv_file in csv_files:
        convert_csv_to_parquet(csv_file, s3_helper)
      

# Parse xls_files if present
if 'xls_files' in metadata['job_parameter']:
    xls_files = metadata['job_parameter']['xls_files']
    
    for xls_file in xls_files:
        xls_filename = xls_file['filename']
        xls_engine_name = xls_file['xls_engine_name']
        is_fixed_file_name = xls_file.get('is_fixed_file_name', True)
        file_prefix = xls_file.get('file_prefix_length', 0)
        file_suffix = xls_file.get('file_suffix_length', 0)
        logger.info(f"XLS File Details - filename: {xls_filename}, is_fixed_file_name: {is_fixed_file_name}")

        if 'sheets' in xls_file:
            sheets = xls_file['sheets']
            for sheet in sheets:
                # Error status is not verified. If one xlsx has issues, the code will continue processing other files
                # However, this logic can be modified to exit out if even one file encounters an issue.
                status = convert_xlsx_to_parquet(xls_filename, sheet, xls_engine_name, is_fixed_file_name, file_prefix, file_suffix)
                
                
job.commit()
