import sys
import uuid
import os
import shutil
import boto3
import pandas as pd
from io import BytesIO
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
from pyspark.sql.functions import lit, first
from awsglue.dynamicframe import DynamicFrame

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
    sys.argv, ["JOB_NAME", "source_name","instance_name","function_name", "environment", "metadata_table_name", "connector_file_path"]
)


# Source name can be "e3d, assurance, sharepoint, oraclegbs" etc.
source_name = args.get("source_name")

instance_name = args.get("instance_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
connector_file_path = args['connector_file_path']
environment = args.get("environment")
logger.info(f"Connector file path: {connector_file_path}")

# Init the job to use booksmarks
job.init(args["JOB_NAME"] + source_name + function_name + metadata_table_name, args)
logger.info(f"function name: {function_name} environment: {environment} source name: {source_name} ")
# Define the Sort Keys for DynamoDB Fetch
primary_key =  source_name
input_keys = "csv_xlsx#" + source_name + "#" + environment + "#" + function_name

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=primary_key, metadata_type=input_keys
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


def find_matching_files_in_s3_bucket(filename, file_prefix=0, file_suffix=0):
    try:
        # Set up the S3 client
        s3 = boto3.client('s3', region_name=metadata["aws_region"])  # Replace with actual AWS region
    
        # Extract filename components
        file_extension = os.path.splitext(filename)[1]
        base_filename = filename.replace(file_extension, "")  # Remove extension
        logger.info(f"Looking for files matching: {base_filename} with extension {file_extension}")

        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_files_path)

        if "Contents" not in response:
            logger.warning("No files found in the S3 bucket.")
            return []

        matching_files = []

        # Iterate over files in S3
        for obj in response["Contents"]:
            s3_filename = obj["Key"].split("/")[-1]  # Extract actual file name
            s3_extension = os.path.splitext(s3_filename)[1]

            # Check if the file extension matches and the filename contains "SupplierReport"
            if s3_extension == file_extension and ("SupplierReport" in s3_filename or "PackageReport" in s3_filename):
                logger.info(f"Match found: {s3_filename}")
                matching_files.append(s3_filename)

        if matching_files:
            return matching_files
        else:
            logger.info("No matching files found.")
            return []

    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error occurred: {e}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return []

def convert_csv_to_parquet(csv_file):
    csv_filename = csv_file['filename']
    file_prefix = csv_file.get('file_prefix_length', 0)
    file_suffix = csv_file.get('file_suffix_length', 0)
    csv_tablename = csv_file['tablename']
    is_fixed_file_name = csv_file.get('is_fixed_file_name', True)

    # Find matching files on S3 for a given input file in metadata config
    if not is_fixed_file_name:
        input_filename = csv_filename
        csv_filename = find_matching_files_in_s3_bucket(input_filename, file_prefix=0, file_suffix=0)

        if not csv_filename:
            logger.info(f"No matching files found for {input_filename}")
            return

        logger.info(f"find_matching_file_in_s3_bucket matched {csv_filename} for input {input_filename}")

    with_header = csv_file.get('with_header', True)
    separator = csv_file.get('separator', ',')
    quote_character = csv_file.get('quote_character', '"')
    multiline = csv_file.get('multiline', True)

    logger.info(f"CSV File Details - tablename: {csv_tablename}, is_fixed_file_name: {is_fixed_file_name}")
    logger.info(f"with_header: {with_header}, separator: {separator}, quote_character: {quote_character}, multiline: {multiline}")
    
    for file in csv_filename:
        logger.info(f"Processing file {file}")
        # Read the CSV file into a DataFrame
        datasource = glueContext.create_dynamic_frame.from_options(
            format_options={
                "quoteChar": quote_character,
                "withHeader": with_header,
                "separator": separator,
                "multiline": multiline,
                "optimizePerformance": False
            },
            connection_type="s3",
            format="csv",
            connection_options={"paths": [f"s3://{bucket_name}/{raw_files_path}{file}"], "recurse": True},
            transformation_ctx=function_name
        )
        record_count = datasource.count()
        logger.info(f"Number of records in {file}: {record_count}")
        
        # Converted Glue DF to SparkDF
        df = datasource.toDF()

        #Columns to be selected
        columns_to_select=["Project ID","Project Number", "Project Code"]

        #Columns available
        available_columns = [col for col in columns_to_select if col in df.columns]

        first_row = df.select(*available_columns).first()
        
        if first_row:
            for col in available_columns:
                if col in first_row.asDict():
                    value = first_row[col]
                    df = df.withColumn(col, lit(value))
            datasource = DynamicFrame.fromDF(df, glueContext)

        try:
            logger.info(f"Schema of {file}")
            logger.info(datasource.printSchema())

            if record_count > 0:
                partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
                partition_str = f"{get_partition_str_mi(partition_date)}"
                output_path = f"s3://{bucket_name}/{parquet_files_path}{csv_tablename}.parquet/Instance={instance_name}/{partition_str}"

                partitioned_s3_key = f"{parquet_files_path}{csv_tablename}.parquet/Instance={instance_name}/{partition_str}"
                
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

                logger.info(f"Parquet file written to {output_path}")

                # Sample Data Write
                sample_data = (
                    datasource.toDF().sample(withReplacement=False, fraction=0.5, seed=42)
                ).repartition(1)

                sample_s3_key = f"{sample_data_location}/{csv_tablename}/"

                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                    bucket_name,
                    sample_s3_key,
                    function_name + "parquet" + "sampledata",
                    csv_or_xlsx_datasource=True,
                    special_chars=specialchars_to_replace,
                    replacement_chars=replacement_char,
                    replace_non_ascii=replace_non_printable_ascii,
                    replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
                )

                logger.info(f"Sample data written to {sample_s3_key}")

        except Exception as e:
            logger.error(f"Error processing {file}: {e}")


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
        
        if not is_fixed_file_name:
            input_filename = xls_filename
            xls_filename = find_matching_files_in_s3_bucket(input_filename, file_prefix, file_suffix)
            if not xls_filename:
                logger.info(f"No matching file found for {input_filename}")
                return
            logger.info(f"find_matching_file_in_s3_bucket matched {xls_filename} for input {input_filename}")

        df = read_excel_from_s3(bucket_name, f"{raw_files_path}{xls_filename}", sheet_name, header_row, xls_engine_name, metadata["aws_region"])
        
        if df.empty:
            logger.error(f"Pandas could not read {xls_filename} and sheet {sheet_name}")
            return
        
        df = df.astype(str)
        new_df = df.iloc[data_start_row:, data_start_column:]
        new_df.columns = df.columns[header_column:]
        new_df = new_df.dropna(how='all', axis=0)
        new_df = new_df.dropna(how='all', axis=1)
        
        first_row = new_df.iloc[0]
        for col in new_df.columns:
            new_df[col] = new_df[col].fillna(first_row[col])
        
        csv_buffer = BytesIO()
        new_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_file_path = f"{csv_files_path}{sheet_name}/{xls_filename.lower().replace('.xlsx', '')}.csv"
    
        s3 = boto3.client('s3', region_name=metadata["aws_region"])
        s3.put_object(Bucket=bucket_name, Key=csv_file_path, Body=csv_buffer.getvalue())
        
        datasource = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": quote_character, "withHeader": with_headers, "separator": separator, "multiline": multiline, "optimizePerformance": False},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [f"s3://{bucket_name}/{csv_file_path}"], "recurse": True},
            transformation_ctx="read_csv"
        )
        
        if datasource.count() > 0:
            logger.info(f"Number of rows in datasource: {datasource.count()}")
            
            # Converted Glue DF to SparkDF
            df = datasource.toDF()
            first_row = df.select("Project Number", "Project Code").first()
            
            if first_row:
                project_number_value = first_row["Project Number"]
                project_code_value = first_row["Project Code"]
                
                # Fill missing values with the first row's values
                df = df.withColumn("Project Number", lit(project_number_value))
                df = df.withColumn("Project Code", lit(project_code_value))
                
                datasource = DynamicFrame.fromDF(df, glueContext)
            
            partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
            partition_str = f"{get_partition_str_mi(partition_date)}"
            output_path = f"s3://{bucket_name}/{parquet_files_path}{tablename}.parquet/{partition_str}"
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
                logger.info(f"Parquet file has been written to {output_path}")
    
                sample_data = (
                    datasource.toDF().sample(withReplacement=False, fraction=0.5, seed=42)
                ).repartition(1)
    
                sample_s3_key = f"{sample_data_location}/{tablename}/"
    
                success = write_glue_df_to_s3_with_specific_file_name(
                    glueContext,
                    DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                    bucket_name,
                    sample_s3_key,
                    function_name + "parquet" + "sampledata",
                    csv_or_xlsx_datasource=True,
                    special_chars=specialchars_to_replace,
                    replacement_chars=replacement_char,
                    replace_non_ascii=replace_non_printable_ascii,
                    replace_non_alphanumeric_with_underscore=replace_non_alphanumeric_with_underscore
                )

                if success:
                    logger.info(f"Sample data written to {sample_s3_key}")
                else:
                    logger.error(f"Failed to write sample data for {xls_filename}")
            else:
                logger.error(f"Failed to write Parquet file for {xls_filename}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    
    
# Parse csv_files if present
if 'csv_files' in metadata['job_parameter']:
    csv_files = metadata['job_parameter']['csv_files']
    for csv_file in csv_files:
        convert_csv_to_parquet(csv_file)
        

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
