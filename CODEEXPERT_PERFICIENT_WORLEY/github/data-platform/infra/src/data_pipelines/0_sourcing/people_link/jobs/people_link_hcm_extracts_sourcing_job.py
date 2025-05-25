import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3
from io import BytesIO
import zipfile
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import traceback

from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import DynamoDB
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.helpers import get_partition_str_mi, standardize_columns, extract_column_names, add_masked_columns


# Init the logger
logger = get_logger(__name__)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize S3 client
s3_client = boto3.client('s3')

################################################
# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_name", "function_name", "metadata_table_name", "masking_metadata_table_name", "database_name"]
)

#source_name should be "csv"
source_name = args.get("source_name", "people")
#Function name can be "OCI, S3" etc.
function_name = args.get("function_name", "hcm_extracts")
metadata_table_name = args.get("metadata_table_name")
database_name = args.get("database_name")
masking_metadata_table_name = args.get("masking_metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]

# Init the job to use booksmarks
job.init(args["JOB_NAME"] + source_name + function_name + metadata_table_name, args)

# Define the Sort Keys for DynamoDB Fetch
input_keys = source_name + "#" + function_name

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")


# Access and print values from metadata
bucket_name = metadata['job_parameter']['bucket_name']
raw_files_path = metadata['job_parameter']['raw_files_path']        # This is the place where .zip files from oci will be stored
parquet_files_path = metadata['job_parameter']['parquet_files_path']    # This si the place where you can find table level parquet files
csv_files_path = metadata['job_parameter']['csv_files_path']        # This is the place where the zipped file will be extracted and placed in this location
sample_data_location = metadata['job_parameter']['sample_data_location']
drop_duplicate_columns = metadata['job_parameter']['drop_duplicate_columns']
drop_duplicate_rows = metadata['job_parameter']['drop_duplicate_rows']
specialchars_to_replace = metadata['job_parameter']['specialchars_to_be_replaced_in_columnnames']
replacement_char = metadata['job_parameter']['replacement_char']
replace_non_printable_ascii = metadata['job_parameter']['replace_non_printable_ascii_with_underscore']
replace_non_alphanumeric_with_underscore = metadata['job_parameter']['replace_non_alphanumeric_with_underscore']
sampling_fraction = float(metadata['job_parameter']['sampling_fraction'])
sampling_seed = metadata['job_parameter']['sampling_seed']
pii_protected_table_names = metadata['job_parameter']['pii_protected_table_names']


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
logger.info(f"pii_protected_table_names: {pii_protected_table_names}")


ddb1 = DynamoDB(metadata_table_name=masking_metadata_table_name,default_region=REGION)
masking_metadata = ddb1.get_masking_metadata_from_ddb(
    databasename=database_name.lower()
)

logger.info(f"Masking Metadata Response :{masking_metadata}") 


def configure_spark_session():
    """Configure Spark session with optimized settings"""
    spark.conf.set("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024)  # 128MB per partition
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000000)  # Max records per file
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def read_csv_from_temp_directory(glueContext, temp_path, csv_metadata_info):
    """
    Read CSV files from temporary directory using Glue Dynamic Frame
    """
    try:
        # Create Dynamic Frame from options
        dynamic_frame = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options={
                "paths": [temp_path],
                "recurse": True
            },
            format="csv",
            format_options={
                "withHeader": csv_metadata_info.get('with_header', True),
                "separator": csv_metadata_info.get('separator', ','),
                "quote": csv_metadata_info.get('quote_character', '"'),
                "multiline": csv_metadata_info.get('multiline', True),
                # "optimizePerformance": True,
                "escape": "\\",
                "dateFormat": "yyyy-MM-dd",
                "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
            }
        )
        
        # Convert to DataFrame for easier manipulation
        df = dynamic_frame.toDF()
        return df
    
    except Exception as e:
        logger.error(f"Error reading CSV from temp directory {temp_path}: {str(e)}")
        raise


def get_zip_files(bucket, s3_key, file_prefixes):
    """Get list of zip files from S3 location"""
    zip_files = []

    for prefix in file_prefixes:
        # List the files in the S3 directory
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_key + "/" + prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.zip'):
                    zip_files.append(obj['Key'])
                else:
                    logger.info(f"File {obj['Key']} is not a zip file")
        else:
            logger.info(f"No files found in the S3 directory {s3_key} with prefix {prefix}")
    
    return zip_files


def delete_s3_prefix(s3_client, bucket, prefix):
    """Delete all objects under a prefix in S3"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    except Exception as e:
        logger.warning(f"Error cleaning up intermediate files: {str(e)}")


def move_to_archive(s3_client, source_bucket, source_key, log_com_element, archive_bucket=None, archive_prefix=None):
    """
    Move file to archive location in S3
    
    Args:
        s3_client: boto3 s3 client
        source_bucket: Source bucket name
        source_key: Source file key
        archive_bucket: Archive bucket name (if None, uses source_bucket)
        archive_prefix: Archive prefix path (if None, uses 'archive/')
    """
    try:
        # If archive bucket not specified, use source bucket
        archive_bucket = archive_bucket or source_bucket
        
        # If archive prefix not specified, use default
        archive_prefix = archive_prefix or 'archive/'
        
        # Generate archive key with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = os.path.basename(source_key)
        archive_key = f"{archive_prefix}{timestamp}/{file_name}"
        
        logger.info(f"filename - {log_com_element}, Moving file {source_key} to archive location {archive_key}")
        
        # Copy object to archive location
        s3_client.copy_object(
            Bucket=archive_bucket,
            Key=archive_key,
            CopySource={
                'Bucket': source_bucket,
                'Key': source_key
            }
        )
        
        # Delete original file
        s3_client.delete_object(
            Bucket=source_bucket,
            Key=source_key
        )
        
        logger.info(f"filename - {log_com_element}, Successfully archived file to: s3://{archive_bucket}/{archive_key}")
        return archive_key
        
    except Exception as e:
        logger.error(f"filename - {log_com_element}, Error archiving file {source_key}: {str(e)}")
        raise



def process_zip_file(bucket, zip_key, output_prefix, csv_files_info):
    """Process individual zip file"""
    try:
        # Extract table name from zip file name
        zipped_file_prefix = os.path.splitext(os.path.basename(zip_key))[0].split('-')[0]
        logger.info(f"processing file - {zipped_file_prefix}")

        csv_metadata_info = csv_files_info.get(zipped_file_prefix)
        table_name = csv_metadata_info.get('tablename')
        logger.info(f"filename - {zipped_file_prefix}, {table_name=}")
        
        logger.info(f"filename - {zipped_file_prefix}, Started Downloading the file from S3 location - {bucket=}, {zip_key}")
        # Download zip file
        zip_obj = s3_client.get_object(Bucket=bucket, Key=zip_key)

        logger.info(f"filename - {zipped_file_prefix}, Downloaded the file")

        zip_content = BytesIO(zip_obj['Body'].read())
        
        logger.info(f"filename - {zipped_file_prefix}, Extracting the .zip file in memory")

        files_in_tmp_folder = 0
        # Extract and upload CSV contents directly to S3
        with zipfile.ZipFile(zip_content) as zip_ref:
            for file_name in zip_ref.namelist():
                logger.info(f"filename - {zipped_file_prefix}, Extracted files are - {file_name}")
                if file_name.lower().endswith('.csv'):
                    files_in_tmp_folder+=1
                    with zip_ref.open(file_name) as csv_file:
                        # Upload CSV directly to intermediate S3 location
                        s3_client.put_object(
                            Bucket=bucket,
                            Key=f"{csv_files_path}/{table_name}/{file_name}",
                            Body=csv_file.read()
                        )
                else:
                    logger.info(f"filename - {zipped_file_prefix}, Skipping non-csv file - {file_name}")

        logger.info(f"filename - {zipped_file_prefix}, Extracted successfully. And uploaded to intermediate location - {csv_files_path}")


        staging_file_path = f"s3://{bucket}/{csv_files_path}/{table_name}/"

        if files_in_tmp_folder > 0:

            # Read the CSV files using Glue Dynamic Frame
            
            df = read_csv_from_temp_directory(glueContext, staging_file_path, csv_metadata_info)

            logger.info(f"filename - {zipped_file_prefix}, Successfully read the files - {files_in_tmp_folder}")
            logger.info(f"filename - {zipped_file_prefix},  Row Count - {df.count()}")
            logger.info(f"filename - {zipped_file_prefix},  Schema - {df.printSchema()}")
            

            if metadata["job_parameter"].get('drop_duplicate_rows', False):
                df = df.dropDuplicates()
                logger.info(f"filename - {zipped_file_prefix}, Dropped duplicate rows, Row Count - {df.count()}")

            # standardize columns
            current_columns = df.columns
            logger.info(f"filename - {zipped_file_prefix}, Current column names - {current_columns}")

            # Rename the columns
            renamed_columns = [standardize_columns(col) for col in current_columns]
            logger.info(f"filename - {zipped_file_prefix}, standardized column names - {renamed_columns}")

            # Create a new DataFrame with renamed columns
            _dyf = df.select(
                [
                    F.col("`{}`".format(c)).alias(renamed_columns[i])
                    for i, c in enumerate(current_columns)
                ]
            )

            # Masking PII info

            if table_name in pii_protected_table_names:

                columns_to_hash = extract_column_names(masking_metadata, table_name)
                print(columns_to_hash)
                
                # # Apply the function to hash columns
                _dyf = add_masked_columns(_dyf, columns_to_hash)
                logger.info(f"filename - {zipped_file_prefix},  After Masked Schema - {_dyf.printSchema()}")

            
            # Define output path for parquet
            # Define the output path for Parquet file
            partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
                DATETIME_FORMAT
            )
            partition_str = f"{get_partition_str_mi(partition_date)}"

            output_path = f"s3://{bucket}/{output_prefix}/{table_name}.parquet/{partition_str}"

            logger.info(f"filename - {zipped_file_prefix}, Outputpath for parquet files - {output_path}")

            glueContext.write_dynamic_frame.from_options(
                frame= DynamicFrame.fromDF(_dyf, glueContext, f"{zipped_file_prefix}_data"),
                connection_type="s3",
                connection_options={
                    "path": output_path
                },
                format="parquet",
                format_options={
                    "compression": "snappy"
                },
                transformation_ctx=f"{zipped_file_prefix}_main_data"

            )

            logger.info(f"filename - {zipped_file_prefix}, Successfully converted {files_in_tmp_folder} to Parquet at {output_path}")

            # data sampling

            # Write sample data to S3 without partitioning
            sample_data = _dyf.sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
            # Adjust the fraction as needed
            sample_data = sample_data.repartition(1)

            logger.info(f"filename - {zipped_file_prefix}, Selected sample data of {zipped_file_prefix}")

            sample_s3_key = f"s3://{bucket}/{sample_data_location}/{table_name}/"

            glueContext.write_dynamic_frame.from_options(
                frame= DynamicFrame.fromDF(sample_data, glueContext, f"{zipped_file_prefix}_sampling_data"),
                connection_type="s3",
                connection_options={
                    "path": sample_s3_key
                },
                format="parquet",
                format_options={
                    "compression": "snappy"
                },
                transformation_ctx=f"{zipped_file_prefix}_sample_data"

            )

            logger.info(f"filename - {zipped_file_prefix}, Successfully generated sample of {zipped_file_prefix} to sampling data at {sample_s3_key}")
        else:
            logger.info(f"filename - {zipped_file_prefix}, As it contains no records skipping it.")
        
        
        # Clean up intermediate S3 location
        delete_s3_prefix(s3_client, bucket, csv_files_path + "/" + table_name)

        logger.info(f"filename - {zipped_file_prefix}, Successfully cleaned up the temporary files at loc - {staging_file_path}")
        
        # Optionally delete source zip file


        # After successful processing, archive the zip file
        if csv_metadata_info.get('archive_source_file', True):
            archive_prefix = metadata['job_parameter'].get('archive_raw_files_path', 'people_link/hcm_extracts/archive')
            archived_key = move_to_archive(
                s3_client=s3_client,
                source_bucket=bucket,
                source_key=zip_key,
                archive_prefix=archive_prefix + "/" + table_name + "/",
                log_com_element=zipped_file_prefix
            )
            logger.info(f"filename - {zipped_file_prefix}, Archived source file from {zip_key} to {archived_key}")
        
        # If you want to delete instead of archive
        elif csv_metadata_info.get('delete_source_file', False):
            s3_client.delete_object(Bucket=bucket, Key=zip_key)
            logger.info(f"filename - {zipped_file_prefix}, Successfully deleted the zip file from S3 location - {zip_key}")
        else:
            logger.info(f"filename - {zipped_file_prefix}, As both delete and archive is set to False kept the file at raw {zip_key}")


        logger.info(f"filename - {zipped_file_prefix}, Successfully processed {table_name}")

        return f"Successfully processed {table_name}"
        
    except Exception as e:
        return f"Error processing {zip_key}: {str(e)}"


def main():

    try:

        configure_spark_session()

        untree_csv_file_info = {csv_file_info["zipped_file_name"]: csv_file_info for csv_file_info in metadata["job_parameter"]["csv_files"]}
        
        # Get list of zip files
        zip_files = get_zip_files(bucket=bucket_name, s3_key=raw_files_path, file_prefixes=list(untree_csv_file_info.keys()))
        
        # Process zip files in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    process_zip_file,
                    bucket_name,
                    zip_key,
                    parquet_files_path,
                    untree_csv_file_info
                )
                for zip_key in zip_files
            ]
            
            # Wait for all tasks to complete and collect results
            for future in futures:
                logger.info(future.result())
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        logger.info("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        error_message = f"Error: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        raise

if __name__ == "__main__":
    main()