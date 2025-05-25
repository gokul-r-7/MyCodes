import os
import sys
import json
import uuid
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from worley_helper.utils.aws import DynamoDB, SecretsManager
from worley_helper.utils.smb import SMBConnection, SMBDownloader, SMBFileListor, SMBArchiveHandler
from worley_helper.utils.helpers import S3Helper
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import REGION, TIMESTAMP_FORMAT, DATETIME_FORMAT
from worley_helper.configuration.config import SMBIngestConfiguration


# Initialize logger
logger = get_logger(__name__)

# Constants
TIMEZONE = timezone.utc

def fetch_job_config(source_name, metadata_table_name, function_name , environment, region_name):
    """Fetch job configuration from DynamoDB."""
    
    logger.info(f"Input arguments recieved are Source -{source_name}, function_name - {function_name}, env - {environment}")
    
    primary_key =  source_name
    input_key =  "batch#" + source_name + "#" + environment + "#" + function_name
    env = environment
    region_name = region_name
    
    logger.info("metadata_type -> " + input_key)
    logger.info("source_system_id -> " + primary_key)

    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=region_name)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=primary_key, metadata_type=input_key
    )

    logger.info(f"Metadata fetched: {metadata}")
    
    # Parse the metadata into the Pydantic model for validation and structure
    job_config = SMBIngestConfiguration(**metadata)

    return job_config

def get_smb_credentials(secret_name, region_name):
    """Retrieve SMB credentials from Secrets Manager."""
    sm = SecretsManager(default_region = region_name)
    secret = json.loads(sm.get_secret(secret_name))
    return secret.get("username"), secret.get("password")

def process_source(source, smb_host, username, password, s3_helper, partition_str, batch_run_start_time, function_name, job_config):
    """Process a single and multiple file source configuration."""
    try:
        # Extract the pattern(s) and source path
        patterns = source.get('pattern', [])
        remote_path = source.get('SourcePath', '')
        
        if isinstance(patterns, str):  
            patterns = [patterns] 

        all_files = []

        # Loop through each pattern and get files
        for pattern in patterns:
            logger.info(f"Looking for the files for pattern: {pattern}")
            smb_file_listor = SMBFileListor(
                smb_host, username, password, remote_path, pattern
            )
            files = smb_file_listor.list_files_recursive()  # Get files for the current pattern
            all_files.extend(files)  # Add to the overall list of files

        # Check if files are found
        if not all_files:
            logger.warning(f"No files found for the patterns: {patterns}")
            return []
            
        downloader = SMBDownloader(smb_host, username, password, remote_path)
        downloaded_files = downloader.download_bot_files(files) #downloader.download_files(files)
        logger.info(f"Downloaded files: {downloaded_files}")
        
        bucket_key = job_config.smb_configuration.destination_folder
        
        instance_name = f"instance_name={function_name}"
        s3_key = f"{bucket_key}/{instance_name}/{batch_run_start_time}/"
        s3_helper = S3Helper(job_config.smb_configuration.destination_bucket, s3_key, job_config.smb_configuration.kms_key_arn)
        upload_files = [(os.path.basename(file), open(file, "rb")) for file in downloaded_files]
        s3_helper.upload_files(upload_files)

        # Archive or delete files if flags are set
        for file_name in downloaded_files:
            archive_files(job_config, smb_host, username, password, file_name, source, job_config.smb_configuration.file_archive_flag, job_config.smb_configuration.file_delete_flag)

        return downloaded_files

    except Exception as e:
        logger.error(f"Error in process_source: {e}")
        raise e

def archive_files(job_config, host_url, username, password, file_name, source, file_archive_flag, file_delete_flag):
    """Archive or delete files if flags are set."""
    archive_folder = job_config.smb_configuration.archive_folder
    remote_folder = source.get("SourcePath")  # Extract remote path

    file_name = os.path.basename(file_name)  
    smb_handler = SMBArchiveHandler(remote_folder, archive_folder, host_url, username, password)

    if file_archive_flag:
        logger.info(f"Starting the Archiving in SMB Protocol for {file_name}")
        smb_handler.archive_and_delete_file(file_name, file_archive_flag=True, file_delete_flag=False)
        logger.info(f"Completed Archiving of {file_name} to {archive_folder}")

    if file_delete_flag:
        logger.info(f"Starting deletion of {file_name}")
        smb_handler.archive_and_delete_file(file_name, file_archive_flag=False, file_delete_flag=True)
        logger.info(f"Completed deletion of {file_name}")


def main():
    """Main function to execute the SMB ingestion job."""
    logger.info("SMB ingestion job started.")

    # Parse arguments
    args = getResolvedOptions(
        sys.argv, ["source_name", "function_name", "metadata_table_name", "environment", "batch_run_start_time", "region_name"]
    )
    source_name = args["source_name"]
    batch_run_start_time = args["batch_run_start_time"]
    function_name = args["function_name"]
    environment = args["environment"]
    metadata_table_name = args["metadata_table_name"]
    region_name = args["region_name"]

    logger.info(f"Job function name: {function_name}")

    # Fetch job configuration and validate using Pydantic model
    job_config = fetch_job_config(source_name, metadata_table_name, function_name , environment, region_name)

    # Get SMB credentials
    username, password = get_smb_credentials(job_config.smb_configuration.secret_credentials, region_name)
    partition_str = generate_timestamp_string('UTC').strftime(DATETIME_FORMAT)

    try:
        with SMBConnection(
            job_config.smb_configuration.host_url, username, password, job_config.smb_configuration.host_port
        ) as smb_conn:
            s3_helper = S3Helper(
                job_config.smb_configuration.destination_bucket, "", job_config.smb_configuration.kms_key_arn
            )
           
            # Process sources from metadata
            for source in job_config.smb_configuration.source:
                downloaded_files = process_source(
                source, 
                job_config.smb_configuration.host_url,  
                username,                               
                password,                               
                s3_helper, partition_str, batch_run_start_time, function_name, job_config
            )
        logger.info("SMB ingestion job completed successfully.")

    except Exception as e:
        logger.exception("Job failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()


