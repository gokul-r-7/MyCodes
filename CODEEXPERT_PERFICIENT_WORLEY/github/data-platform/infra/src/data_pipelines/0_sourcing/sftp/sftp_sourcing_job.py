import boto3
import paramiko
import json
import io
import re
import os
import sys
import zipfile
from io import BytesIO
from datetime import datetime
import pyspark
import pytz
import string
from botocore.exceptions import ClientError
import pyspark.sql.functions as F
import stat
import glob
import csv

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


from worley_helper.utils.logger import get_logger
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, TIMESTAMP_FORMAT_WITH_UTC
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.helpers import get_partition_str_mi, standardize_columns, write_glue_df_to_s3_with_specific_file_name, S3Helper




FILE_PATTERN = r'^.*\.zip$'  #  regex for specific pattern
s3_client = boto3.client('s3')

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.sql('set spark.sql.caseSensitive=true')

# # Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["source_name", "metadata_table_name", "function_name", "environment", "bucket_key"]
)

#remote_paths_list = json.loads(remote_paths)

# Init the logger
logger = get_logger(__name__)

# Define the Sort Keys for DynamoDB Fetch
source_name = args.get("source_name")
metadata_table_name = args.get("metadata_table_name")
function_name = args.get("function_name")
bucket_key = args.get("bucket_key")
env = args.get("environment")
               
input_keys = "batch#" + source_name + "#" + env + "#" + function_name
primary_key = source_name + "_" + function_name

logger.info("metadata_type -> " + input_keys)
logger.info("source_system_id -> " + primary_key)
logger.info("bucket_key -> " + bucket_key)

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=primary_key, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")
export_config = metadata

############## Load sftp configs ################
#Job parameters
archival_folder = export_config['Ingest']['archival_folder']
archive_flag = export_config['Ingest']['archive_flag']

destination_bucket = export_config['Ingest']['destination_bucket']
delete_flag = export_config['Ingest']['delete_flag']
# sampling_fraction = float(
#     export_config['job_parameter']['sampling_fraction'])
remote_folder = export_config['Ingest']['remote_folder']
temp_dir = export_config['Ingest']['temp_dir']
zip_unzip_flag = export_config['Ingest']['zip_unzip_flag']
patterns = export_config['Ingest']['patterns']
compiled_patterns = [re.compile(pattern) for pattern in patterns]
Source = export_config['Ingest']['Source']
txt_file_flag = export_config['Ingest']['txt_file_flag']
txt_file_delimiter = export_config['Ingest']['txt_file_delimiter']

os.makedirs(temp_dir, exist_ok=True)

#Metadata Parameter check
logger.info("archival_folder -> " + archival_folder)
logger.info("archive_flag -> " + archive_flag)
logger.info("destination_bucket -> " + destination_bucket)
logger.info("delete_flag -> " + delete_flag)
logger.info("remote_folder -> " + remote_folder)
logger.info("temp_dir -> " + temp_dir)
logger.info("zip_unzip_flag -> " + zip_unzip_flag)


def get_sftp_credentials(sftp_secret_name):
    """Get SFTP connection credentials from AWS Secrets Manager."""
    secretsmanager = boto3.client("secretsmanager")
    try:
        get_secret_value_response = secretsmanager.get_secret_value(SecretId=sftp_secret_name)
    except secretsmanager.exceptions.ResourceNotFoundException:
        logger.info(f"The requested secret {sftp_secret_name} was not found.")
        raise
    except Exception as e:
        logger.info(f"An unknown error occurred: {str(e)}.")
        raise

    return json.loads(get_secret_value_response["SecretString"])

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return secret
    except Exception as e:
        logger.info(f"Error retrieving secret: {e}")
        return None

def validate_credentials(username, credential):
    """Validate the provided username and credential (private key or password)."""
    if not username or not isinstance(username, str):
        logger.info("Invalid username.")
        return False

    if not credential:
        logger.info("Credential is empty.")
        return False

    return True
    
def sftp_conn_operations(hostname, port, username, private_key=None, password=None):
    """Establish an SFTP connection and process files."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if private_key:
            private_key_obj = paramiko.RSAKey.from_private_key(io.StringIO(private_key))
            ssh.connect(hostname, port=port, username=username, pkey=private_key_obj)
            logger.info("SSH connection established using private key.")
        elif password:
            ssh.connect(hostname, port=port, username=username, password=password)
            logger.info("SSH connection established using password.")
        else:
            logger.info("No valid authentication method provided.")
            return
        
        # Open an SFTP session
        sftp = ssh.open_sftp()
        logger.info("SFTP session opened.")
        # multiple folder iteration
        
        partition_date = generate_timestamp_string(
            timezone=TIMEZONE_SYDNEY
        ).strftime(DATETIME_FORMAT)
        all_s3_keys = {}
        
        partition_str = f"{get_partition_str_mi(partition_date)}"
         # Loop through each source folder configuration
        for source in Source:
            pattern = source['pattern']
            s3_path = source['s3Path']
            
            logger.info(s3_path)
            source_folder = source['SourcePath']
            #s3_key = s3_path + "/" + partition_str
            s3_key = bucket_key #Get the bucket folder and partition str from DAG argument.
            logger.info(s3_key)
            logger.info(source_folder)
            s3_helper = S3Helper(export_config["Ingest"]["destination_bucket"], s3_key, export_config["Ingest"]["kms_key_arn"])
            sftp.chdir(source_folder)
            
            # Loop through multiple files/directory within the specified folder
            for filename in sftp.listdir():
                logger.info("Checking for files...")
                file_path = os.path.join(source_folder, filename)
                local_file_path = os.path.join('/tmp', filename)
                
                try:
                
                    file_stat = sftp.stat(file_path)
                    if stat.S_ISDIR(file_stat.st_mode):  # Check if the path is a directory and skip it. 
                        logger.info(f"Skipping directory: {filename}")
                        continue  # Skip directories
                        
                    # Check if the filename matches the provided pattern
                    logger.info(f"Checking for file: {filename}")
                    logger.info(f"local file path is {local_file_path}")
                    if pattern:
                        if glob.fnmatch.fnmatch(filename, pattern):
                            logger.info(f"Found file: {filename}")
                            logger.info(f"Local file path: {local_file_path}")
                            sftp.get(file_path, local_file_path)  # Download the file to the local path
                            logger.info(f"Downloaded {filename} successfully.")
                            logger.info(f"Downloaded file_path {file_path} to local_file_path {local_file_path}")
                            
                        else:
                            logger.info(f"Skipping file (does not match pattern): {filename}")
                            continue
                            
                except Exception as e:
                    logger.info(f"Error processing {filename}: {e}")
                    continue  # Skip to the next file if there's an error

                         
                # Check if zip_unzip_flag is enabled and unzip and upload to S3
                if zip_unzip_flag == 'Y':
                    
                    extracted_files = []
                    try:
                        
                        with zipfile.ZipFile(local_file_path, 'r') as local_zip_file:
                            local_zip_file.extractall(temp_dir)
                            extracted_files = [os.path.join(temp_dir, file) for file in local_zip_file.namelist()]
                            
                            logger.info(f"Extracted files: {extracted_files}")
                    except zipfile.BadZipFile:
                        logger.error(f"Failed to unzip {local_file_path}: Not a valid zip file.")
                        return  # Exit the function if the file is not a valid zip        
                    
                    # Upload extracted files to S3
                    for extracted_file in extracted_files:
                        file_name = os.path.basename(extracted_file)
                            
                        #Upload file to S3
                        s3_helper.upload_file(extracted_file,file_name)
                        logger.info(f"Uploaded {file_name} to S3 {s3_key}")
                        
                    # Archive the extracted files to SFTP if required
                    if archive_flag == 'Y':
                        sftp_arch_path = f"{archival_folder}/{os.path.basename(local_file_path)}"
                        sftp.put(local_file_path, sftp_arch_path)
                        logger.info(f"Archived {local_file_path} to SFTP {sftp_arch_path}")

                    # Remove the local extracted file from S3 after upload
                    os.remove(extracted_file)
                    logger.info(f"Removed local file {extracted_file}.")
                            
                # Check if txt_file_flag is enabled, then convert txt file to csv and upload to S3
                elif txt_file_flag =='Y':
                    csv_file_path = local_file_path.replace('.txt', '.csv')
                    
                    # Read the txt file from local
                    
                    try:
                    
                        with open(local_file_path, 'r') as txt_file:
                            lines = txt_file.readlines()
                        
                            # Open the CSV file in write mode
                            with open(csv_file_path, 'w', newline='') as csv_file:
                                csv_writer = csv.writer(csv_file)
                                for line in lines:
                                    row = line.strip().split(txt_file_delimiter) 
                                    csv_writer.writerow(row)  # Write the row to the CSV file
                                logger.info(f"Converted {local_file_path} to {csv_file_path}")
                                
                    except Exception as e:
                        logger.error(f"Error processing {local_file_path}: {e}")
                        return  # Exit the function if there was an error in reading or converting            
                        
                    # Upload the CSV file to S3
                    #s3_helper.upload_file(csv_file_path,filename.replace('.txt', '.csv'))
                    s3_helper.upload_file(csv_file_path, os.path.basename(csv_file_path))
                    logger.info(f"uploaded converted file {csv_file_path}  to s3 {s3_key}")
                        
                    # Archive the CSV file to SFTP if required
                    if archive_flag == 'Y':
                        logger.info("archive flag is Y")
                        sftp_arch_path = f"{archival_folder}/{os.path.basename(csv_file_path)}"
                        sftp.put(filename, sftp_arch_path)
                        logger.info(f"Archived {csv_file_path} to SFTP {sftp_arch_path}")
                            
                    # Remove the local files from S3 after processing
                    os.remove(local_file_path)
                    os.remove(csv_file_path)
                    logger.info(f"Removed local files {local_file_path} and {csv_file_path}.")
                    
                # Check if zip_unzip_flag and  txt_file_flag is disabled, download the file directly (for direct csv files) and upload to S3
                else:
                    # Create the S3 key with timestamp partitioning
                    
                    # Upload the file to S3
                    s3_helper.upload_file(local_file_path,filename)
                    logger.info(f"Uploaded {filename} to S3 {s3_key}")
                    
                    # Archive the CSV file to SFTP if required
                    if archive_flag == 'Y':
                        sftp_arch_path = f"{archival_folder}/{filename}"
                        sftp.put(filename, sftp_arch_path)
                        logger.info(f"Archived {filename} to SFTP {sftp_arch_path}")
                    
                    # Remove the local files from S3 after processing
                    os.remove(local_file_path)
                    logger.info(f"Removed local file {local_file_path}.")
                
                # Delete the source file from SFTP if delete_flag is set as 'Y'
                if delete_flag == 'Y':
                    sftp_file_path = f"{source_folder}/{filename}"
                    # Delete the file
                    sftp.remove(sftp_file_path)
                    logger.info(f"Deleted {sftp_file_path} successfully.")
                all_s3_keys[s3_path] = s3_key
                    


        
       


    except paramiko.AuthenticationException:
        logger.info("Authentication failed, please verify your credentials.")
    except paramiko.SSHException as e:
        logger.info(f"Could not establish SSH connection: {e}")
    except Exception as e:
        logger.info(f"An error occurred: {e}")
    finally:
        # Ensure that the SFTP session and SSH connection are closed
        try:
            if 'sftp' in locals():
                sftp.close()
                logger.info("SFTP session closed.")
            ssh.close()
            logger.info("SSH connection closed.")
        except Exception as e:
            logger.info(f"Error closing connections: {e}")


        

def main():
    """Main function to execute the SFTP process."""
    sftp_secret_name = export_config['Ingest']['sftp_secret_name']
    credentials = get_sftp_credentials(sftp_secret_name)
    hostname = credentials.get("hostname")
    username = credentials.get("username")
    password = credentials.get("password")
    
    # Get the secret key for private key authentication, if it exists
    secret_key = export_config['Ingest'].get('secret_key')
    
    if secret_key:
        private_key = get_secret(secret_key)  # Get the private key if secret_key is defined
    else:
        private_key = None  # No private key if not defined
    
    if not username:
        logger.info("Username is missing from the credentials.")
        return  # Return early if username is missing
    
    if private_key:
        try:
            # If private key is available, use it for authentication
            logger.info("Attempting to authenticate using private key.")
            sftp_conn_operations(
                hostname=hostname,
                port=22,
                username=username,
                private_key=private_key  # Private key-based authentication
            )
        except Exception as e:
            logger.error(f"Error during SFTP connection with private key: {e}")
            return  # Return if there's an error with private key authentication
    elif password:
        # If no private key, then use username/password based authentication
        logger.info("Trying to authenticate using password.")
        sftp_conn_operations(
            hostname=hostname,
            port=22,
            username=username,
            password=password
        )
    else:
        logger.info("Neither private key nor password is provided for authentication.Please provide either of them in metdata file")
        return  

if __name__ == "__main__":
    main()
