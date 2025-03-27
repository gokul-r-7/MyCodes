https://github.com/amuley11/SFTPwithAWSLambda#sftpwithawslambda







Url: ftp://ftp.cox.com/
User id: _omniture
Password: 0mn!turE


import paramiko
import boto3
from io import BytesIO

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # SFTP server details
    sftp_host = 'ftp.cox.com'
    sftp_username = '_omniture'
    sftp_password = '0mn!turE'
    sftp_filepath = 'Cox HealthScore Card.xlsx'  # Path to the file on the SFTP server

    # S3 details
    bucket_name = 'your-s3-bucket-name'
    s3_key = 'CoxHealthScoreCard/Cox_HealthScore_Card.xlsx'  # Path in S3 where the file will be stored

    # Connect to SFTP server using paramiko
    transport = paramiko.Transport((sftp_host, 22))
    transport.connect(username=sftp_username, password=sftp_password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    file_data = sftp.open(sftp_filepath, 'rb').read()

    # Upload file to S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_data)

    # Close the SFTP connection
    sftp.close()
    transport.close()

    return {
        'statusCode': 200,
        'body': 'File uploaded successfully to S3'
    }





























import paramiko
import boto3
import os
from io import BytesIO

# SFTP and S3 configurations
SFTP_HOST = 'your_sftp_host'
SFTP_PORT = 22
SFTP_USER = 'your_sftp_user'
SFTP_PASSWORD = 'your_sftp_password'  # Or use a private key
SFTP_FILE_PATH = '/path/to/your/file.txt'

S3_BUCKET = 'your_s3_bucket_name'
S3_FILE_KEY = 'your/s3/path/file.txt'

# Initialize Glue context and S3 client
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client('s3')

def upload_to_s3(file_data, s3_bucket, s3_key):
    try:
        # Upload data to S3 using boto3
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_data)
        print(f"File uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")

def read_from_sftp():
    try:
        # Connect to the SFTP server
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

        # Open SFTP session
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Open the file on SFTP
        with sftp.open(SFTP_FILE_PATH, 'rb') as file_handle:
            file_data = file_handle.read()  # Read the file data

        # Close SFTP connection
        sftp.close()
        transport.close()

        return file_data
    except Exception as e:
        print(f"Error reading from SFTP: {str(e)}")
        return None

def main():
    # Read file from SFTP
    file_data = read_from_sftp()
    
    if file_data:
        # Upload file to S3
        upload_to_s3(file_data, S3_BUCKET, S3_FILE_KEY)

# Call the main function
main()
