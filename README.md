
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
