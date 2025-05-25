import pandas as pd
from helpers import lakeformation,utilities
import os
import time

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()

temp_dir = '/tmp'
landing_dir = f"{temp_dir}/landing" 

def download_file_from_s3(bucket,key):
    # s3://worley-mf-sydney-dev-external-user-store/landing/User_Access_Controls.xlsx
    s3 = session.client('s3')
    create_landing_dir(landing_dir)
    s3.download_file(bucket,key,f"{temp_dir}/{key}")


def upload_file_to_s3(file, bucket, key):
    s3 = session.client('s3')
    s3.upload_file(file, bucket, key)

def create_landing_dir(dir):
    try:
      os.mkdir(dir)
      logger.info("Directory created successfully")
    except FileExistsError:
        logger.info("Directory already exists")
    except PermissionError:
        logger.info("Permission denied to create directory")
    except Exception as e:
        logger.info(f"An error occurred: {e}")

def run(bucket,key):
    file = key
    read_file = pd.read_excel (file) 

    read_file.to_csv (f"{landing_dir}/User_Access_Controls.csv",  
                  index = None, 
                  header=True) 
    
    df = pd.DataFrame(pd.read_csv(f"{landing_dir}/User_Access_Controls.csv"))

    unique_domains = df['Domain'].unique()

    for domain in unique_domains:
      if utils.validate_domain(domain):
        spilt_domain(df, domain,bucket)


def spilt_domain(df,domain,bucket):
    

    domain_df = df.loc[df['Domain'] == domain,:] 

    selected_columns = domain_df.loc[:, ['Project_ID', 'User_Name']]  
    selected_columns.loc[:, 'User_Name'] = 'AWSIDC:' + selected_columns['User_Name']

    directory = f"{landing_dir}/{domain.lower()}"
    os.makedirs(directory, exist_ok=True)

    time_stamp = time.time()

    outfile = f"User_Access_Controls_{time_stamp}.csv"

    selected_columns.to_csv (f"{directory}/{outfile}",  
                index = None, 
                header=True)
    
    upload_file_to_s3(file=f"{landing_dir}/{domain.lower()}/{outfile}",
                      bucket=bucket,
                      key=f"domain/{domain.lower()}/{outfile}"
                      )


def lambda_handler(event, context): 
    logger.info(f'got event{event}')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    download_file_from_s3(bucket, key)
    run(bucket,f"{temp_dir}/{key}")

    

if __name__ == "__main__":
    event = {
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "2023-04-15T20:30:12.456Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "worley-mf-sydney-dev-external-user-store",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "landing/User_Access_Controls_14_10.xlsx",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}

    lambda_handler(event, None)
    