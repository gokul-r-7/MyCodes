import time

import pandas as pd

from helpers import utilities

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()

temp_dir = '/tmp'
landing_dir = f"{temp_dir}/landing" 

def download_file_from_s3(bucket,key):
    # s3://worley-mf-sydney-dev-external-user-store/landing/User_Access_Controls.xlsx
    s3 = session.client('s3')
    folders = key.split('/')
    file_path = '/'.join(folders[:-1])
    create_child_folders(temp_dir,file_path)
    s3.download_file(bucket,key,f"{temp_dir}/{key}")
    return file_path


def upload_file_to_s3(file, bucket, key):
    s3 = session.client('s3')
    s3.upload_file(file, bucket, key)


import os
from typing import List


def create_child_folders(parent_dir: str, file_path: str) -> None:
    """
    Create child folders under the specified parent directory.

    Args:
        parent_dir (str): Parent directory path
        folder_list (List[str]): List of folder names to create
    """
    try:
        # Ensure parent directory exists
        os.makedirs(parent_dir, exist_ok=True)
        path = os.path.join(parent_dir, file_path)
        os.makedirs(path, exist_ok=True)

    except PermissionError:
        logger.error(f"Permission denied to create directories in {parent_dir}")
    except Exception as e:
        logger.error(f"Error creating directories: {str(e)}")


def create_landing_dir(dir: object) -> object:
    try:
      os.mkdir(dir)
      logger.info(f"Directory {dir} created successfully")
    except FileExistsError:
        logger.info(f"Directory {dir} already exists")
    except PermissionError:
        logger.info("Permission denied to create directory")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def run(bucket,key,file_path):
    file = key
    read_file = pd.read_excel (file) 

    read_file.to_csv (
        f"{temp_dir}/{file_path}/User_Access_Controls.csv",
        index = None,
        header=True
    )
    
    df = pd.DataFrame(pd.read_csv(f"{temp_dir}/{file_path}/User_Access_Controls.csv"))

    unique_domains = df['Domain'].unique()

    for domain in unique_domains:
      if utils.validate_domain(domain):
        spilt_domain(df, domain,bucket,file_path)


def spilt_domain(df,domain,bucket,file_path):

    data_base = file_path.split('/')[0]
    identity = file_path.split('/')[-1]

    domain_df = df.loc[df['Domain'] == domain,:] 

    #### Read auth_table_mapping.json as per the database and domain i.e database/schema/auth_table_mapping.json
    #### Override the schema column with the columns defined in auth_table_mapping.json
    auth_table_mapping_data = utils.get_database_rls_schema(data_base,domain)
    user_column = auth_table_mapping_data['user_identifier']['column_name']
    project_id_column = auth_table_mapping_data['project_identifier']['column_name']
    selected_columns = domain_df.loc[:, ['Project_ID', 'User_Name']].rename(columns={'Project_ID':project_id_column,'User_Name':user_column})
    selected_columns.loc[:, user_column] = f'{identity.upper()}:' + selected_columns[user_column]

    directory = f"{landing_dir}/{domain.lower()}"
    os.makedirs(directory, exist_ok=True)

    time_stamp = time.time()

    outfile = f"User_Access_Controls_{identity}_{time_stamp}.csv"

    selected_columns.to_csv (f"{directory}/{outfile}",  
                index = None, 
                header=True)



    
    upload_file_to_s3(
        file=f"{landing_dir}/{domain.lower()}/{outfile}",
        bucket=bucket,
        key=f"{data_base}/processed/{identity}/{domain.lower()}/{outfile}"
    )


def lambda_handler(event, context): 
    logger.info(f'got event{event}')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    file_path = download_file_from_s3(bucket, key)
    run(bucket,f"{temp_dir}/{key}",file_path)

    

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
          "key": "global_standard_reporting/landing/azure/User_Access_Controls.xlsx",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}

    lambda_handler(event, None)
    
