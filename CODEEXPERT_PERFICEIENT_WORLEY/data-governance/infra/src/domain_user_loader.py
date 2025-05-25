import pandas as pd
from helpers import lakeformation,utilities,redshiftClient
import os
import time

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()
red_client = redshiftClient()


def lambda_handler(event, context): 
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    domain = key.split('/')[1]

    if utils.validate_domain(domain):
      red_client.create_domain_auth_lookup_table(domain)
      red_client.import_s3_file(
          database='global_standard_reporting',
          bucket=bucket,
          key=key,
          schema=domain,
          region='ap-southeast-2'
          
      )

    

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
          "key": "domains/construction/User_Access_Controls_1728623997.4305542.csv",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}
# s3://worley-mf-sydney-dev-external-user-store/domains/engineering/User_Access_Controls_1728374751.593101.csv
    lambda_handler(event, None)
    
