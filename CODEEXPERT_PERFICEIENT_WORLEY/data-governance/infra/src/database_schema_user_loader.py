from helpers import utilities, redshiftClient,RedshiftServerlessClient

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()
red_client = redshiftClient()
red_client_serverless = RedshiftServerlessClient()


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    database = key.split('/')[0]
    schema = key.split('/')[3]

    if utils.validate_schema(database_name=database,schema_name=schema):
        red_client_serverless.import_s3_file(
            database=database,
            bucket=bucket,
            key=key,
            schema=schema,
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

                        "key": "global_standard_reporting/processed/aws/document_control/User_Access_Controls_aws_1740355181.7271595.csv",
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

