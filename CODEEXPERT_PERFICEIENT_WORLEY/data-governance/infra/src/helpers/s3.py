from .utilities import utilities
import json

utils = utilities()
session = utils.get_aws_session()
# tag_config = utils.get_tag_config()
settings = utils.get_settings()
logger = utils.get_logger()


s3_client = session.client('s3')
lambda_client = session.client('lambda')

class S3:
    def __init__(self):
        pass
        # self.config = config
        # self.utilities = utilities(config)

    def create_database_structure(self):
        bucket = settings["external_user_store_bucket"]
        data_base_structure = utils.get_database_structure_config()

        for i in data_base_structure:
            logger.info(f"Creating s3 structure for {i['database_name']} ")
            s3_client.put_object(Bucket=bucket, Key=f"{i['database_name']}/processed/")
            
            s3_client.put_object(Bucket=bucket, Key=f"{i['database_name']}/landing/awsidc/")
            
            s3_client.put_object(Bucket=bucket, Key=f"{i['database_name']}/landing/aad/")
