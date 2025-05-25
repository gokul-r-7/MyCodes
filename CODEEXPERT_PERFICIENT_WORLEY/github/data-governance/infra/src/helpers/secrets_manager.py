from .utilities import utilities
import json

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()

secrets_manager_client = session.client('secretsmanager')

class SecretsManager:

    def __init__(self) -> None:
        pass

    def get_secret(self,secret_name):
        try:
            logger.info('Getting redshift credentials from secrets manager')
            get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
            secret = json.loads(get_secret_value_response["SecretString"])
            username = secret["username"]
            password = secret["password"]
            return username, password
        except secrets_manager_client.exceptions.ResourceNotFoundException:
            msg = f"The requested secret {secret_name} was not found."
            logger.info(msg)
            return msg
        except Exception as e:
            logger.error(f"An unknown error occurred: {str(e)}.")
            raise