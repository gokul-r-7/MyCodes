from .utilities import utilities

utils = utilities()
session = utils.get_aws_session()
tag_config = utils.get_tag_config()
settings = utils.get_settings()
logger = utils.get_logger()

qs_client = session.client('quicksight')

class quicksight:

    def create_role_membership(self,member_name,role):
        response = qs_client.create_role_membership(
            MemberName=member_name,
            AwsAccountId=settings['aws_account_id'],
            Namespace='default',
            Role=role
        )
        logger.info(f"Role membership created: {response}")
        return response