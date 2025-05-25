from .utilities import utilities
import json

utils = utilities()
session = utils.get_aws_session()
# tag_config = utils.get_tag_config()
settings = utils.get_settings()
logger = utils.get_logger()


sns_client = session.client('sns')

class notify:
    def __init__(self):
        pass
        # self.config = config
        # self.utilities = utilities(config)

    def send(self,topic_arn,subject,message):
        env = utils.get_env()
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=f'{env.upper()} - {subject}',
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure='json'
        )
        print(message)