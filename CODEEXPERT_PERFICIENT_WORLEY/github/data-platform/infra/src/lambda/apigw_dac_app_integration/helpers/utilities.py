import json
import logging
import boto3
import os



class utilities():
    def __init__(self):
        self.env = os.environ['ENV']
        #TODO - convert settings to yaml
        self.settings_file = 'settings.json'
        try:
            self.aws_profile = os.environ['PROFILE']
        except KeyError:
            self.aws_profile = None

    def get_env(self):
        return self.env

    def get_settings(self):
        return self.get_data(self.env, self.settings_file)

    def get_data(self, env, file):
        data = {}
        with open(f'configs/{env}/{file}', 'r') as file:
            data = json.load(file)
        return data
    
    def get_logger(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel("INFO")
        return logger
    
    def get_aws_session(self):
        if self.aws_profile is not None:
            return boto3.Session(profile_name=self.aws_profile)
        else:
            return boto3.Session()
 
 