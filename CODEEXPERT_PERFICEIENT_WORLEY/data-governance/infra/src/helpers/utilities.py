import json
import logging
import boto3
import os
import yaml


class utilities():
    def __init__(self):
        self.env = os.environ['ENVIRONMENT']
        #TODO - convert settings to yaml
        self.settings_file = 'settings.json'
        self.auth_table_mapping = 'auth_table_mapping.json'
        self.governance_config = 'governance_config.json'
        self.lake_permissions = 'lake_permissions.yml'
        self.database_structure_config = 'database_structure.yml'
        #self.rs_perms_config = 'redshift_permissions.yml'
        try:
            self.aws_profile = os.environ['PROFILE']
        except KeyError:
            self.aws_profile = None

    def get_env(self):
        return self.env

    def get_file(self):
        return self.file

    def get_settings(self):
        return self.get_data(self.env, self.settings_file)

    def get_tag_config(self):
        config = self.get_data(self.env, self.governance_config)
        return config

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
    
    def validate_domain(self,domain):
        config = self.get_tag_config()
        domains = config['domain']

        for i in domains:
            if i['name'].lower() == domain.lower():
                logging.info(f"Domain {domain} is valid")
                return True
                
        logging.error(f"Domain {domain} is not a valid domain, check the data governance config")
        return False

    def validate_schema(self,database_name, schema_name):
        config = self.get_database_structure_config()

        for i in config:
            if i['database_name'].lower() == database_name:
                for schema in i['product_schema']:
                    if schema.lower() == schema_name.lower():
                        logging.info(f"Schema {schema_name} is valid")
                        return True
                logging.error(f"Schema {schema_name} is not a valid schemas list, check the config")
                return False
        logging.error(f"Database {database_name} is not a valid database, check the config")
        return False

    
    def get_data_yaml(self,env,file=None,db=None):
        data = {}
        
        if db:
            file_path = f'configs/{env}/redshift_permissions/{db}.yml'
        elif file:
            file_path = f'configs/{env}/{file}'
        else:
            raise ValueError('Either file or db must be provided')
        
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
        
        return data

    def get_database_structure_config(self):
        db_structure_config = self.get_data_yaml(self.env, self.database_structure_config)
        return db_structure_config['databases_configs']

    def get_redshift_permissions_config(self,db):
        rs_perms_config = self.get_data_yaml(self.env, db=db)
        return rs_perms_config['redshift_permissions_config']
    
    def get_all_redshift_permissions_config(self):
        configs = []
        
        folder = f'configs/{self.env}/redshift_permissions/'
        
        for filename in os.listdir(folder):
            if filename.endswith('.yml'):
                file_path = os.path.join(folder, filename)
                with open(file_path, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    config = yaml_data['redshift_permissions_config']
                    configs.append(config)           
        
        for config in configs:
            self.validate_identity_provider(config)
        
        return configs
        
    def get_database_rls_schema(self,db,schema):
        data = {}
        with open(f'policies/{db.lower()}/{schema.lower()}/{self.auth_table_mapping}', 'r') as file:
            data = json.load(file)
        return data
    
    def validate_identity_provider(self,config):
        allowed_identity_provider = ["AWSIDC", "AAD", "internal"]
        for perms_config in config:
            db = perms_config['database_name']
            users = perms_config['users']
            ad_roles = perms_config['ad_roles']
            for user in users:
                if user['identity_provider'] not in allowed_identity_provider:
                    raise ValueError(f"Invalid identity provider for {user['user_name']} from database {db}. Must be one of {allowed_identity_provider}")
            for ad_role in ad_roles:
                if ad_role['identity_provider'] not in allowed_identity_provider:
                    raise ValueError(f"Invalid identity provider for {ad_role['role_name']} from database {db}. Must be one of {allowed_identity_provider}")

    def get_lakeformation_permissions(self):
        version = os.environ.get("RBAC_VERSION")
        if version == 'v1':
            logging.info(f"RBAC Version: {version} and getting values from {self.governance_config}")
            config = self.get_data_yaml(self.env, file=self.governance_config)
        else:
            logging.info(f"RBAC Version: {version} and getting values from {self.lake_permissions}")
            config = self.get_data_yaml(self.env, file=self.lake_permissions)

        return config