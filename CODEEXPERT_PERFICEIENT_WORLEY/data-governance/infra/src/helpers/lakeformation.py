
from .utilities import utilities
from .identitystore import IdentityStore
from .dynamodb import dynamodb

utils = utilities()
session = utils.get_aws_session()
tag_config = utils.get_lakeformation_permissions()
settings = utils.get_settings()
logger = utils.get_logger()



lf_client = session.client('lakeformation')
service_roles = settings["service_roles"]

class lakeformation:
    # Class attributes go here
    class_attribute = "This is a class attribute"

    # The __init__ method is the constructor
    def __init__(self):
        self.instance_attribute = "This is an instance attribute"
        # self.instance_attribute = instance_attribute

    def create_lakeformation_tags(self,tags):

        lf_client.create_lf_tag(
            TagKey=tags['TagKey'],
            TagValues=tags['TagValues']
        )
        logger.info(f"Created tags: {tags['TagKey']} with values {tags['TagValues']}")


    def delete_lakeformation_tags(self, tags):
        lf_client.delete_lf_tag(
            CatalogId=f'{settings["aws_account_id"]}',
            TagKey=tags['TagKey']
        )
        logger.info(f"Removed tag: {tags['TagKey']}")

    def add_remove_compare_tags(self):
        current_state = self.get_all_available_tags_frm_lf()
        desired_state = self.get_tags_from_config()
        logger.info(f"Current State ::: {current_state}")
        logger.info(f"Desired State ::: {desired_state}")

        current_keys = set(tag['TagKey'] for tag in current_state)
        desired_keys = set(tag['TagKey'] for tag in desired_state)

        tags_to_associate = [tag for tag in desired_state]

        if current_keys == desired_keys:
            for i in desired_state:
               for j in current_state:
                    if i['TagKey'] == j['TagKey']:
                        logger.info(f"comparing {i['TagKey']} tag")
                        self.compare_tag_values(i['TagKey'],i['TagValues'], j['TagValues'])
            # Add code to compare tag values here
        else:
            # Delete tags that are present in current state but not in desired state
            tags_to_delete = [tag for tag in current_state if tag['TagKey'] not in desired_keys]
            for tag in tags_to_delete:
                self.delete_lakeformation_tags(tag)

            # Create tags that are present in desired state but not in current state
            tags_to_create = [tag for tag in desired_state if tag['TagKey'] not in current_keys]
            for tag in tags_to_create:
                self.create_lakeformation_tags(tag)

        for tag in tags_to_associate:
            self.set_associate_tag_permissions(
                role_arn=service_roles['lakeformation_lambda_role'],
                tag=tag
            )

    def compare_tag_values(self,tag_key,desired_tag_vals,current_tag_vals):
        if sorted(desired_tag_vals) == sorted(current_tag_vals):
            logger.info("Tag values are the same")
        else:
            values_to_add = []
            values_to_remove = []
            for i in desired_tag_vals:
                if i not in current_tag_vals:
                    values_to_add.append(i)
                    logger.info(f"Adding {i} to {tag_key}")

            for i in current_tag_vals:
                if i not in desired_tag_vals:
                    values_to_remove.append(i)
                    logger.info(f"Removing {i} from {tag_key}")

            self.update_lakeformation_tags(tag_key, values_to_add,values_to_remove)


    def update_lakeformation_tags(self, tag_key, values_to_add,values_to_remove):

        if len(values_to_add) <= 0:
            lf_client.update_lf_tag(
                CatalogId=f'{settings["aws_account_id"]}',
                TagKey=tag_key,
                TagValuesToDelete=values_to_remove
            )
        elif len(values_to_remove) <= 0:
            lf_client.update_lf_tag(
                CatalogId=f'{settings["aws_account_id"]}',
                TagKey=tag_key,
                TagValuesToAdd=values_to_add
            )
        else:
            lf_client.update_lf_tag(
                CatalogId=f'{settings["aws_account_id"]}',
                TagKey=tag_key,
                TagValuesToDelete=values_to_remove,
                TagValuesToAdd=values_to_add
            )

    def get_all_available_tags_frm_lf(self):
        #TODO Refactor this 
        
        lf_tags = []
        response = lf_client.list_lf_tags(
            CatalogId=f'{settings["aws_account_id"]}',
            ResourceShareType='ALL',
            MaxResults=100
        )

        if 'LFTags' in response:
            for tag in response['LFTags']:
                tag_obj = {
                    'TagKey': tag['TagKey'],
                    'TagValues': tag['TagValues']
                }
                
                lf_tags.append(tag_obj)

        if 'NextToken' in response:
            next_token = response['NextToken']
            while next_token:
                response = lf_client.list_lf_tags(
                    CatalogId=f'{settings["aws_account_id"]}',
                    ResourceShareType='ALL',
                    MaxResults=100,
                    NextToken=next_token
                )
                if 'LFTags' in response:
                    for tag in response['LFTags']:
                        tag_obj = {
                        'TagKey': tag['TagKey'],
                        'TagValues': tag['TagValues']
                        }
                        lf_tags.append(tag_obj)

                if 'NextToken' in response:
                    next_token = response['NextToken']
                else:
                    break

            return lf_tags

    def get_tags_from_config(self):
        tag_keys = tag_config.keys()
        lf_tags = []
        for i in tag_keys:
            tag_values = []
            for j in tag_config[i]:
                tag_values.append(j['name'])
            tag_obj = {
                'TagKey': i,
                'TagValues': tag_values
            }
        
            lf_tags.append(tag_obj)
        return lf_tags

    def grant_table_permissions(self,role_id,key,value):
        
        logger.info(f"Granting table permissions to {role_id} for {key}={value}")

        res = lf_client.grant_permissions(
            CatalogId=f'{settings["aws_account_id"]}',
            Principal={'DataLakePrincipalIdentifier': f'arn:aws:identitystore:::group/{role_id}'},
            Resource= {
                'LFTagPolicy': {
                    "ResourceType": "TABLE",
                    'CatalogId': f'{settings["aws_account_id"]}',
                    'Expression': [
                        {
                            'TagKey': key,
                            'TagValues': [value]
                        }
                    ]    
                }
            },
            Permissions=['SELECT'],
        )

    def grant_database_permissions(self, role_id, key, value):

        logger.info(f"Granting database permissions to {role_id} for {key}={value}")
        
        res = lf_client.grant_permissions(
            CatalogId=f'{settings["aws_account_id"]}',
            Principal={'DataLakePrincipalIdentifier': f'arn:aws:identitystore:::group/{role_id}'},
            Resource= {
                'LFTagPolicy': {
                    "ResourceType": "DATABASE",
                    'CatalogId': f'{settings["aws_account_id"]}',
                    'Expression': [
                        {
                            'TagKey': key,
                            'TagValues': [value]
                        }
                    ]
                }
            },
            Permissions=['DESCRIBE']
        )

    def get_tag_value_role_mapping(self):
        tag_value_role_mapping_obj_for_grants = []
        tag_value_role_mapping_obj_for_revokes = []
        for tag in tag_config:
            for i in tag_config[tag]:
                granted_roles = []
                revoked_roles = []
                for x in i['roles']:
                    if "role_name" in x:
                        try:
                            if x['active']:
                                logger.info(f"Adding {x['role_name']} to the grant list")
                                granted_roles.append(x['role_name'])
                            else:
                                logger.info(f"Adding {x['role_name']} to the revoke list")
                                revoked_roles.append(x['role_name'])
                        except KeyError:
                            logger.info(f"No active key found, so adding {x['role_name']} to the grant list")
                            granted_roles.append(x['role_name'])
                    else:
                        logger.warning(f"role_name not found in {x}")

                tag_value_role_mapping_obj_for_grants.append ({
                    'TagKey': tag,
                    'TagValue': i['name'],
                    'Roles': granted_roles
                })

                tag_value_role_mapping_obj_for_revokes.append ({
                    'TagKey': tag,
                    'TagValue': i['name'],
                    'Roles': revoked_roles
                })


        return tag_value_role_mapping_obj_for_grants,tag_value_role_mapping_obj_for_revokes

    def manage_tag_permissions(self):
        grants,revokes = self.get_tag_value_role_mapping()
        identity_store = IdentityStore()
        for role_map in grants:
            for role in role_map['Roles']:
                logger.info(f"Granting permissions to {role}")
                role_arn = identity_store.get_group_id(role)
                if role_arn is not None:
                    self.grant_table_permissions(role_arn, role_map['TagKey'], role_map['TagValue'])
                    self.grant_database_permissions(role_arn, role_map['TagKey'], role_map['TagValue'])
        
        for role_map in revokes:
            for role in role_map['Roles']:
                logger.info(f"Revoking permissions to {role}")
                role_arn = identity_store.get_group_id(role)
                if role_arn is not None:
                    self.revoke_database_permissions(role_arn, role_map['TagKey'], role_map['TagValue'])
                    

    
    def revoke_database_permissions(self, role_id, key, value):

        logger.info(f"Revoke database permissions to {role_id} for {key}={value}")
        try:
            res = lf_client.revoke_permissions(
                CatalogId=f'{settings["aws_account_id"]}',
                Principal={'DataLakePrincipalIdentifier': f'arn:aws:identitystore:::group/{role_id}'},
                Resource= {
                    'LFTagPolicy': {
                        "ResourceType": "DATABASE",
                        'CatalogId': f'{settings["aws_account_id"]}',
                        'Expression': [
                            {
                                'TagKey': key,
                                'TagValues': [value]
                            }
                        ]
                    }
                },
                Permissions=['DESCRIBE'],
            )
        except lf_client.exceptions.InvalidInputException as e:
            logger.warning(e)
            logger.warning(f"please remove the role {role_id} from the data governance config, as the permissions has been already revoked")


    def revoke_table_permissions(self,role_id,key,value):
        logger.info(f"Revoking table permissions to {role_id} for {key}={value}")

        res = lf_client.revoke_permissions(
            CatalogId=f'{settings["aws_account_id"]}',
            Principal={'DataLakePrincipalIdentifier': f'arn:aws:identitystore:::group/{role_id}'},
            Resource= {
                'LFTagPolicy': {
                    "ResourceType": "TABLE",
                    'CatalogId': f'{settings["aws_account_id"]}',
                    'Expression': [
                        {
                            'TagKey': key,
                            'TagValues': [value]
                        }
                    ]    
                }
            },
            Permissions=['SELECT'],
        )


    def grant_service_permissions_to_lf(self,database):

        #glue_role_arn = settings["glue_service_role"]

        # create a list with MWAA Role & Glue Role
        service_role_list = [service_roles["glue_service_role"], service_roles["mwaa_service_role"]]
        if "glue_notebook_service_role" in service_roles and service_roles["glue_notebook_service_role"] is not None and service_roles["glue_notebook_service_role"].strip():
            service_role_list.append(service_roles["glue_notebook_service_role"])

        for role_arn in service_role_list:
            logger.info(f"Granting database {database} permissions to {role_arn} role")
            try:
                res = lf_client.grant_permissions(
                    CatalogId=f'{settings["aws_account_id"]}',
                    Principal={'DataLakePrincipalIdentifier': f'{role_arn}'},
                    Resource= {
                        'Database': {
                            'CatalogId': f'{settings["aws_account_id"]}',
                            'Name': database
                    },
                    },
                    Permissions=['ALL']
                )
            except lf_client.exceptions.InvalidInputException as e:
                error_message = str(e)
                if "Database not found" in error_message:
                    logger.warning(f"Database not found error: {error_message}")
                else:
                    logger.error(f"Lake Formation error: {error_message}")
                    raise
    
    def grant_idc_role_permissions_to_lf(self,database):
        idc_role_arn = service_roles["redshift_iam_role"]

        logger.info(f"Granting database {database} permissions to {idc_role_arn} role")

        try:
            res = lf_client.grant_permissions(
                CatalogId=f'{settings["aws_account_id"]}',
                Principal={'DataLakePrincipalIdentifier': f'{idc_role_arn}'},
                Resource= {
                    'Database': {
                        'CatalogId': f'{settings["aws_account_id"]}',
                        'Name': database
                },
                },
                Permissions=['DESCRIBE']
            )

            res = lf_client.grant_permissions(
                CatalogId=f'{settings["aws_account_id"]}',
                Principal={'DataLakePrincipalIdentifier': f'{idc_role_arn}'},
                Resource= {
                    'Table': {
                        'CatalogId': f'{settings["aws_account_id"]}',
                        'DatabaseName': database,
                        'TableWildcard': {}
                    }
                },
                Permissions=['DESCRIBE','SELECT']
            )

        except lf_client.exceptions.InvalidInputException as e:
            error_message = str(e)
            if "Database not found" in error_message:
                logger.warning(f"Database not found error: {error_message}")
            else:
                logger.error(f"Lake Formation error: {error_message}")
                raise


    def grant_database_permissions_all(self):
        database_permission_store = settings["database_permissions_store"]
        all_dbs_settings = dynamodb().get_all_db_items(database_permission_store)
        for i in all_dbs_settings:
            logger.info(f"Granting database permissions to {i['database_name']} for domain {i['domain_name']}")
            
            self.grant_service_permissions_to_lf(i['database_name'])
            self.grant_idc_role_permissions_to_lf(i['database_name'])
            self.add_lf_tags_to_database(i['database_name'],"domain",i["domain_name"])
            
            if "curated" in i['database_name']:
                db_postfix = "curated"
            elif "raw" in i['database_name']:
                db_postfix = "raw"
            elif "transformed" in i['database_name']:
                db_postfix = "transformed"
            else:
                raise ValueError(f"Invalid database name {i['database_name']}, must contain curated, raw or transformed")
            
            if "table_definition" in i:

                for table in i['table_definition']:
                    logger.info(f"Applying CLS :: Table definition found for DB {i['database_name']} in table: {table['table_name']}")
                    col_names = []
                    for col in table['column_definitions']:
                        self.add_tf_tags_to_table_with_cols(
                            database_name=i['database_name'], 
                            table_name=f"{db_postfix}_{table['table_name']}", 
                            tag_key="data_classification",
                            tag_val=col['data_classification'], 
                            col_names=[col['column_name']]
                        )
                


    def add_lf_tags_to_database(self,database_name,tag_key,tag_val):
        logger.info(f"Adding LF tags to database {database_name}")
        try:
            database = lf_client.add_lf_tags_to_resource(
                CatalogId=f'{settings["aws_account_id"]}',
                Resource={
                    'Database': {
                        'Name': f'{database_name}',
                        'CatalogId': f'{settings["aws_account_id"]}'
                        }
                },
                LFTags=[
                    {
                        'TagKey': f'{tag_key}', 
                        'TagValues': [f'{tag_val}']
                    }
                ]
            )
        except lf_client.exceptions.InvalidInputException as e:
            error_message = str(e)
            if "Database not found" in error_message:
                logger.warning(f"Database not found error: {error_message}")
            else:
                logger.error(f"Lake Formation error: {error_message}")
                raise

    def add_tf_tags_to_table_with_cols(self,database_name,tag_key,tag_val,table_name,col_names):
        
        try:
            table = lf_client.add_lf_tags_to_resource(
                CatalogId=f'{settings["aws_account_id"]}',
                Resource={
                    'TableWithColumns': {
                    'CatalogId': f'{settings["aws_account_id"]}',
                    'DatabaseName': database_name,
                    'Name': table_name,
                    'ColumnNames': col_names
                    }
                },
                LFTags=[
                    {
                        'TagKey': f'{tag_key}', 
                        'TagValues': [f'{tag_val}']
                    }
                ]
            )
        except lf_client.exceptions.EntityNotFoundException as e:
            error_message = str(e)
            if "Table not found" in error_message:
                logger.warning(f"Table not found error: {error_message}")
            else:
                logger.error(f"Lake Formation error: {error_message}")
                raise
        except Exception as e:
            logger.error(e)
            raise

    def set_associate_tag_permissions(self,role_arn,tag):
        logger.info(f"Granting associate tag permissions to {role_arn} for {tag['TagKey']}={tag['TagValues']}")

        response = lf_client.grant_permissions(
            CatalogId=f'{settings["aws_account_id"]}',
            Principal={
                'DataLakePrincipalIdentifier': role_arn
            },
            Resource={
                'LFTag': {
                    'CatalogId': settings["aws_account_id"],
                    'TagKey': tag['TagKey'],
                    'TagValues': tag['TagValues']
                }
            },
            Permissions=[
                'ASSOCIATE',
            ]
        )

    def grant_iam_role_permissions_to_lf(self, database, iam_role, permissions):

        """
            Grants IAM role permissions to LF databased
            Permission Value should be SELECT / ALL / DROP / ALTER / CREATE
        """



        logger.info(f"Granting database {database} permissions to {iam_role} role")

        try:
            res = lf_client.grant_permissions(
                CatalogId=f'{settings["aws_account_id"]}',
                Principal={'DataLakePrincipalIdentifier': f'{iam_role}'},
                Resource={
                    'Database': {
                        'CatalogId': f'{settings["aws_account_id"]}',
                        'Name': database
                    },
                },
                Permissions=['DESCRIBE']
            )

            res = lf_client.grant_permissions(
                CatalogId=f'{settings["aws_account_id"]}',
                Principal={'DataLakePrincipalIdentifier': f'{iam_role}'},
                Resource={
                    'Table': {
                        'CatalogId': f'{settings["aws_account_id"]}',
                        'DatabaseName': database,
                        'TableWildcard': {}
                    }
                },
                Permissions=['DESCRIBE', f'{permissions.upper()}']
            )

        except lf_client.exceptions.InvalidInputException as e:
            error_message = str(e)
            if "Database not found" in error_message:
                logger.warning(f"Database not found error: {error_message}")
            else:
                logger.error(f"Lake Formation error: {error_message}")
                raise

    def grant_permissions_to_iam_roles(self):
        domain_config = tag_config['domain']

        for domain in domain_config:
            logger.info(f"Granting permissions to IAM roles for domain {domain['name']}")
            glue_databases = dynamodb().scan_by_domain(settings["database_permissions_store"], domain['name'])
            for db in glue_databases:
                for role in domain['roles']:
                    if all(key in role for key in ('lake_permissions', 'lf_role_arn','role_name')):
                        self.grant_iam_role_permissions_to_lf(db['database_name'], role['lf_role_arn'], role['lake_permissions'])

if __name__ == "__main__":
    lfadmin = lakeformation()
    lfadmin.grant_database_permissions_all()
    # lfadmin.manage_tag_permissions()
    # lakeformation.get_tags_from_config()
    # lakeformation.compare_tags()