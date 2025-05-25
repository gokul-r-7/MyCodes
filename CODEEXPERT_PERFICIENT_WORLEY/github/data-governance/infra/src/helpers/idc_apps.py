import boto3
import logging

from .utilities import utilities
from .identitystore import IdentityStore
from .quicksight import quicksight


utils = utilities()
session = utils.get_aws_session()
domain_config = utils.get_tag_config()
settings = utils.get_settings()
logger = utils.get_logger()
identity_store = IdentityStore()
quicksight = quicksight()
all_rs_perms_config = utils.get_all_redshift_permissions_config()

sso_client = session.client('sso-admin')
identity_store_client = session.client('identitystore')


def add_group_to_redshift_app(ad_grp_name):

    # get PrincipalId id by group name
    group_id = identity_store.get_group_id(ad_grp_name)

    if group_id is not None:

        response = sso_client.create_application_assignment(
            ApplicationArn=settings['redshift_idc_application_arn'],
            PrincipalId=group_id,
            PrincipalType='GROUP'
        )

        return response

def add_group_to_redshift_serverless_app(ad_grp_name):

    # get PrincipalId id by group name
    group_id = identity_store.get_group_id(ad_grp_name)

    if group_id is not None:

        response = sso_client.create_application_assignment(
            ApplicationArn=settings['redshift_serverless_idc_application_arn'],
            PrincipalId=group_id,
            PrincipalType='GROUP'
        )

        return response

def add_ad_groups_to_redshift_app():
    logger.info('Adding AD groups to Redshift application')
    domains = domain_config['domain']

    for domain in domains:
        logger.info(f"Adding AD groups for {domain['name']}")
        for i in domain['roles']:
            if "redshift" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to redshift idc app")
                add_group_to_redshift_app(i['role_name'])

def add_ad_groups_to_redshift_serverless_app():
    logger.info('Adding AD groups to Redshift Serverless application')
    distinct_role_names = set()
    
    for rs_perms_config in all_rs_perms_config:
        for db in rs_perms_config:
            roles = db['ad_roles']
            for role in roles:
                if role['identity_provider'] == "AWSIDC":
                    role_name = role['role_name']
                    logger.info(f"Adding {role_name} to list of AWSIDC roles")
                    distinct_role_names.add(role_name) 
                    
    distinct_aws_roles = list(distinct_role_names)

    for role in distinct_aws_roles:
        logger.info(f"Adding AD group - {role}")
        add_group_to_redshift_serverless_app(role)

def add_ad_groups_to_quicksight():
    logger.info('Adding AD groups to quicksight application')
    domains = domain_config['domain']

    for domain in domains:
        logger.info(f"Adding AD groups for {domain['name']}")
        for i in domain['roles']:
            if "quicksight" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to quicksight idc app")
                quicksight.create_role_membership(i['role_name'], "READER")
            elif "quicksight_author" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to quicksight Author group")
                quicksight.create_role_membership(i['role_name'], "AUTHOR")
            
            elif "quicksight_reader_pro" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to quicksight READER_PRO group")
                quicksight.create_role_membership(i['role_name'], "READER_PRO")
        
            elif "quicksight_author_pro" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to quicksight AUTHOR_PRO group")
                quicksight.create_role_membership(i['role_name'], "AUTHOR_PRO")

def add_serverless_ad_groups_to_quicksight():
    
    for rs_perms_config in all_rs_perms_config:
        for db in rs_perms_config:
            roles = db['ad_roles']
            for role in roles:
                if role['identity_provider'] == "AWSIDC":
                    if 'quicksight_role' in role:
                        logger.info('Adding AD groups to quicksight application')
                        quicksight_role = role['quicksight_role']
                        role_name = role['role_name']
                        if quicksight_role == "quicksight":
                            logger.info(f"Adding {role_name} to quicksight idc app")
                            quicksight.create_role_membership(role_name, "READER")
                        elif quicksight_role == "quicksight_author":
                            logger.info(f"Adding {role_name} to quicksight idc app")
                            quicksight.create_role_membership(role_name, "AUTHOR")
                        elif quicksight_role == "quicksight_reader_pro":
                            logger.info(f"Adding {role_name} to quicksight idc app")
                            quicksight.create_role_membership(role_name, "READER_PRO")
                        elif quicksight_role == "quicksight_author_pro":
                            logger.info(f"Adding {role_name} to quicksight idc app")
                            quicksight.create_role_membership(role_name, "AUTHOR_PRO")         
    

if __name__ == "__main__":
    add_ad_groups_to_redshift_app()
