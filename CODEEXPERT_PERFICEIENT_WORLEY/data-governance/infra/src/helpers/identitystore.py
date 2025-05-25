
from .utilities import utilities

utils = utilities()
domain_config = utils.get_tag_config()
settings = utils.get_settings()
logger = utils.get_logger()
session = utils.get_aws_session()


identity_store_client = session.client('identitystore')

class IdentityStore:
    
    def __init__(self) -> None:
        pass

    def get_group_id(self, ad_grp_name):

        try:
            response = identity_store_client.list_groups(
            IdentityStoreId=settings['identity_store_id'],
            Filters=[
                {
                    'AttributePath': 'DisplayName',
                    'AttributeValue': ad_grp_name
                },
            ],
            MaxResults=1
            )

            return (response['Groups'][0]['GroupId'])
        # return response
        except IndexError as e:
            logger.warning(f'an AD group call {ad_grp_name} is not AWS IDC, Please check with AD Team')

def add_group_to_redshift_app(ad_grp_name):

    # get PrincipalId id by group name
    response = identity_store_client.list_groups(
        IdentityStoreId=settings['identity_store_id'],
        Filters=[
            {
                'AttributePath': 'DisplayName',
                'AttributeValue': ad_grp_name
            },
        ],
        MaxResults=1
    )

    group_id = response['Groups'][0]['GroupId']

    response = sso_client.create_application_assignment(
        ApplicationArn=settings['redshift_idc_application_arn'],
        PrincipalId=group_id,
        PrincipalType='GROUP'
    )

    return response


def add_ad_groups_to_redshift_app():
    logger.info('Adding AD groups to Redshift application')

    for domain in domain_config:
        logger.info(f"Adding AD groups for {domain['name']}")
        for i in domain['roles']:
            if "redshift" in i['allowed_services']:
                logger.info(f"Adding {i['role_name']} to redshift idc app")
                add_group_to_redshift_app(i['role_name'])



# if __name__ == "__main__":
#     add_ad_groups_to_redshift_app()

