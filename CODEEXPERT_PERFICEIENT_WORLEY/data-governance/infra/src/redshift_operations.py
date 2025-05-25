from helpers import lakeformation, utilities, RedshiftServerlessClient, S3
import time
import os
import sys
from typing import Dict, Any, Optional
import boto3

rs_serverless = RedshiftServerlessClient()
utils = utilities()
logger = utils.get_logger()
settings = utils.get_settings()
db_structure_config = utils.get_database_structure_config()
s3 = S3()

lambda_client = boto3.client('lambda')


def validate_event(event: Dict[str, Any]) -> None:
    """
    Validate the input event structure and data types.

    Args:
        event (dict): The event to validate

    Raises:
        TypeError: If event structure is invalid
        KeyError: If required fields are missing
    """
    if not isinstance(event, dict):
        raise TypeError(
            f"Event must be a dictionary, got {type(event)} instead")

    # Validate required fields if they exist
    if 'schema' in event and not isinstance(event['schema'], str):
        raise TypeError(
            f"Schema must be a string, got {type(event['schema'])} instead")

    if 'db' in event and not isinstance(event['db'], str):
        raise TypeError(
            f"Database must be a string, got {type(event['db'])} instead")

    if 'obt_name' in event and not isinstance(event['obt_name'], (list, tuple)):
        raise TypeError("OBT name must be a list or tuple")
    
    if 'obt_name' in event:
        if 'schema' not in event:
            raise KeyError("Add schema in event for RLS to apply")
        if 'db' not in event:
            raise KeyError("Add db in event for RLS to apply")
        
def setup_db_strut():
    logger.info("Setting up database structure")

    s3.create_database_structure()

    for database in db_structure_config:
        database_name = database['database_name']
        external_schemas = database['external_schema']
        product_schemas = database['product_schema']

        rs_serverless.create_database(database_name)

        for ext_schema in external_schemas:
            glue_db = ext_schema['glue_db_name']
            external_schema_name = ext_schema['external_schema_name']
            access_via_odbc = ext_schema['access_via_odbc']

            rs_serverless.create_external_schema_dbt(
                external_schema_name=external_schema_name,
                database=database_name,
                glue_db=glue_db
            )

            if access_via_odbc:
                rs_serverless.create_external_schema_roles(
                    external_schema_name=external_schema_name,
                    database=database_name,
                    glue_db=glue_db
                )

            for product_schema in product_schemas:
                rs_serverless.create_schema(
                    schema_name=product_schema,
                    database=database_name
                )

def create_roles():
    logger.info("Creating Redshift roles")
    rs_serverless.create_roles()

def apply_perms_ad_roles(db=None):
    if db is None:
        all_rs_perms_configs = utils.get_all_redshift_permissions_config()
        for rs_perms_config in all_rs_perms_configs:
            for db in rs_perms_config:
                roles = db['ad_roles']
                database = db['database_name']
                for role in roles:
                    for schema in role['allowed_schemas']:
                        if 'active' in schema and schema['active'] == False:
                            rs_serverless.revoke_usage_schema_role(
                                schema_name=schema['name'],
                                entity_name=role['role_name'],
                                database=database,
                                identity_provider=role['identity_provider']
                            )
                            rs_serverless.revoke_select_tables_role(
                                schema_name=schema['name'],
                                entity_name=role['role_name'],
                                database=database,
                                identity_provider=role['identity_provider']
                            )
                        elif 'active' not in schema:
                            rs_serverless.grant_usage_schema(
                                schema_name=schema['name'],
                                entity_name=role['role_name'],
                                database=database,
                                identity_provider=role['identity_provider']
                            )

                            rs_serverless.grant_select_on_all_tables_schema(
                                schema_name=schema['name'],
                                entity_name=role['role_name'],
                                database=database,
                                identity_provider=role['identity_provider']
                            )
    else:
        rs_perms_config = utils.get_redshift_permissions_config(db=db)
        for db in rs_perms_config:
            roles = db['ad_roles']
            database = db['database_name']
            for role in roles:
                for schema in role['allowed_schemas']:
                    if 'active' in schema and schema['active'] == False:
                        rs_serverless.revoke_usage_schema_role(
                            schema_name=schema['name'],
                            entity_name=role['role_name'],
                            database=database,
                            identity_provider=role['identity_provider']
                        )
                        rs_serverless.revoke_select_tables_role(
                            schema_name=schema['name'],
                            entity_name=role['role_name'],
                            database=database,
                            identity_provider=role['identity_provider']
                        )
                    elif 'active' not in schema:
                        rs_serverless.grant_usage_schema(
                            schema_name=schema['name'],
                            entity_name=role['role_name'],
                            database=database,
                            identity_provider=role['identity_provider']
                        )

                        rs_serverless.grant_select_on_all_tables_schema(
                            schema_name=schema['name'],
                            entity_name=role['role_name'],
                            database=database,
                            identity_provider=role['identity_provider']
                        )


def apply_perms_users(db=None):
    if db is None:
        all_rs_perms_configs = utils.get_all_redshift_permissions_config()
        for rs_perms_config in all_rs_perms_configs:
            for db in rs_perms_config:
                users = db['users']
                database = db['database_name']
                for user in users:
                    for schema in user['allowed_schemas']:
                        if 'active' in schema and schema['active'] == False:
                            rs_serverless.revoke_usage_schema_role(
                                schema_name=schema['name'],
                                entity_name=user['user_name'],
                                database=database,
                                identity_provider=user['identity_provider']
                            )
                            rs_serverless.revoke_select_tables_role(
                                schema_name=schema['name'],
                                entity_name=user['user_name'],
                                database=database,
                                identity_provider=user['identity_provider']
                            )
                        elif 'active' not in schema:
                            rs_serverless.grant_usage_schema(
                            schema_name=schema['name'],
                            entity_name=user['user_name'],
                            database=database,
                            identity_provider=user['identity_provider']
                            )
                            rs_serverless.grant_select_on_all_tables_schema(
                                schema_name=schema['name'],
                                entity_name=user['user_name'],
                                database=database,
                                identity_provider=user['identity_provider']
                            )
                            if 'permissions' in schema and schema['permissions'].upper() == "CREATE":
                                rs_serverless.grant_create_schema_to_user(
                                    schema_name=schema['name'],
                                    dbt_user=user['user_name'],
                                    database=database
                                )
    else:
        rs_perms_config = utils.get_redshift_permissions_config(db=db)
        for db in rs_perms_config:
            users = db['users']
            database = db['database_name']
            for user in users:
                for schema in user['allowed_schemas']:
                    rs_serverless.grant_usage_schema(
                        schema_name=schema['name'],
                        entity_name=user['user_name'],
                        database=database,
                        identity_provider=user['identity_provider']
                    )
                    rs_serverless.grant_select_on_all_tables_schema(
                        schema_name=schema['name'],
                        entity_name=user['user_name'],
                        database=database,
                        identity_provider=user['identity_provider']
                    )
                    if 'permissions' in schema and schema['permissions'].upper() == "CREATE":
                        rs_serverless.grant_create_schema_to_user(
                            schema_name=schema['name'],
                            dbt_user=user['user_name'],
                            database=database
                        )
                    if 'active' in schema and schema['active'] == False:
                        rs_serverless.revoke_usage_schema_role(
                            schema_name=schema['name'],
                            entity_name=user['user_name'],
                            database=database,
                            identity_provider=user['identity_provider']
                        )
                        rs_serverless.revoke_select_tables_role(
                            schema_name=schema['name'],
                            entity_name=user['user_name'],
                            database=database,
                            identity_provider=user['identity_provider']
                        )


def apply_permissions(db=None):
    logger.info("Applying Redshift permissions")
    apply_perms_ad_roles(db)
    apply_perms_users(db)

def setup_db_structure_handler(event, context):
    setup_db_strut()

def create_and_apply_permissions_handler(event, context):
    validate_event(event)
    
    if "create_roles" in event:
        create_roles()

    if "obt_name" in event:
        rs_serverless.create_rls_policy(
            tgt_db=event['db'],
            schema=event['schema']
        )
        for obt in event['obt_name']:
            rs_serverless.attach_rls_policy(
                tgt_db=event['db'],
                schema=event['schema'],
                obt_name=obt
            )

    if "db" in event:
        apply_permissions(event['db'])
    else:
        apply_permissions()

