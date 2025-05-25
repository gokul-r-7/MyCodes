from helpers import lakeformation,utilities,dynamodb,sns
import os

lfadmin = lakeformation()
dynamodb = dynamodb()
utils = utilities()
logger = utils.get_logger()
settings = utils.get_settings()

def lambda_handler(event,context):
    logger.info(f"event::: {event}")
    logger.info(f"Setting up lakeformation for {settings['aws_account_id']}")

    try:
        db_name = event['detail']['databaseName']
        data_catalog = dynamodb.get_all_db_items(settings["database_permissions_store"])
        matching_items = [item for item in data_catalog if item.get('database_name') == db_name]

        if matching_items:
            lfadmin.grant_database_permissions_all()
            lfadmin.grant_permissions_to_iam_roles()

        else:
            sns.notify().send(topic_arn=settings['rbac_notifications_arn'], subject="Data-platform RBAC Permissions WARNING", message=f"No permissions meta data found for {db_name} database,please inform the data engineering team")
            logger.warning(f"No permissions meta data found for {db_name}")

    except KeyError as ke:
        logger.warning(f"Invalid event payload structure, must be test event {ke}")
        # lfadmin.grant_database_permissions_all()

if __name__ == "__main__":
    event = {'version': '0', 'id': 'cf10e71f-0d45-fad2-321f-c0b29bfbecdc', 'detail-type': 'Glue Data Catalog Database State Change', 'source': 'aws.glue', 'account': '891377181979', 'time': '2024-10-01T02:56:36Z', 'region': 'ap-southeast-2', 'resources': ['arn:aws:glue:ap-southeast-2:891377181979:table/worley_datalake_sydney_dev_glue_catalog_database_construction_o3_curated/tst_rbac_03'], 'detail': {'databaseName': 'worley_datalake_sydney_dev_glue_catalog_database_construction_o3_curated', 'typeOfChange': 'CreateTable', 'changedTables': ['tst_rbac_03']}}
    lambda_handler(event,"")