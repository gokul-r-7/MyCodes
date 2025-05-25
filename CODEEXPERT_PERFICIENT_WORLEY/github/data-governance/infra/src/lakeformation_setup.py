from helpers import lakeformation,utilities
import os

lfadmin = lakeformation()
utils = utilities()
logger = utils.get_logger()
settings = utils.get_settings()


def lambda_handler(event,context):
    if "version" in event:
        os.environ["RBAC_VERSION"] = event["version"]
    logger.info(f"event::: {event}")
    logger.info(f"Setting up lakeformation for {settings['aws_account_id']}")
    lfadmin.add_remove_compare_tags()
    lfadmin.manage_tag_permissions()
    # lfadmin.grant_database_permissions_all() # this has to be run when the data is avaiable


if __name__ == "__main__":
    lambda_handler("","")