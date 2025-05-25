from helpers import lakeformation,utilities,redshiftClient,CustomMetrics
import time
import os
import sys
from typing import Dict, Any, Optional

custom_metrics = CustomMetrics(service= "rbac-redshift-setup", namespace= 'rbac-framework')

tracer = custom_metrics.get_tracer()
metrics = custom_metrics.get_metrics()

rs = redshiftClient()
utils = utilities()
logger = utils.get_logger()
settings = utils.get_settings()


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
        raise TypeError(f"Event must be a dictionary, got {type(event)} instead")

    # Validate required fields if they exist
    if 'domain' in event and not isinstance(event['domain'], str):
        raise TypeError(f"Domain must be a string, got {type(event['domain'])} instead")

    if 'target_database' in event and not isinstance(event['target_database'], str):
        raise TypeError(f"Target database must be a string, got {type(event['target_database'])} instead")

    if 'obt_name' in event and not isinstance(event['obt_name'], (list, tuple)):
        raise TypeError("OBT name must be a list or tuple")


# Adding tracer
# See: https://docs.powertools.aws.dev/lambda-python/latest/core/tracer/
@tracer.capture_lambda_handler
# ensures metrics are flushed upon request completion/failure and capturing ColdStart metric
@metrics.log_metrics(capture_cold_start_metric=True)

def lambda_handler(event,context):

    validate_event(event)

    logger.info(f"Setting up redshift objects and permissions for {settings['aws_account_id']}")
    start_time = time.time()

    if "init" in event:
        logger.info("Received init instructions from event, triggering init activities")
        rs.create_roles()
        rs.set_up_domain_db()

        for i in settings['consumer_databases']:
                rs.set_up_consumer_db(i)

    rs.revoke_permissions()

    if "obt_name" in event:
        rs.create_rls_policy(
            tgt_db= event['target_database'],
            domain= event['domain']
        )
        for obt in event['obt_name']:
            rs.attach_rls_policy(
                tgt_db= event['target_database'],
                domain= event['domain'],
                obt_name= obt
            )

    try:
        rs.grant_permissions_roles(event['domain'])
        rs.grant_permissions_users(event['domain'])

    except KeyError as e:
        logger.error(f"ERROR - domain is not specified in the dag, please fix the dag")
        logger.error("refer https://github.com/Worley-AWS/construction-domain-model/pull/153")
        raise
        sys.exit(1)

class MockContext:
    def __init__(self):
        self.function_name = "test-function"
        self.function_version = "$LATEST"
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        self.memory_limit_in_mb = 128
        self.aws_request_id = "test-request-id"
        self.log_group_name = "/aws/lambda/test-function"
        self.log_stream_name = "2023/12/31/[$LATEST]123456789"

    def get_remaining_time_in_millis(self):
        return 30000  # 30 seconds


if __name__ == "__main__":
    init_event = {"init": "true"}
    event = {'domain': 'construction', 'obt_name': ['construction_obt_demo_with_rls'], 'target_database': 'global_standard_reporting'}
    event_2 = {'domain': 'construction',
             'target_database': 'global_standard_reporting'}
    context = MockContext()
    lambda_handler({},context)