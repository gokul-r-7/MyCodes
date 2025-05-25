import json
from helpers import utilities, RedshiftServerlessClient

server_less_client = RedshiftServerlessClient()
utils = utilities()
logger = utils.get_logger()


def exe_query(useremail):
    """
    Execute Redshift query and return results
    """
    try:
        logger.info("Executing query")
        select_query=f"select on_time,overdue,last_refresh_date from document_control.workflow_details where email='{useremail}'"
        db_result = server_less_client.run_redshift_statement(select_query,
            "dac"
        )
        #check for empty result if no result found
        if len(db_result) == 0:
            result = { "Message": "User not present in Aconex",
        "Error": "User not found"}
        else:
            result = {"ontime":f"{db_result[0][0]['longValue']}",
                      "overdue":f"{db_result[0][1]['longValue']}",
                      "last_refresh_date":f"{db_result[0][2]['stringValue']}"}
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler with proper API Gateway response formatting
    """
    try:
        logger.info(f"Received event: {event}")
        useremail=event["queryStringParameters"]['EmailId']

        # Execute the query and get results
        query_results = exe_query(useremail.lower())

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({
                "Result": query_results
            })
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "message": "Internal server error",
                "error": str(e)
            })
        }
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

