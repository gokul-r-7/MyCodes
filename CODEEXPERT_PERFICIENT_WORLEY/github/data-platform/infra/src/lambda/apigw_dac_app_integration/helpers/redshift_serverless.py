from .utilities import utilities 
import os
import time
from .metrics import CustomMetrics


custom_metrics = CustomMetrics(service= "rbac-redshift-setup", namespace= 'rbac-framework')
tracer = custom_metrics.get_tracer()


utils = utilities()
settings = utils.get_settings()
logger = utils.get_logger()
session = utils.get_aws_session()
redshift_data_client = session.client('redshift-data') 

account_id = settings["aws_account_id"]

MAX_WAIT_CYCLES = 30


class RedshiftServerlessClient:
    def __init__(self):
        pass
      
    def run_redshift_statement(self,sql_statement,database):
        """
        Generic function to handle redshift statements (DDL, SQL..),
        it retries for the maximum MAX_WAIT_CYCLES.
        Returns the result set if the statement return results.
        """
         

        logger.info(f"Connecting to database:: {database}")
        logger.info("Running statement: {}".format(sql_statement))

        res = redshift_data_client.execute_statement(
            WorkgroupName=settings['redshift_serverless_host'],
            Database=database,
            SecretArn=settings['redshift_serverless_secret_arn'],
            Sql=sql_statement
        )

        # DDL statements such as CREATE TABLE doesn't have result set.
        has_result_set = False
        done = False
        attempts = 0

        while not done and attempts < MAX_WAIT_CYCLES:

            desc = redshift_data_client.describe_statement(Id=res['Id'])
            query_status = desc['Status']

            if query_status == "FAILED":
                if "already exists" in desc["Error"] or "already attached" in desc["Error"]:
                    logger.warning(f"ignoring {desc['Error']}")
                    done = True
                    break
                else:
                    logger.error('SQL query failed: ' + desc["Error"])    
                    raise Exception('SQL query failed: ' + desc["Error"])

            elif query_status == "FINISHED":
                done = True
                has_result_set = desc['HasResultSet']
                break
            else:
                logger.info("Current working... query status is: {} ".format(query_status))

            if not done and attempts >= MAX_WAIT_CYCLES:
                raise Exception('Maximum of ' + str(attempts) + ' attempts reached.')
            
            attempts += 1
            time.sleep(1)

        if has_result_set:
            data = redshift_data_client.get_statement_result(Id=res['Id'])
            response = data['Records']
            return response
        