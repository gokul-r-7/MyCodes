import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
from typing import Union as Un

try:
    from awsglue.transforms import *
    from awsgluedq.transforms import EvaluateDataQuality 
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import SelectFromCollection
except Exception as e:
    logger.warning("Couldn't import awsglue Data Quality libs")

from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from worley_helper.quality.base import BaseDataQuality
from worley_helper.configuration.config import Configuration

glue_client = boto3.client('glue')


class GlueDataQuality(BaseDataQuality):
    def __init__(self, configuration: Configuration, glue_context):
        self.configuration = configuration
        self.glue_context = glue_context
        self.ruleOutcome = None



    def run_quality_checks(self, df: DataFrame) -> DataFrame:
        """Function that runs the Data Quality checks"""

        print("Running Data Quality Checks")
        logger.info("Running Data Quality Checks")
        if self.configuration.quality is None:
            logger.info("There are no Data Quality rules required... Skipping")
            print("There are no Data Quality rules required... Skipping")
            return None

        rules = self.configuration.quality.configuration.rules
        ruleset=f"""Rules = {str(rules).replace("'","")}"""
        # ruleset = """Rules = [ColumnExists "documentid" ,IsComplete "documentid"]"""
     

        # Convert to Glue Dynamic Frame
        quality_df = DynamicFrame.fromDF(df, self.glue_context, "quality_df")

        print (f"Quality Count : {quality_df.count()}")
        try:
            EvaluateDataQualityMultiframe = EvaluateDataQuality().apply(
            frame=quality_df,
            ruleset = ruleset,
            publishing_options=self.configuration.quality.configuration.publishing_options
            )

            print(f"EvaluateDataQualityMultiframe :{EvaluateDataQualityMultiframe}")
            print("Data Quality results format:", type(EvaluateDataQualityMultiframe))

            logger.info("Data Quality checks completed, please review results")
            # Get rule outcomes and row level outcomes
            if isinstance(EvaluateDataQualityMultiframe, list):
                self.ruleOutcomes = SelectFromCollection.apply(
                dfc=EvaluateDataQualityMultiframe,
                key="ruleOutcomes",
                transformation_ctx="ruleOutcomes",
                )

                self.rowLevelOutcomes = SelectFromCollection.apply(
                dfc=EvaluateDataQualityMultiframe,
                key="rowLevelOutcomes",
                transformation_ctx="rowLevelOutcomes",
                )
            else:
                # If results are returned as a single DynamicFrame
                print("Data Quality results format:", type(EvaluateDataQualityMultiframe))
                # You might need to adjust this based on the actual structure
                self.ruleOutcomes = EvaluateDataQualityMultiframe
                self.rowLevelOutcomes = EvaluateDataQualityMultiframe

            return self.rowLevelOutcomes.toDF()
        except Exception as e:
            logger.error(f"Error in data quality checks: {str(e)}")
            print(f"Error in data quality checks: {str(e)}")
            raise

    def get_quality_checks_pass(self) -> bool:
        """Function that returns easy to consume status"""
        if self.ruleOutcomes is None:
            raise Exception("Please run DQ rules first")
        df: DataFrame = self.ruleOutcomes.toDF()

        return df.filter("Outcome = 'Failed'").count() == 0

    def get_quality_checks_data(self) -> DataFrame:
        """Function that returns easy to consume status"""
        if self.ruleOutcomes is None:
            raise Exception("Please run DQ rules first")
        return self.ruleOutcomes.toDF()
    
    #Get the results from an AWS Glue Data Quality run using boto3
    def get_quality_outcome(self, glue_client, job_run_id: str) -> Un[DataFrame, None]:
        """Function that returns easy to consume status"""
        try:
            response = self.glue_client.get_data_quality_result(
                RunId=job_run_id
            )
            print(response)
            return response
        except Exception as e:
            logger.error(f"Error in data quality checks: {str(e)}")
            print(f"Error in data quality checks: {str(e)}")
            raise  

class GlueWrapper:
        """Encapsulates AWS Glue actions."""
        def __init__(self,configuration: Configuration):
            """
            :param glue_client: A Boto3 AWS Glue client.
            """
            self.glue_client = glue_client
            self.configuration = configuration

        #List the results  AWS Glue Data Quality run using boto3 using database name and table name
        def list_data_quality_results(self):
            """
            Lists all the AWS Glue Data Quality runs against a given table
            
            :param database_name: The name of the database where the data quality runs 
            :param table_name: The name of the table against which the data quality runs were created
            
            """
            print(f"Glue CLient is : {self.glue_client}")
            try:
                response = self.glue_client.list_data_quality_results(
                    Filter={
                        'DataSource': {
                            'GlueTable': {
                                'DatabaseName': self.configuration.target.iceberg_properties.database_name,
                                'TableName': self.configuration.target.iceberg_properties.table_name
                            }
                        }
                    }
                )
            except ClientError as err:
                logger.error(
                    "Couldn't list the AWS Glue Quality runs. Here's why: %s: %s", 
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
            else:
                return response

        def get_data_quality_result(self, result_id):
            """
            Get details about an AWS Glue Data Quality Run
        
            :param run_id: The AWS Glue Data Quality run ID to look up

            """

     
            try:
                response = self.glue_client.get_data_quality_result(ResultId=result_id)
            except ClientError as err:
                    logger.error(
                        "Couldn't look up the AWS Glue Data Quality result ID. Here's why: %s: %s", 
                    err.response['Error']['Code'], err.response['Error']['Message'])
                    raise
            else:
                    return response

                




