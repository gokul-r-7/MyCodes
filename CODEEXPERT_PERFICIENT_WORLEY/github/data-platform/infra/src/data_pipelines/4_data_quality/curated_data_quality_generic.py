import sys
import yaml
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.configuration.config import Configuration
from worley_helper.spark.runner import SparkDataRunner
from worley_helper.transformations import Transforms
from worley_helper.quality.glue_quality import GlueDataQuality,GlueWrapper
from worley_helper.utils.aws import DynamoDB
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import logging
import boto3
import json
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)          

# Parse Arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "metadata_table_name",
        "source_system_id",
        "metadata_type",
        "region_name",
        "region_short_name"
    ],
)

# Set required Variables
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
region_name = args["region_name"]
metadata_table_name = args.get("metadata_table_name")
source_system_id = args.get("source_system_id", "")
metadata_type = args.get("metadata_type", "")
region_short_name = args["region_short_name"]


# Start Glue Job
# job = Job(glueContext)
# job.init(args["JOB_NAME"] + source_system_id + metadata_type, args)


# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=region_name)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=metadata_type
)

logger.info(f"metadata: {metadata}")

# Setup Configuration
config_job = Configuration(**metadata)

logger.info(f"Configuration: {config_job}")

spark = SparkDataRunner.create_spark_session(
    configuration=config_job
)

# check for entity arguments
config_job = SparkDataRunner.get_entity_load_attributes(
    configuration=config_job,
    job_args=sys.argv
)

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)


job.init(args["JOB_NAME"] + source_system_id + metadata_type, args)

source_df = SparkDataRunner.read_data(
    configuration=config_job,
    spark=spark,
    glue_context=glueContext,
)
raw_df = source_df.toDF()
logger.info("Getting basic raw statistics...")
total_raw_rows = raw_df.count()
if total_raw_rows == 0:
    logger.warning("No rows found, exiting...")
    job.commit()
    os._exit(0)

# Print Basic Values
logger.info(f"Total number of rows for source: {total_raw_rows}")
#raw_df.printSchema()
schema_str = str(raw_df.schema)
logger.info(f"Schema of raw_df: {schema_str}")

# Run GlueDataQuality

# Creates Spark Runner Class
source_iceberg_df = SparkDataRunner.read_iceberg_table(
    configuration=config_job,
    spark=spark
)

source_iceberg_df.printSchema()

new_dyf = DynamicFrame.fromDF(source_iceberg_df, glueContext, "dynamic_frame_name")
#new_dyf.printSchema()
# Log schema information
schema_str = str(source_iceberg_df.schema)
logger.info(f"Schema of source_iceberg_df: {schema_str}")


'''
 Uncomment this piece of code if you need to Standalone Data Quality Testing 
# #Create data quality ruleset

# ruleset = """Rules = [ColumnExists "documentid" ,IsComplete "documentid"]"""
# #Evaluate data quality

# dqResults = EvaluateDataQuality.apply(
# frame=new_dyf,
# ruleset=ruleset,
# publishing_options={
#     "dataQualityEvaluationContext": "new_dyf",
#     "enableDataQualityCloudWatchMetrics": True,
#     "enableDataQualityResultsPublishing": True,
#     "publishToDataCatalogTarget": "worley_datalake_sydney_dev_glue_catalog_database_document_control_aconex_curated.curated_docregister_custom",
#     "resultsS3Prefix": "s3://worley-test-bucket-to-delete/DQResults/",
# },
# )
# #Inspect data quality results

# dqResults.printSchema()
# dqResults.toDF().show()
 '''

# Runs data quality rules - if present in the configuration file
dq = GlueDataQuality(configuration=config_job, glue_context=glueContext)
results_df = dq.run_quality_checks(source_iceberg_df)
if results_df is not None:
    results_df.show()
    results_df.printSchema()
    logger.info("Data Quality checks completed, please review results")
result_check = dq.get_quality_checks_pass()

logger.info("Data Quality corresponding results are as follows {result_check}")

# Extract the result id  from the Data Quality Runs 
glue_wrapper = GlueWrapper(configuration=config_job)
list_dq_response = glue_wrapper.list_data_quality_results()
result_id = list_dq_response['Results'][0]['ResultId']

# Extract the rresultant overall Score from the Data Quality Runs 
result_response = glue_wrapper.get_data_quality_result(result_id)
logger.info (f" Overall Result Score is {result_response['Score']}")

if (result_response['Score'] <= 0.6):
    logger.info (f"DQ Job FAILED with Score: {result_response['Score']}")
    sys.exit(1)

job.commit()
