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
#from worley_helper.quality.glue_quality import GlueDataQuality
from worley_helper.utils.aws import DynamoDB

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

# Setup Configuration
config_job = Configuration(**metadata)

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
print("Getting basic raw statistics...")
total_raw_rows = raw_df.count()
if total_raw_rows == 0:
    print("No rows found, exiting...")
    job.commit()
    os._exit(0)

# Print Basic Values
print(f"Total number of rows for source: {total_raw_rows}")
raw_df.printSchema()

# Run Custom Transforms
transformed_df = Transforms(configuration=config_job).run(df=raw_df)
print("Getting basic transformed statistics...")
total_transformed_rows = transformed_df.count()
print(f"Total number of rows for transformed: {total_transformed_rows}")
transformed_df.printSchema()

# Runs data quality rules - if present in the configuration file
# dq = GlueDataQuality(configuration=config_job, glue_context=glueContext)
# results_df = dq.run_quality_checks(transformed_df)
# if results_df is not None:
#     results_df.show()

# Write Data
result = SparkDataRunner.write_data(
    df=transformed_df,
    configuration=config_job,
    spark=spark,
)

# Commit Job
job.commit()
