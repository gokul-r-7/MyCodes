import sys
import yaml
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.configuration.config import Configuration
from worley_helper.utils.aws import DynamoDB
from worley_helper.spark.runner import SparkDataRunner

from worley_helper.utils.helpers import add_primary_key, get_run_date_column_name



# Parse Arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "metadata_table_name",
        "source_system_id",
        "metadata_type",
        "operation_type",
        "region_name"
    ],
)

# Set required Variables
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
metadata_table_name = args.get("metadata_table_name")
source_system_id = args.get("source_system_id", "")
metadata_type = args.get("metadata_type", "")
region_name = args["region_name"]

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=region_name)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=metadata_type
)

# Setup Configuration
config = Configuration(**metadata)

spark = SparkDataRunner.create_spark_session(
    configuration=config
)

spark.conf.set("spark.sql.catalog.glue_catalog.cache-enabled", "false")

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

job.init(args["JOB_NAME"] + source_system_id + metadata_type, args)


operation = args['operation_type']

def perform_primary_key_change(config):

  curated_df = SparkDataRunner.read_iceberg_table(
      configuration=config,
      spark=spark,
      glue_context=glueContext,
  )

  print("Getting basic statistics for curated table...")
  total_curated_rows = curated_df.count()

  if total_curated_rows == 0:
      print("No rows found, exiting...")
      job.commit()
      os._exit(0)

  print(f"Total number of rows for curated: {total_curated_rows}")


  primary_key = config.table_schema.schema_properties.primary_key
  run_date_column_name = get_run_date_column_name(config.transforms)
  
  updated_curated_df = add_primary_key(curated_df, primary_key, run_date_column_name)
  return updated_curated_df

def perform_delete_rewrite(config, dataframe):
    
    #get table name from metadata
    table_name = config.target.iceberg_properties.table_name
    database = config.target.iceberg_properties.database_name
    
    print("Table Schema : ")
    dataframe.printSchema()

    dataframe.createOrReplaceTempView(f"temp_{table_name}")
    
    sql = f""" INSERT OVERWRITE glue_catalog.{database}.{table_name}
               SELECT * FROM temp_{table_name} """

    print(f"Executing Statement : {sql}")
    spark.sql(sql)
    print(f"Rewrite completed for table {table_name}")


if operation == 'change_primary_key':
    updated_curated_df = perform_primary_key_change(config)
    perform_delete_rewrite(config, updated_curated_df)
    job.commit()

else:
     print(f"Operation {operation} not supported")