  
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import sys
from awsglue.utils import getResolvedOptions
import logging

from worley_helper.spark.runner import read_iceberg_sql
from worley_helper.configuration.project_mapping import ProjectMappingMetadataModel
from worley_helper.utils.project_mapping_helper import FilterPreprocessor
from worley_helper.configuration.config import Configuration
from worley_helper.utils.aws import DynamoDB
from pyspark.sql import SparkSession
from worley_helper.utils.helpers import (
    check_if_table_exists
)

#intialize logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Parse Arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "metadata_table_name",
        "region_name",
        "region_short_name",
        "domain_name",
        "environment",
        "target_table",
        "transformed_s3_bucket"
    ],
)

job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
region_name = args["region_name"]
metadata_table_name = args["metadata_table_name"]
region_short_name = args["region_short_name"]
domain_name = args["domain_name"]
environment = args["environment"]
target_table = args["target_table"]
transformed_s3_bucket = args["transformed_s3_bucket"]


# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=region_name)
mapping_metadata_list = ddb.get_project_metdata_from_ddb(domain_name, target_table)

# Logging Metadata
logger.info(f"Metadata for {domain_name} {target_table} is {mapping_metadata_list}")


# convert each element of the mapping_metadata_list to ProjectMappingMetadataModel and also run validate_model
project_mapping_metadata_list = []

for metadata in mapping_metadata_list:
    model = ProjectMappingMetadataModel(**metadata)
    validated_model = ProjectMappingMetadataModel.model_validate(metadata)
    project_mapping_metadata_list.append(validated_model)

# Iceberg Warehouse configuration
warehouse = f"s3://{transformed_s3_bucket}/{domain_name}/warehouse/"
       
    #Create Iceberg Spark Session
spark = SparkSession.builder \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{warehouse}") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold","-1")\
            .config("spark.sql.autoBroadcastJoinThreshold","-1")\
            .config("spark.sql.adaptive.enabled","false")\
            .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

job.init(args["JOB_NAME"] + domain_name + target_table, args)


# Set default database
target_database = f"worley_datalake_{region_short_name}_{environment}_glue_catalog_database_{domain_name}_transformed"
logger.info(f"Setting default database to {target_database}")
spark.sql(f"USE {target_database}")



logger.info("Checking if stg_{target_table} exists")
stg_table_status = check_if_table_exists(target_database, f"stg_{target_table}")

if stg_table_status:
    logger.info(f"Deleting existing data from stg_{target_table}")
    spark.sql(f"DELETE TABLE stg_{target_table}")
    logger.info(f"Deleted existing data from stg_{target_table}")



for project_mapping_metadata in project_mapping_metadata_list:
    
    source_config_list = FilterPreprocessor.preprocess(
        source_tables = project_mapping_metadata.source_table_info,
        table_filters = project_mapping_metadata.source_business_filter  
    )

    # Use Config Source List and read the source table data with pushdown filters
    source_table_df_list = []
    for source_config in source_config_list:

        source_table_df = read_iceberg_sql(
            spark=spark,
            database = source_config.database,
            table = source_config.table,
            pushdown_filters = source_config.pushdown_filters
        )
        source_table_df_list.append(source_table_df)
        source_table_df.createOrReplaceTempView(f"{source_config.alias}")


        logger.info(f"Read {source_config.table} from {source_config.database} with pushdown filters: {source_config.pushdown_filters}")
        logger.info(f" Record Count is : {source_table_df.count()}")
    
    logger.info("Completed Reading Source Table Information for {project_mapping_metadata.target_table} for {project_mapping_metadata.project_id}")

    logger.info("Starting the execution of Data Load SQL")
    logger.info(f"SQL Query: {project_mapping_metadata.data_load_sql}")

    # Execute the Data Load SQL
    try:
        dataload_df = spark.sql(project_mapping_metadata.data_load_sql)
        logger.info(f"Records after executing the Data Load SQL : {dataload_df.show()}")
        logger.info(f"Record Count after executing the Data Load SQL : {dataload_df.count()}")
        
        dataload_df.format("iceberg").mode("append").saveAsTable("stg_{target_table}")
        logger.info("Completed inserting data to the stg_{target_table} for {project_mapping_metadata.project_id}")

    except Exception as e:
        logger.warning(f"Error executing the Data Load SQL: {str(e)}")
        logger.warning(f"Data SQL Failed for {project_mapping_metadata.target_table} for {project_mapping_metadata.project_id}")


logger.info("Completed the execution of Data Load SQL into staging table for {target_table}")
