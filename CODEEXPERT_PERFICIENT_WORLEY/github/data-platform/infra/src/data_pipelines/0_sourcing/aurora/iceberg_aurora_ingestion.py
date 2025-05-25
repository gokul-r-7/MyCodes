import sys
import boto3
import json
import psycopg2
from psycopg2 import sql
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit
from functools import reduce
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT
from pyspark.sql import functions as F

# Initialize the logger
logger = get_logger(__name__)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_table_name', 'source_system_id', 'metadata_type'])

logger.info(args)
# Set required Variables
job_name = args["JOB_NAME"]
metadata_table_name = args.get("metadata_table_name")
source_system_id = args.get("source_system_id")
metadata_type = args.get("metadata_type")

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_system_id, metadata_type=metadata_type
)

# Set parameters 
iceberg_warehouse = metadata["target"]["iceberg_properties"]["iceberg_configuration"]["iceberg_catalog_warehouse"]
iceberg_db_name = metadata["target"]["iceberg_properties"]["database_name"]
iceberg_table_name = metadata["target"]["iceberg_properties"]["table_name"]
aurora_host = metadata["target"]["db_load"]["aurora_host"]
aurora_port = metadata["target"]["db_load"]["aurora_port"]
aurora_secret = metadata["target"]["db_load"]["aurora_secret"]
aurora_db_name = metadata["target"]["db_load"]["aurora_db_name"]
aurora_db_target_table_name = metadata["target"]["db_load"]["aurora_db_target_table_name"]
aurora_db_target_schema = metadata["target"]["db_load"]["aurora_db_target_schema"]
aurora_data_load_type = metadata["target"]["db_load"]["aurora_data_load_type"]
# S3 bucket and key for storing the last processed snapshot ID
data_load_snapshot_s3_bucket = metadata["target"]["db_load"]["snapshot_s3_bucket"]
data_load_snapshot_s3_key = metadata["target"]["db_load"]["snapshot_s3_key"]
kms_key_id = metadata["target"]["db_load"]["snapshot_s3_kms_key_id"]
primary_key_var = metadata["target"]["db_load"]["primary_key"]

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark session for Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://" + iceberg_warehouse + "")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Get the database credentials from AWS Secrets Manager
secret_params = json.loads(get_secret(aurora_secret, REGION))
db_username = secret_params.get("username")
db_password = secret_params.get("password")
db_jdbc_url = f"jdbc:postgresql://{aurora_host}:{aurora_port}/{aurora_db_name}"

# Function to get the last processed snapshot ID from S3
def get_last_processed_snapshot(data_load_snapshot_s3_bucket, data_load_snapshot_s3_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=data_load_snapshot_s3_bucket, Key=data_load_snapshot_s3_key)
        return int(response['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.error(f"Error reading last processed snapshot ID: {str(e)}")
        return None
# Function to update the last processed snapshot ID in S3, using KMS Keys
def update_last_processed_snapshot(data_load_snapshot_s3_bucket, data_load_snapshot_s3_key, snapshot_id):
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket=data_load_snapshot_s3_bucket,
            Key=data_load_snapshot_s3_key,
            Body=str(snapshot_id),
            ServerSideEncryption='aws:kms',
            SSEKMSKeyId=kms_key_id
        )
        logger.info(f"Updated last processed snapshot ID: {snapshot_id}")
    except Exception as e:
        logger.error(f"Error updating last processed snapshot ID: {snapshot_id}, to the S3 bucket {data_load_snapshot_s3_bucket}, with the standard error: {str(e)}")
        
def get_primary_key(dataframe):
  logger.info("Getting the primary key from the table")
  if primary_key_var:
    primary_key = primary_key_var
  else:
    primary_key = dataframe.columns[0]
    logger.info(f"Using the first column '{primary_key}' as the primary key")
  return primary_key

# Get the current snapshot ID using Spark SQL
snapshot_df = spark.sql(f"SELECT snapshot_id, committed_at FROM glue_catalog.{iceberg_db_name}.{iceberg_table_name}.snapshots ORDER BY committed_at DESC LIMIT 1")
current_snapshot_id = snapshot_df.collect()[0]['snapshot_id']

logger.info(f"Current snapshot ID from Iceberg Table: {current_snapshot_id}")

# Get the last processed snapshot ID
last_snapshot_id = get_last_processed_snapshot(data_load_snapshot_s3_bucket, data_load_snapshot_s3_key)
logger.info(f"Last processed snapshot ID by Glue: {last_snapshot_id}")

# Data loading logic
if last_snapshot_id is None:
  # If it's the first run, read the entire table
  logger.info("No last processed snapshot id found on the specified location. Performing a full data load")
  df = spark.read.format("iceberg").load(f"glue_catalog.{iceberg_db_name}.{iceberg_table_name}")
  process_data = True
  data_load_mode = "overwrite"
elif last_snapshot_id == current_snapshot_id:
  logger.info("No new changes detected. Current iceberg table snapshot id and last processed snapshot id by Glue are the same since the last run. Skipping processing...")
  process_data = False
else:
  logger.info(f"Loading the current dataframe from the iceberg table")
  current_df = spark.read.format("iceberg").load(f"glue_catalog.{iceberg_db_name}.{iceberg_table_name}").filter("is_current = 1")
    
  # Read the previous state of the table
  logger.info(f"Determining the state of the iceberg table from last processed snapshot ID: {last_snapshot_id}.") 
  previous_df = spark.read.format("iceberg") \
      .option("snapshot-id", last_snapshot_id) \
      .load(f"glue_catalog.{iceberg_db_name}.{iceberg_table_name}").filter("is_current = 1")
  #previous_df.explain(True)
  previous_df_count=previous_df.count()
  current_df_count=current_df.count()
  logger.info (f"Total records count in previous snapshot -> {previous_df_count}")
  logger.info (f"Total records count in current snapshot -> {current_df_count}")
  # Get the primary key column (assuming it's the first column)
  primary_key = get_primary_key(current_df)
  # Identify deleted rows
  logger.info("Identifying the deleted rows...")
  deleted_df = previous_df.join(current_df, primary_key, "left_anti")
  deleted_df_count=deleted_df.count()
  logger.info(f"Total number of deleted rows: {deleted_df.count()}")

  # Identify new rows
  logger.info("Identifying new addition to the table since the last run.")
  new_df = current_df.join(previous_df, primary_key, "left_anti")
  new_df_count=new_df.count()
  logger.info(f"Total number of new rows: {new_df_count}")

  # Identify rows that has been modified
  logger.info("Identifying any rows that have been modified")

  # Create a list of conditions for comparison
  #conditions = [col(f"current_df.{c}") != col(f"previous_df.{c}") for c in current_df.columns if c != primary_key]
  conditions = [
       (col(f"current_df.{c}").isNull() & col(f"previous_df.{c}").isNotNull()) |
       (col(f"current_df.{c}").isNotNull() & col(f"previous_df.{c}").isNull()) |
       ((col(f"current_df.{c}") != col(f"previous_df.{c}")) & 
        col(f"current_df.{c}").isNotNull() & 
        col(f"previous_df.{c}").isNotNull())
       for c in current_df.columns if c != primary_key
  ]
  # Combine all conditions with OR
  combined_condition = reduce(lambda a, b: a | b, conditions)
  # Identify modified rows
  modified_df = current_df.alias("current_df").join(
    previous_df.alias("previous_df"),
    primary_key,
    "inner"
  ).where(combined_condition).select("current_df.*")
  modified_df_count=modified_df.count()
  logger.info(f"Total number of modified rows: {modified_df_count}")
  process_data = True
  data_load_mode = "append"

if process_data:
    if data_load_mode == "overwrite":
        # Perform a full overwrite
        logger.info(f"Detected data load mode as {data_load_mode}. Performing a full overwrite")
        df.write \
            .format("jdbc") \
            .option("url", db_jdbc_url) \
            .option("dbtable", f"\"{aurora_db_target_schema}\".{aurora_db_target_table_name}") \
            .option("user", db_username) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
    else:
        # Handle deletions
        if deleted_df_count > 0:
            logger.info(f"Starting deletions")
            logger.info(f"Handling {deleted_df_count} number of deletions")
            
            # Assume the first column is the primary key
            primary_key = get_primary_key(deleted_df)
            
            deleted_ids = deleted_df.select(primary_key).rdd.flatMap(lambda x: x).collect()
            
            conn = psycopg2.connect(
                host=aurora_host,
                dbname=aurora_db_name,
                user=db_username,
                password=db_password
            )
            
            try:
                with conn.cursor() as cur:
                    delete_stmt = f"DELETE FROM \"{aurora_db_target_schema}\".{aurora_db_target_table_name} WHERE \"{primary_key}\" IN ({','.join(['%s' for _ in deleted_ids])})"
                    cur.execute(delete_stmt, tuple(deleted_ids))
                conn.commit()
                logger.info(f"Deleted {len(deleted_ids)} rows from PostgreSQL")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error occurred during deletion: {str(e)}")
            finally:
                conn.close()

        # Handle modifications
        if modified_df_count > 0:
            logger.info(f"Starting modifications...")
            logger.info(f"Handling {modified_df_count} number of modification(s)")
            
            conn = psycopg2.connect(
                host=aurora_host,
                dbname=aurora_db_name,
                user=db_username,
                password=db_password
            )
            
            try:
                with conn.cursor() as cur:
                    # Get the column names from the DataFrame
                    columns = modified_df.columns
                    
                    # Assume the first column is the primary key
                    primary_key = get_primary_key(modified_df)
                    
                    logger.info(f"creating temp table \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}_temp\"")
                    # Assuming target table has the same structure as the modified dataframe
                    create_temp_table_stmt = f"""
                        CREATE TEMP TABLE {aurora_db_target_table_name}_temp  AS 
                        SELECT * FROM \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\" 
                        WHERE 1=0;  -- Create the structure without inserting any data
                        """
                    cur.execute(create_temp_table_stmt)
                    logger.info(f"Temple table created")
                    
                    #Create an index on the primary key
                    create_temp_table_idx_stmt = f"""
                        CREATE INDEX IF NOT EXISTS idx_{aurora_db_target_table_name}_temp_primary_key 
                        ON {aurora_db_target_table_name}_temp("{primary_key}");
                        """
                    cur.execute(create_temp_table_idx_stmt)
                    logger.info(f"Index created for temp table")
                    
                    logger.info(f"Insert into temp table for update records")
                    
                    logger.info(f"Create idx on table \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\" if doesn't exists ")
                    #Create an index on the primary key
                    create_table_idx_stmt = f"""
                        CREATE INDEX IF NOT EXISTS idx_{aurora_db_target_table_name}_primary_key 
                        ON \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\"("{primary_key}");
                        """
                    cur.execute(create_table_idx_stmt)

                    data = modified_df.collect() 
                    columns = modified_df.columns
                    # Set the batch size
                    batch_size = 10000
                    logger.info(f"batch size set to {batch_size}")
                    
                    # Define the insert statement
                    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(f"{aurora_db_target_table_name}_temp"),  # Temp table name without schema
                        sql.SQL(', ').join(map(sql.Identifier, columns)),  # Include all columns (primary key included)
                        sql.SQL(', ').join([sql.Placeholder()] * len(columns))  # Placeholders for values
                    )
                    
                    logger.info("Insert records into temp table for update operation started")
                    # Insert data in batches
                    for i in range(0, len(data), batch_size):
                        
                        batch_data = [tuple(row[col] for col in columns) for row in data[i:i + batch_size]]  # Prepare the batch data
                        cur.executemany(insert_stmt, batch_data)  # Insert the batch of data
                        
                    logger.info("Insert records into temp table for update operation completed")

                    # Create the SET clause for the UPDATE statement
                    set_clause = ', '.join([f"\"{col}\" = {aurora_db_target_table_name}_temp.\"{col}\"" for col in columns if col != primary_key])
                                                    
                    # Define the SQL insert statement
                    insert_stmt = f"""
                        INSERT INTO \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\" ({', '.join([f'"{col}"' for col in columns])})
                        SELECT {', '.join([f'"{col}"' for col in columns])}
                        FROM {aurora_db_target_table_name}_temp
                        WHERE NOT EXISTS (
                            SELECT 1 FROM \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\" 
                            WHERE \"{aurora_db_target_table_name}\".\"{primary_key}\" = {aurora_db_target_table_name}_temp.\"{primary_key}\"
                        );
                    """
                    #logger.info(insert_stmt)
                    
                    # Define the SQL update statement
                    update_stmt = f"""
                        UPDATE \"{aurora_db_target_schema}\".\"{aurora_db_target_table_name}\" t
                        SET {', '.join([f'"{col}" = {aurora_db_target_table_name}_temp."{col}"' for col in columns if col != primary_key])}
                        FROM {aurora_db_target_table_name}_temp
                        WHERE t.\"{primary_key}\" = {aurora_db_target_table_name}_temp.\"{primary_key}\";
                    """
                    #logger.info(update_stmt)

                     # Execute the update statement
                    cur.execute(update_stmt)

                     # Execute the update statement
                    cur.execute(insert_stmt)
                    

                    cur.execute(f"DROP TABLE IF EXISTS {aurora_db_target_table_name}_temp;")               
                conn.commit()
                logger.info(f"Modified {modified_df_count} row(s) on PostgreSQL")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error occurred during modification: {str(e)}")
                raise
            finally:
                conn.close()
        # Handle inserts
        if new_df_count > 0:
            logger.info("Starting new inserts")
            logger.info(f"Handling {new_df_count} inserts")
            new_df.write \
                .format("jdbc") \
                .option("url", db_jdbc_url) \
                .option("dbtable", f"\"{aurora_db_target_schema}\".{aurora_db_target_table_name}") \
                .option("user", db_username) \
                .option("password", db_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

    # Update the last processed snapshot ID
    update_last_processed_snapshot(data_load_snapshot_s3_bucket, data_load_snapshot_s3_key, current_snapshot_id)
    logger.info("Data processing completed")
    logger.info(f"Updated last processed snapshot ID : {current_snapshot_id}" )
else:
    logger.info("No data processed. Last processed snapshot ID remains unchanged.")

# Commit the job
job.commit()