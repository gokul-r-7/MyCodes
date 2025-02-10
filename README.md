select da.*,count(ivr.customer_key) from digital_adoption_all_channels da JOIN "call"."call_ivr_fact" ivr
    ON da.customer_key = ivr.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3--IVR_call_total_customers
select da.*,count(mch.customer_key) from digital_adoption_all_channels da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_app_total_customers
select da.*,count(wch.customer_key) from digital_adoption_all_channels da JOIN webanalytics.web_contact_history wch
    ON da.customer_key = wch.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_com_total_customers



import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a CloudWatch log handler
handler = logging.StreamHandler(sys.stdout)  # Use stdout for CloudWatch
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# Start the job
logger.info("Starting Glue job: %s", args['JOB_NAME'])
job.init(args['JOB_NAME'], args)

try:
    # Your ETL logic goes here
    logger.info("Performing ETL operations.")
    
    # Example: reading data
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="your_table")
    logger.info("Data read successfully from catalog.")
    
    # Example: transforming data
    transformed_data = datasource0 # Replace with your transformation logic
    logger.info("Data transformed successfully.")
    
    # Example: writing data
    glueContext.write_dynamic_frame.from_catalog(transformed_data, database="your_output_database", table_name="your_output_table")
    logger.info("Data written successfully to catalog.")

except Exception as e:
    logger.error("An error occurred: %s", str(e))

finally:
    job.commit()
    logger.info("Glue job completed.")
