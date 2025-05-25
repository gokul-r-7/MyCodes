import sys
import time
import json
import uuid
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from worley_helper.utils.helpers import get_partition_str_mi, create_comma_separated_string
from worley_helper.utils.logger import get_logger
from worley_helper.utils.oracle_p6 import transform_and_store_in_parquet, get_spread_ids
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.aws import get_secret, S3, DynamoDB

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


#=================
# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "project_id", "source_name", "function_name","metadata_table_name", "start_date", "end_date"]
)
project_id = args.get("project_id")
project_id_partition = f"Project={project_id}"
source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
delta_start_date = args.get("start_date")
delta_end_date = args.get("end_date")

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

# Load Configuration of Oracle P6 Export API
#p6_export_config = yaml.safe_load(api_config)
spread_config = metadata
logger.info(f" P6 {function_name} Config :{spread_config}")
#=================


#############  Generate Dynamic Config for Oracle P6 Export API  ################
# Ensure the date format is "2024-05-30T00:00:00".
logger.info(f"Start date is {delta_start_date}")
logger.info(f"End date is {delta_end_date}")

#if delta_start_date:
#    spread_config['api_parameter']['api_query_params']['StartDate'] = delta_start_date

bucket_name = spread_config['job_parameter']['bucket_name']
source_data_location = spread_config['job_parameter']['input_path']
source_data_location = source_data_location + project_id_partition

spread_data_path = spread_config['job_parameter']['temp_output_path']
parquet_data_path = spread_config['job_parameter']['output_s3']
sample_data_path = spread_config['job_parameter']['schema_output_s3']
sampling_fraction = float(spread_config['job_parameter']['sampling_fraction'])
sampling_seed = spread_config['job_parameter']['sampling_seed']
region = spread_config['aws_region']
spread_id_attribute = spread_config['api_parameter']['api_custom_attributes']['spread_id_attribute']
batch_size = spread_config['api_parameter']['api_custom_attributes']['spread_id_batch_size']
dynamic_query_key_name = spread_config['api_parameter']['dynamic_api_query_params']['dynamic_query_key']

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(spread_config['auth_api_parameter']['secret_key'], region))
spread_config['auth_api_parameter']['auth_headers']['AuthToken'] = secret_param_key.get("oracle_p6_oauth_secret")


# Read Activity or ResourceAssignment IDs for the given Project Id
logger.info(
    "==================executing get_spread_ids ============"
)
ids = get_spread_ids(
    spark,
    glueContext,
    bucket_name,
    source_data_location,
    spread_id_attribute,
)
logger.info(f"IDs:  {ids}")

#Sort ids
ids.sort()

# Extract Spread Data
logger.info(
    "==================Invoking Spread API endpoint ============"
)

# Invoke Spread API in batches
temp_files = []
for i in range(0, len(ids), batch_size):
    batch = ids[i : i + batch_size]
    comma_separated_ids = create_comma_separated_string(batch)
    logger.info(f"Invoking spread api with {comma_separated_ids}")
    spread_config['api_parameter']['api_query_params'][dynamic_query_key_name] = comma_separated_ids
    http_client = HTTPClient(spread_config)
    response, api_status, api_resp_code = http_client.run()
    kms_key_id = spread_config['job_parameter']['kms_key_id']

    if response:
        logger.info(f"Successfully able to fetch Oracle P6 Spread API response for batch {comma_separated_ids}")
        random_filename = str(uuid.uuid4())  # Generate a random UUID
        s3_key = f"{spread_data_path}/{random_filename}.json"
        logger.info(f"Uploading raw JSON of spread API to {s3_key}")
        s3_client = S3(bucket_name, region)

        if s3_client.upload_to_s3( response, s3_key, kms_key_id):
            temp_files.append(s3_key)
            logger.info(f"Uploaded P6 Spread info to {bucket_name}/{s3_key}")
        else:
            logger.error("P6 Spread API was successful but the content could not be uploaded to S3")
            logger.info("Continuing to try and process rest of the batch")
    else:
        logger.error(f"Failed to fetch Oracle P6 Spread API payload for batch {comma_separated_ids}")
        logger.info("Continuing to try and process rest of the batch")


if temp_files:
    logger.info(
        "==================Transforming Spread Data into Parquet and store on S3 ============"
    )

    partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
        DATETIME_FORMAT
    )
    partition_str = f"{project_id_partition}/{get_partition_str_mi(partition_date)}"
    s3_key = parquet_data_path + "/" + partition_str

    api_source_data = (
        f"s3://{bucket_name}/{spread_data_path}"
    )
    target_parquet_data = f"s3://{bucket_name}/{s3_key}"
    schema_location = f"s3://{bucket_name}/{sample_data_path}"

    # Call the function to process Spread JSON response data and write to S3 in parquet
    transform_and_store_in_parquet(
        glueContext, api_source_data, target_parquet_data, schema_location, sampling_fraction, sampling_seed,project_id 
    )

    logger.info("==================Transformed Spread Data into Parquet and store on S3 successfully============")

    # Clean up temp files
    s3_client = S3(bucket_name, region)
    for file in temp_files:
        s3_client.delete_file_from_s3(file)

else:
    logger.error("==================Processing of Spread API failed============")

job.commit()
