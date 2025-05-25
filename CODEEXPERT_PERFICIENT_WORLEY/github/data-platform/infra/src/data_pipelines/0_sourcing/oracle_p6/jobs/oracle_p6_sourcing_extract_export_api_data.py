import sys
import json
from pyspark.sql.functions import lit
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


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

logger.info(f"Start date is {delta_start_date}")
logger.info(f"End date is {delta_end_date}")

# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)

logger.info(f" Metadata Response :{metadata}")

p6_export_config = metadata
logger.info(f" P6 Export Config :{p6_export_config}")

#############  Load Oracle P6 Export API Config ################
bucket_name = p6_export_config['job_parameter']['bucket_name']
raw_data_location = p6_export_config['job_parameter']['input_path']
relationalized_data_path = p6_export_config['job_parameter']['temp_output_path']
relationalized_data_path = relationalized_data_path + project_id_partition

parquet_data_path = p6_export_config['job_parameter']['output_s3']
sample_data_path = p6_export_config['job_parameter']['schema_output_s3']
source_tables = p6_export_config['job_parameter']['source_tables']
renamed_tables = p6_export_config['job_parameter']['renamed_tables']
sampling_fraction = float(p6_export_config['job_parameter']['sampling_fraction'])
sampling_seed = p6_export_config['job_parameter']['sampling_seed']
p6_root_node = p6_export_config['job_parameter']['export_api_root_node']
activity_column = p6_export_config['job_parameter']['activity_column']
activity_code_column = p6_export_config['job_parameter']['activity_code_column']
activity_code_join_condition = p6_export_config['job_parameter']['activity_code_join_condition']
activity_join_condition = p6_export_config['job_parameter']['activity_join_condition']
activitycodeassignment_table = p6_export_config['job_parameter']['activitycodeassignment_table_name']
udf_column = p6_export_config['job_parameter']['udf_column']
udf_table_name = p6_export_config['job_parameter']['udf_table_name']
udf_join_condition = p6_export_config['job_parameter']['udf_join_condition']
region = p6_export_config['aws_region']
api_retry_count=p6_export_config['api_parameter']['api_retry']

#############  Generate Dynamic Config for Oracle P6 Export API  ################
# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)
export_api_body_params = s3_client.read_s3_file_as_json(p6_export_config['api_parameter']['custom_config']['export_api_input_body'])

# Add Project ID to the HTTP POST body parameters
export_api_body_params["ProjectObjectId"] = project_id
p6_export_config['api_parameter']['api_body'] = export_api_body_params

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(p6_export_config['auth_api_parameter']['secret_key'], region))
p6_export_config['auth_api_parameter']['auth_headers']['AuthToken'] = secret_param_key.get("oracle_p6_oauth_secret")


#######################################################
try:

    #success = load_p6_project_data(p6_export_config)
    api_call_attempt = 1

    http_client = HTTPClient(p6_export_config)
    while (api_call_attempt < api_retry_count):
        response, api_status, api_resp_code = http_client.run()
        if api_resp_code == 200:
            break
        else:
            print(f"API failed. Attempt {api_call_attempt}")
            api_call_attempt=api_call_attempt+1
    logger.info(f"API Status: {api_status}, API Response Code: {api_resp_code}")

    if api_resp_code != 200:
        logger.info("Glue job failed as response code is not 200")
        sys.exit(1)

    success = False
    object_name = f"{raw_data_location}{project_id}.gz"
    kms_key_id = p6_export_config['job_parameter']['kms_key_id']

    if response:
        if s3_client.upload_to_s3(response,object_name,kms_key_id,is_gzip=True):
            logger.info(f"Uploaded P6 Project info to {bucket_name}/{object_name}")
            success = True
        else:
            logger.error("P6 Export API was successful but the gzip content could not be uploaded to S3")
            success = False
    else:
        logger.error("Failed to fetch Oracle P6 Export API payload")
        success = False

    if success:

        # Load whole XML file into a DynamicFrame
        root_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    f"s3://{bucket_name}/{raw_data_location}"
                ]
            },
            format="xml",
            format_options={"rowTag": p6_root_node},
            transformation_ctx=f"{project_id}",
        )

        logger.info("Dataframe created")

        meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
        )

        repartitioned_df = root_df.repartition(meta_num_partitions)

        logger.info(f"Relationalized data path location is {relationalized_data_path}")

        root_df_rf = Relationalize.apply(
            frame=repartitioned_df,
            staging_path=relationalized_data_path,
            name="root",
            transformation_ctx=f"relationalize__{project_id}",
        )

        logger.info("Relationalize applied")

        ## Iterate over nodes
        ## Note that root_Project.Activity.val.Code is not part of what is being stored on S3. We are directly operating on root_Project.Activity.val.Code in memory
        for source_node, target_node in zip(
            source_tables, renamed_tables
        ):
            partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
                DATETIME_FORMAT
            )
            partition_str = f"{project_id_partition}/{get_partition_str_mi(partition_date)}"
            partitioned_s3_key = (
                parquet_data_path
                + "/"
                + target_node
                + "/"
                + partition_str
            )
            sample_s3_key = sample_data_path + target_node + "/"

            ctx = target_node
            
            # Convert DynamicFrame to DataFrame, add the hardcoded column, and convert it back to DynamicFrame
            df_with_hardcoded_column = root_df_rf.select(source_node).toDF().withColumn("projectid", lit(project_id))
            dynamic_frame_with_hardcoded_column = DynamicFrame.fromDF(df_with_hardcoded_column, glueContext, "main_data")

            # Write partitioned data to S3
            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                dynamic_frame_with_hardcoded_column,
                bucket_name,
                partitioned_s3_key,
                ctx,
                typecast_cols_to_string = True
            )
            logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the main data {target_node}")

            # Write sample data to S3 without partitioning
            sample_data = (
                df_with_hardcoded_column
                .sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
            )  # Adjust the fraction as needed

            sample_data = sample_data.repartition(1)

            logger.info(f"Selected sample data {target_node}")

            ctx = target_node + "sampledata"

            success = write_glue_df_to_s3_with_specific_file_name(
                glueContext,
                DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
                bucket_name,
                sample_s3_key,
                ctx,
                typecast_cols_to_string = True
            )

            logger.info(f"write_glue_df_to_s3_with_specific_file_name completed for the sample data {target_node}")

        # Join Activity index with id of Code
        activity_df = root_df_rf.select(activity_column).toDF().withColumn("projectid", lit(project_id))
        ## The node root_Project.Activity.val.Code won't be store in S3. Will be operating directly on in-memory DynamicFrame.
        activity_code_df = root_df_rf.select(activity_code_column).toDF().withColumn("projectid", lit(project_id))
        udf_df = root_df_rf.select(udf_column).toDF().withColumn("projectid", lit(project_id))
        
        activity_df = DynamicFrame.fromDF(activity_df, glueContext, "activity_df")
        activity_code_df = DynamicFrame.fromDF(activity_code_df, glueContext, "activity_code_df")
        udf_df = DynamicFrame.fromDF(udf_df, glueContext, "udf_df")
        
        activityCodeAssignment_df = activity_code_df.join(
            paths1=[activity_code_join_condition], paths2=[activity_join_condition], frame2=activity_df
        )
        UDF_df = udf_df.join(
            paths1=[udf_join_condition], paths2=[activity_join_condition], frame2=activity_df
        )

        partition_date = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(
            DATETIME_FORMAT
        )
        partition_str = f"{project_id_partition}/{get_partition_str_mi(partition_date)}"
        partitioned_s3_key = (
            parquet_data_path
            + "/"
            + activitycodeassignment_table
            + "/"
            + partition_str
        )

        sample_s3_key = sample_data_path + activitycodeassignment_table + "/"

        ctx = activitycodeassignment_table

        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            activityCodeAssignment_df,
            bucket_name,
            partitioned_s3_key,
            ctx,
            typecast_cols_to_string = True
        )

        logger.info("write_glue_df_to_s3_with_specific_file_name completed for the main data - activityCodeAssignment")

        # Write sample data to S3 without partitioning
        sample_data = activityCodeAssignment_df.toDF().sample(
            withReplacement=False, fraction=sampling_fraction, seed=sampling_seed
        )  # Adjust the fraction as needed

        sample_data = sample_data.repartition(1)

        ctx = activitycodeassignment_table + "sampledata"
        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
            bucket_name,
            sample_s3_key,
            ctx,
            typecast_cols_to_string = True
        )

        logger.info("Selected sample data - activityCodeAssignment")

        sample_s3_key = sample_data_path + udf_table_name + "/"

        ctx = udf_table_name

        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            UDF_df,
            bucket_name,
            partitioned_s3_key,
            ctx,
            typecast_cols_to_string = True
        )

        logger.info("write_glue_df_to_s3_with_specific_file_name completed for the main data - UDF")

        # Write sample data to S3 without partitioning
        sample_data = UDF_df.toDF().sample(
            withReplacement=False, fraction=sampling_fraction, seed=sampling_seed
        )  # Adjust the fraction as needed

        sample_data = sample_data.repartition(1)

        ctx = udf_table_name + "sampledata"
        success = write_glue_df_to_s3_with_specific_file_name(
            glueContext,
            DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
            bucket_name,
            sample_s3_key,
            ctx,
            typecast_cols_to_string = True
        )

        logger.info("Selected sample data - UDF")


        logger.info("==================Processing of Export API data completed============")

    else:
        logger.error("Failed to fetch Export API data")

    # Delete any existing staaging data for this project as accumulated staging data can slowdown Relationalize's performance in future runs of the GlueJob
    logger.info(f"Delete staging data after relationalizing - {relationalized_data_path}")

    staging_path = s3_client.get_folder_path_from_s3(relationalized_data_path)
    logger.info(f"Deleting folder path {staging_path}")
    s3_client.delete_folder_from_s3(staging_path)

    # clean up the gzip file
    s3_client.delete_file_from_s3(object_name)


    job.commit()

except Exception as e:

    print("Error --> " + str(e))
    
    sys.exit(1)  # Exit with failure code 