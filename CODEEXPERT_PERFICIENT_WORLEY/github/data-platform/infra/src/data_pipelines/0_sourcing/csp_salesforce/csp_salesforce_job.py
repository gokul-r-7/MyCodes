import sys
import json
import pytz
import os
from dateutil.parser import parse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from worley_helper.utils.logger import get_logger
from worley_helper.utils.constants import REGION, TIMEZONE_SYDNEY, TIMESTAMP_FORMAT
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.sources.csp_salesforce_runner import Salesforce

# Init the logger
logger = get_logger(__name__)

def get_sf_soql_config(
        job_config: dict,
        func_job_config: dict,
        s3_client: S3,
        sf_runner: Salesforce,
        fields_list="ALL"):
    """ Generates the associated sf object API config for http client

    Args:
        job_config (dict): Generic base API metadata
        func_job_config (dict): API config for the respective Sf Object
        s3_client (S3): S3 client wrapper for raw bucket
        sf_runner (Salesforce): SF data processor wrapper
        fields_list (str, optional): named fields to call API.

    Returns:
        dict: API config for the respective SF objects/ Object describe API
    """
    sf_qualified_api_name = func_job_config['api_parameter'][
        "sf_qualified_api_name"]
    obj_full_incremtal = ("I" == func_job_config[
        'job_parameter']["full_incremental"])
    increm_key = func_job_config['job_parameter']["index_key"]
    increm_default = func_job_config['job_parameter']["incremental_default"]
    logger.info(f"incremental {obj_full_incremtal}")
    from_data = sf_runner.get_incremental_value(
        s3_client, func_job_config['job_parameter']['job_process_folder'],
        func_job_config['job_parameter']['job_process_file'],
        increm_default)
    
    if not obj_full_incremtal:
        from_data = increm_default
        logger.info(f"Full_load default {increm_default} from data {from_data}")
    logger.info(f"incremental {obj_full_incremtal} and inc_key_value: {from_data}")
    success, obj_metadata = sf_runner.api_config_builder(
        job_config, sf_qualified_api_name, func_job_config,
        full_incremental=obj_full_incremtal, fields_list=fields_list,
        incremental_default=increm_default, incremental_key=increm_key,
        incremental_key_value=from_data,
        )
    job_config['job_parameter']["schema_output_s3"] = func_job_config['job_parameter']["schema_output_s3"]
    job_config['job_parameter']["output_s3"] = func_job_config['job_parameter']["output_s3"]
    if not success:
        return success, None
    return success, obj_metadata


def get_sf_object_desc(sf_func_name,
                       sf_obj_desc_config,
                       sf_data_processor,
                       http_client=None
                       ):
    try:
        http_client = HTTPClient(sf_obj_desc_config)
        sf_obj_desc_response, success, status_code = http_client.run()
        
        if not success:
            logger.error(f"Obj Desc API request failed for {sf_func_name}. Exiting.")
            return None, False
        
        if sf_obj_desc_response is None:
            logger.error(f"Obj Desc API request failed for {sf_func_name}. Exiting.")
            return None, False
        
        obj_desc_fields = sf_data_processor.get_fields_of_desc_api(
            sf_obj_desc_response)
        return obj_desc_fields, True
    except Exception as e:
        logger.info(f"Error during obj desc: {e}")
        return None, False


def main():
    # Extract the arguments into Glue Job
    args = getResolvedOptions(
        # "start_date", "end_date"
        sys.argv, ["JOB_NAME", "source_name", "application_name", "function_name", "metadata_table_name", "start_date", "end_date","load_type"]
    )

    # start of config parse into the job
    # source identifier 
    root_source_name = args["source_name"]
    app_name = args["application_name"]
    source_name = f"{app_name}_{root_source_name}"
    # drive different sf table ingst through function_name
    sf_function_name = args["function_name"]
    # config identifier for each sf table from DynamoDB
    metadata_table_name = args["metadata_table_name"]
    job_name = args["JOB_NAME"]
    # job_run_id = args["JOB_RUN_ID"]
    delta_start_date = parse(args["start_date"])
    delta_end_date = parse(args["end_date"])
    load_type = args["load_type"]
   
    # setup spark and glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job_identifier_name = args['JOB_NAME'] + source_name + sf_function_name
    job.init(job_identifier_name, args)
    
    logger.info(f"Ingestion Job execution started for {job_identifier_name}")

    # Convert to Australia/Melbourne timezone
    melbourne_tz = pytz.timezone("Australia/Melbourne")
    delta_start_date = delta_start_date.astimezone(melbourne_tz)
    delta_end_date = delta_end_date.astimezone(melbourne_tz)
    delta_start_date = delta_start_date.strftime("%Y-%m-%d %H:%M:%S")
    delta_end_date = delta_end_date.strftime("%Y-%m-%d %H:%M:%S")
    job_start_time = generate_timestamp_string(
        timezone=TIMEZONE_SYDNEY
    ).strftime(TIMESTAMP_FORMAT)
    logger.info(f"====job for {source_name} Start time is :{job_start_time} =====")

    # Following the Sort Key for source for DynamoDB Fetch
    ddb_auth_metadata_key = "api#" + source_name + "#" + "extract_api"
    ddb_describe_metadata_key = "api#" + source_name + "#" + "describe"

    ddb_obj_metadata_key = "api#" + source_name + "#" + sf_function_name

    # Read Metadata from Dynamodb based on ddb_metadata_key
    sf_ddb = DynamoDB(metadata_table_name=metadata_table_name,
                      default_region=REGION)
    sf_metadata = sf_ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=ddb_auth_metadata_key
    )
    # Fill CRP-SALESFORCE generic/base metadata
    raw_bckt = sf_metadata['job_parameter']['bucket_name']
    # s3_app_prefix = sf_metadata["job_parameter"]["output_s3"]
    kms_key = sf_metadata["job_parameter"]["kms_key_id"]

    s3_client = S3(raw_bckt, REGION)

    sf_data_processor = Salesforce("dev")
    logger.info("==================Fetching DB Secrets ============")
    secret_params = json.loads(get_secret(
        sf_metadata['auth_api_parameter']['secret_key'], REGION))
    client_id = secret_params.get("client_id")
    client_secret = secret_params.get("client_secret")
    uname = secret_params.get("username")
    password = secret_params.get("password")
    grant_type = secret_params.get("grant_type")

    sf_metadata['auth_api_parameter'][
        'auth_body']['client_id'] = client_id
    sf_metadata['auth_api_parameter'][
        'auth_body']['client_secret'] = client_secret
    sf_metadata['auth_api_parameter'][
        'auth_body']['username'] = uname
    sf_metadata['auth_api_parameter'][
        'auth_body']['password'] = password
    sf_metadata['auth_api_parameter'][
        'auth_body']['grant_type'] = grant_type
    logger.info(f"Job Config Details  - {sf_metadata}")

    # SalesForce Function/Object specific config
    function_config = sf_ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=ddb_obj_metadata_key
    )
    # Fill Function/Object specific metadata start
    table_name = function_config["job_parameter"]["bucket_data_source_prefix"]
    output_s3 = function_config["job_parameter"]["output_s3"]
    # Function specific S3 absolute path of raw temp root folder
    inpt_raw_tmp_path = function_config["job_parameter"]["temp_output_path"]

    obj_process_folder = function_config["job_parameter"]["job_process_folder"]
    processor_file = function_config["job_parameter"]["job_process_file"]
    if load_type == 'F':
        function_config['job_parameter']["full_incremental"]='F'
    is_data_exist = True
    ingest_success = False
    api_start_time = sf_data_processor.get_formated_api_time(melbourne_tz)

    func_obj_api_fields = None
    try:
        if not function_config["job_parameter"]["is_named_col"]:
            sub_function = "describe"
            logger.info(f"===SalesForce {sub_function} API Invocation Start=.")
            func_obj_desc_api_config = sf_ddb.get_metadata_from_ddb(
                source_system_id=source_name,
                metadata_type=ddb_describe_metadata_key
            )
        
            success, desc_api_config = sf_data_processor.api_config_builder(
                sf_metadata,
                "describe",
                func_obj_desc_api_config,
                sf_object_name=sf_function_name
            )

            func_obj_api_fields, success = get_sf_object_desc(sf_function_name,
                                                              desc_api_config,
                                                              sf_data_processor
                                                              )
        else:
            func_obj_api_fields = function_config[
                "job_parameter"]["named_columns"]

        if func_obj_api_fields is None:
            logger.error(f"Error in getting columns info {sf_function_name}")
            logger.error("SF Job for {sf_function_name} Failed. Exiting...")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error in columns info for {sf_function_name} error {e}")
        sys.exit(1)
    
    success, sf_obj_config = get_sf_soql_config(
        sf_metadata,
        function_config,
        s3_client,
        sf_data_processor,
        func_obj_api_fields)
            
    if not success:
        logger.error(f"Error while getting API columns of {sf_function_name}")
        logger.error("Ingestion Job Failed........")
        sys.exit(1)

    sf_metadata = sf_obj_config
    while is_data_exist:
        try:
            logger.info(f" ===SalesForce {sf_function_name} API Start===.")
            http_client = HTTPClient(sf_metadata)
            sf_api_response, success, api_status_code = http_client.run()
        
            if success:

                if not (len(sf_api_response["records"]) > 0):
                    exit_message = f"No latest records {sf_function_name} API. So stopping job"
                    logger.info(
                        f"{exit_message}"
                    )
                    is_data_exist = False
                    job.commit()
                    os._exit(0)
                
                sf_data = sf_api_response
                arguments = {
                    "process_folder": obj_process_folder,
                    "process_file": processor_file
                }
                tmp_s3_complete, tmp_path, is_data_exist, next_data_url = sf_data_processor.pre_process_soql_data(
                    source_name=source_name,
                    sf_object_name=sf_function_name,
                    input_tmp_folder=inpt_raw_tmp_path,
                    sf_api_response=sf_data,
                    s3_client=s3_client,
                    kms_key_id=kms_key,
                    process_folder=obj_process_folder,
                    process_file=processor_file,
                    )
                
                if tmp_s3_complete:
                    ingest_success = success
                    logger.info(
                        f"Sucessfull completion of {sf_function_name} into tmp s3 {tmp_path}"
                    )

                    logger.info(f"is next data exist: {is_data_exist}")

                    if is_data_exist:
                        logger.info(
                            f"Still data exists for {sf_function_name} in nextrecordurl: {next_data_url}"
                        )

                        # call API agan with the nextrecordurl to get next set of data
                        sf_metadata = sf_data_processor.build_next_chunk_api(sf_metadata,next_data_url)

                    else:
                        logger.info(
                            f"Ingestion of all data Sucessfull for {sf_function_name} into tmp s3 {tmp_path}"
                        )
                else:
                    logger.info(
                        f"Failure of {sf_function_name} into tmp s3 {tmp_path}"
                    )
                    is_data_exist = False
                    sys.exit(1)
            else:
                logger.error(
                    f"API call to {sf_function_name} failed with status code: {api_status_code}"
                )
                ingest_success = False
                break

        except Exception as e:
            logger.error(f"API call to {sf_function_name} failed unexpectedly with error: {e}")
            sys.exit(1)
    
    try:
        if ingest_success:
            ingest_success = sf_data_processor.json_to_parquet(
                glueContext,
                source_name,
                raw_bckt,
                inpt_raw_tmp_path,
                table_name,
                output_s3,
                sampling_fraction=sf_metadata["job_parameter"]["sampling_fraction"],
                sampling_seed=sf_metadata["job_parameter"]["sampling_seed"],
                sampling_path=sf_metadata["job_parameter"]["schema_output_s3"],
                )
            
            s3_client.delete_folder_from_s3(inpt_raw_tmp_path)
            if ingest_success:
                sf_data_processor.update_last_updated(
                    s3_client,
                    obj_process_folder,
                    processor_file,
                    kms_key,
                    api_start_time
                    )
                logger.info(f"Ingestion Job for {sf_function_name} Success at json2parquet conversion")
                logger.info("Ingestion Job Success........")
                
            else:
                logger.error(f"Ingestion Job for {sf_function_name} failed at json2parquet conversion")
                logger.error("Ingestion Job Failed........")
                sys.exit(1)
        else:
            s3_client.delete_folder_from_s3(inpt_raw_tmp_path)
            logger.error(f"Ingestion Job for {sf_function_name} failed at json2parquet conversion")
            logger.error("Ingestion Job Failed........")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Ingestion Job for {sf_function_name} failed at json2parquet conversion with Error {e}")
        s3_client.delete_folder_from_s3(inpt_raw_tmp_path)
        sys.exit(1)
       
    job.commit()


if __name__ == "__main__":
    main()
