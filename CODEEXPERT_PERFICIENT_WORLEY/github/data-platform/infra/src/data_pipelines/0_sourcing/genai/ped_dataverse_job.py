import sys
import json
import os
from datetime import datetime
import pytz
import urllib.parse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from dateutil.parser import parse
import pyspark.sql.functions as f
import pyspark.sql as DataFrame
from typing import Optional, Dict, List, Any, Union

from worley_helper.utils.logger import get_logger
from worley_helper.utils.constants import REGION, TIMEZONE_SYDNEY, TIMESTAMP_FORMAT
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.utils.genai_http_api_client import HTTPClient
from worley_helper.sources.ped_runner import PED

import requests

# Init the logger
logger = get_logger(__name__)


def get_ped_obj_incremental_value(
        ped_obj_name: str,
        incremental_default: str,
        s3_obj_folder_prefix: str,
        process_file: str,
        s3_client: S3
        ):
    """get the last incremental timestmap from the storage
    to support incremental load type in API.

    Args:
        ped_obj_name (str): object/table name in the PED Dataverse
        incremental_default (str): historical load start time for API
        s3_obj_folder_prefix (str): raw bucket S3 folder name for a source
        process_file (str): process file that stores incremental info
        s3_client (S3): S3 Raw bucket object

    Returns:
        str: last successful processed API data if exists or else default
    """
    file_name = f"{process_file}"
    next_run_value = incremental_default
    try:
        # Read from the S3 location to update if exist
        last_process_info = s3_client.read_s3_file_as_json(
            s3_obj_folder_prefix, file_name)
            
        if last_process_info and ("lastSuccessProcessedTime"):
            next_run_value = last_process_info["lastSuccessProcessedTime"]
            
        if not next_run_value:
            next_run_value = incremental_default
        logger.info(f"{ped_obj_name} incremental value")   
        return next_run_value
    except Exception as e:
        logger.error(f"Error retrieving process audit file from s3 {e}")
        return incremental_default


def prepare_ped_api_param(
        ped_obj_name: str,
        job_config: dict,
        ped_obj_config: dict,
        s3_client: S3
        ):
    """Prepare the required API config based on the object
       through the param ped_obj_name. This will get replace
       the necessary endpoints and api params respectively.
       So that it wil be used for Http_Client class for API ready.

    Args:
        ped_obj_name (str): PED OBject/table identifier
        job_config (dict): Main base/generic API config
        ped_obj_config (dict): Next API config that needs to be called.
        s3_client (S3): S3 Client wrapper

    Returns:
        dict: Formated API config with respective changes
    """
    obj_full_incremtal = "I" == ped_obj_config[
        "job_parameter"]["full_incremental"]
    logger.info(f"incremental {obj_full_incremtal}")
    increm_default = ped_obj_config["job_parameter"]["incremental_default"]
    processor_folder_path = ped_obj_config[
        "job_parameter"]["job_process_folder"]
    processor_file = ped_obj_config["job_parameter"]["job_process_file"]
    from_data = get_ped_obj_incremental_value(
        ped_obj_name,
        increm_default,
        processor_folder_path,
        processor_file,
        s3_client)
    if not obj_full_incremtal:
        from_data = increm_default
        logger.info(f"Full_load default {increm_default} for {from_data}")
    logger.info(f"incremental {obj_full_incremtal} inc_key_value: {from_data}")
    # Prepare the Dataverse API params for filter
    ped_filter_condition = str(ped_obj_config["api_parameter"][
        "api_query_params"]["$filter"])
    ped_filter_condition = ped_filter_condition.format(
        incremental_value=from_data)
    ped_obj_config["api_parameter"][
        "api_query_params"]["$filter"] = ped_filter_condition
    job_config["api_parameter"]["api_query_params"] = ped_obj_config[
        "api_parameter"]["api_query_params"]
    logger.info(f"ped src {ped_obj_name}")
    # Prepare the Dataverse API params for endpoint
    ped_tenant_id = job_config["job_parameter"]["tenant_id"]
    ped_auth_endpoint = job_config["auth_api_parameter"]["endpoint"]
    ped_auth_endpoint = ped_auth_endpoint.format(tenant_id=ped_tenant_id)
    job_config["auth_api_parameter"]["endpoint"] = ped_auth_endpoint
    ped_api_uri = ped_obj_config["api_parameter"]["endpoint"]
    api_obj_name = ped_obj_config["api_parameter"]["ped_qualified_api_name"]
    ped_instance_id = job_config["job_parameter"]["ped_instance_id"]
    ped_api_uri = ped_api_uri.format(
        ped_instance_id=ped_instance_id,
        api_obj_name=api_obj_name
        )
    logger.info(f"ped src {ped_obj_name} endpoint - {ped_api_uri}")
    job_config["api_parameter"]["endpoint"] = ped_api_uri
    return job_config, ped_obj_config


def invoke_webhook_notification_process(
        job_run_id,
        metadata_table_name,
        source_name,
        ddb_notfication_system_id,
        notf_ddb_metatype,
        s3_output_folder_uri,
        s3_formated_folder_name,
        job_start_time,
        genai_project,
        no_of_files=1
        ):
    
    SUCCESS = True
    FAILURE = False
    try:
        ddb = DynamoDB(metadata_table_name=metadata_table_name, 
                       default_region=REGION)
        webhook_config = ddb.get_metadata_from_ddb(
            source_system_id=ddb_notfication_system_id,
            metadata_type=notf_ddb_metatype)
        secret_params = json.loads(get_secret(
            webhook_config['api_parameter']['secret_key'], REGION))
        ssl_verify=webhook_config['api_parameter']['api_ssl_verify']
        if ssl_verify == False:
            logger.info(f'SSL verify flag is set to false.')
        else:
            logger.info(f'SSL verify is true. Download ssl pem certificate from secret manager {ssl_verify}.')
            ssl_cert=get_secret(ssl_verify, REGION)
            ca_bundle_path = '/tmp/custom-ca-bundle.pem'
            with open(ca_bundle_path,'w') as file:
                file.write(ssl_cert)
            webhook_config['api_parameter']['api_ssl_verify']=ca_bundle_path
            logger.info(f"SSL verify updated with CA bundle path")

        webhook_auth_token = secret_params["Authorization"]
        webhook_config["api_parameter"]["api_headers"]["Authorization"] = webhook_auth_token

        webhook_config["api_parameter"]["api_body"]["requestNumber"] = job_run_id
        webhook_config["api_parameter"]["api_body"]["folderPath"] = s3_output_folder_uri
        webhook_config["api_parameter"]["api_body"]["timestamp"] = job_start_time
        webhook_config["api_parameter"]["api_body"]["filesCount"] = f"{no_of_files}"
        webhook_config["api_parameter"]["api_body"]["folderName"] = source_name
        webhook_config["api_parameter"]["api_body"]["subFolderName"] = s3_formated_folder_name
        webhook_config["api_parameter"]["api_body"]["project"] = genai_project
        #raw_bckt = webhook_config["job_parameter"]["bucket_name"]
        httpclient = HTTPClient(webhook_config)
        api_response, success, api_status_code = httpclient.run()
        # TODO need to update in audit file
        if not success:
            return FAILURE, None
        
        return SUCCESS, api_response
    except Exception as e:
        logger.error(
            f"Failure during call of webhook for {source_name} with error {e}"
        )
        return FAILURE, None


def main():
    # Parse the arguments into Glue Job
    args = getResolvedOptions(
        # "start_date", "end_date"
        sys.argv,
        [
            "JOB_NAME",
            "source_name",
            "function_name",
            "metadata_table_name",
            "start_date",
            "end_date"
        ],
    )

    # start of config parse into the job
    # source identifier 
    source_name = args["source_name"]
    # drive different sf table ingst through function_name
    ped_obj_name = args["function_name"]
    # config identifier for each sf table from DynamoDB
    metadata_table_name = args["metadata_table_name"]
    job_name = args["JOB_NAME"]
    job_run_id = args["JOB_RUN_ID"]
    delta_start_date = parse(args["start_date"])
    delta_end_date = parse(args["end_date"])

    
    
    # setup spark and glue context
    conf = SparkConf()
    conf.set(f"spark.rpc.message.maxSize", "512")
    sc = SparkContext(conf=conf)
    
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job_identifier_name = args['JOB_NAME'] + source_name + ped_obj_name
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
    logger.info(f"==================job Start time is :{job_start_time} =====")

    # Following the Sort Key for source for DynamoDB Fetch
    ddb_api_metadata_key = "api" + "#" + source_name + "#" + "extract_api"

    # Read Metadata from Dynamodb based on ddb_metadata_key
    ped_ddb = DynamoDB(metadata_table_name=metadata_table_name,
                       default_region=REGION)
    ped_config = ped_ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=ddb_api_metadata_key
    )

    # Fill CRP-PED generic/base metadata
    raw_bckt = ped_config["job_parameter"]["bucket_name"]
    kms_key = ped_config["job_parameter"]["kms_key_id"]
    s3_client = S3(raw_bckt, REGION)

    ped_data_processor = PED("dev")

    logger.info("==================Fetching DB Secrets ============")
    secret_params = json.loads(get_secret(
        ped_config['auth_api_parameter']['secret_key'], REGION))
      
    client_id = secret_params.get("client_id")
    client_secret = secret_params.get("client_secret")

    ped_config['auth_api_parameter'][
        'auth_body']['client_id'] = client_id
    ped_config['auth_api_parameter'][
        'auth_body']['client_secret'] = client_secret

    # Call the API with retrying if needed
    logger.info(f"Job Config Details  - {ped_config}")
    # Fill Function/Object specific metadata end
    # Function specific S3 absolute path of raw temp root folder
    s3_output_folder_name_format = ped_config[
        "job_parameter"]["batch_foldername_format"]
    s3_output_folder_uri = ped_config[
        "job_parameter"]["output_files_path"]
    s3_output_path_uri, s3_formated_folder_name = ped_data_processor.get_batch_s3_output_uri(
        s3_output_folder_uri,
        s3_output_folder_name_format,
        job_start_time,
        TIMESTAMP_FORMAT
        )
    ped_process_folder = ped_config[
        "job_parameter"]["job_process_folder"]
    ped_process_file = ped_config[
        "job_parameter"]["job_process_file"]
    ped_sourcing_entity_list = ped_config["job_parameter"][
        "sourcing_entity_lists"]
    api_start_time = ped_data_processor.get_formated_api_time(melbourne_tz)
    is_transforms_exist = ped_config["job_parameter"][
        "is_transform_step_rquired"]
    for entity in ped_sourcing_entity_list:
        print("ped entity-->", entity)

        # Following the Sort Key for source for DynamoDB Fetch
        ddb_obj_metadata_key = "api" + "#" + source_name + "#" + entity

        ped_obj_config = ped_ddb.get_metadata_from_ddb(
            source_system_id=source_name, metadata_type=ddb_obj_metadata_key)
        ped_obj_name = entity
        # PED Function/Object specific config
        ped_config, function_config = prepare_ped_api_param(
            ped_obj_name,
            ped_config,
            ped_obj_config,
            s3_client
            )
        # Fill Function/Object specific metadata start
        table_name = function_config["job_parameter"]["output_s3"]
        inpt_raw_tmp_path = function_config["job_parameter"]["temp_input_path"]
        tmp_raw_delete_path = ped_config["job_parameter"]["temp_clean_up_path"]
        obj_process_folder = function_config[
            "job_parameter"]["job_process_folder"]
        obj_process_file = function_config[
            "job_parameter"]["job_process_file"]
        is_data_exist = True
        ingest_success = False
        while is_data_exist:
            try:
                logger.info(f" ==PED {ped_obj_name} API Invocation Start==.")
                http_client = HTTPClient(ped_config)
                sf_api_response, success, api_status_code = http_client.run()
                if success:
                    if (len(sf_api_response["value"]) <= 0):
                        exit_message = f"No latest records for {ped_obj_name} API. So stopping job"
                        logger.info(
                            f"{exit_message}"
                        )
                        is_data_exist = False
                        os._exit(0)
                
                    ped_data = sf_api_response
                    tmp_s3_complete, tmp_path, is_data_exist, next_data_url = ped_data_processor.pre_process_api_data(
                        ped_obj_name, inpt_raw_tmp_path, ped_data,
                        s3_client, kms_key)
                    if tmp_s3_complete:
                        ingest_success = success
                        logger.info(
                            f"Sucessfull completion of {ped_obj_name} into tmp s3 {tmp_path}"
                        )

                        logger.info(f"is next data exist: {is_data_exist}")

                        if is_data_exist:
                            logger.info(
                                f"Still data exists for {ped_obj_name} next page url: {next_data_url}"
                            )

                            # call API agan with the nextrecordurl to get next set of data
                            ped_config = ped_data_processor.build_next_chunk_api(
                                ped_config,
                                next_data_url
                                )

                        else:
                            logger.info(
                                f"Ingestion of all data Sucessfull for {ped_obj_name} into tmp s3 {tmp_path}"
                            )
                    else:
                        logger.info(
                            f"Failure of {ped_obj_name} into tmp s3 {tmp_path}"
                        )
                        is_data_exist = False
                        ingest_success = False
                        #sys.exit(1)
                else:
                    logger.error(
                        f"API call to {ped_obj_name} failed with status code: {api_status_code}"
                    )
                    ingest_success = False
                    break

            except Exception as e:
                logger.error(f"API call to {ped_obj_name} failed unexpectedly with error: {e}")
                s3_client.delete_folder_from_s3(tmp_raw_delete_path)
                sys.exit(1)
        try:
            if ingest_success:
                rename_columns = function_config[
                    "job_parameter"]["api_source_to_target_rename"]
                obj_process_path = function_config[
                    "job_parameter"]["temp_output_path"]
                is_rename_order = False
                if "is_rename_ordered" in function_config["job_parameter"]:
                    is_rename_order = function_config[
                        "job_parameter"]["is_rename_ordered"]
                
                if is_rename_order:
                    source_cols_list = function_config["job_parameter"][
                        "api_source_columns"]
                    target_cols_list = function_config["job_parameter"][
                        "target_columns"]
                    src_iter = iter(source_cols_list)
                    target_iter = iter(target_cols_list)
                    rename_columns = dict(zip(src_iter, target_iter))
                transformation = None
                if "transformation" in function_config["job_parameter"]:
                    transformation = function_config[
                        "job_parameter"]["transformation"]
                ingest_success = ped_data_processor.process_api_data(
                    glue_context,
                    source_name,
                    raw_bckt,
                    inpt_raw_tmp_path,
                    table_name,
                    obj_process_path,
                    rename_columns,
                    transformation
                    )
                if ingest_success:
                    ped_data_processor.update_last_updated(
                        s3_client,
                        obj_process_folder,
                        obj_process_file,
                        kms_key,
                        api_start_time
                        )
                    logger.info(f"Ingestion Job for {ped_obj_name} Success at json2parquet conversion")
                    logger.info("Ingestion Job Success........")
                else:
                    ingest_success = False
                    logger.error(f"Ingestion Job for {ped_obj_name} failed at json2parquet conversion")
                    logger.error("Ingestion Job Failed........")
                    sys.exit(1)
            else:
                ingest_success = False
                s3_client.delete_folder_from_s3(tmp_raw_delete_path)
                logger.error(f"Ingestion Job for {ped_obj_name} failed at json2parquet conversion")
                logger.error("Ingestion Job Failed........")
                sys.exit(1)
        except Exception as e:
            ingest_success = False
            logger.error(f"Ingestion Job for {ped_obj_name} failed at json2parquet conversion with Error {e}")
            s3_client.delete_folder_from_s3(tmp_raw_delete_path)
            sys.exit(1)
    if ingest_success:
        try:
            post_merge_delete_cols = ped_config["job_parameter"][
                "transformations"]["source_delete_fields"]
            process_tmp_output_path = ped_config["job_parameter"][
                "temp_output_path"]
            final_out_path = s3_output_path_uri 
            if not is_transforms_exist:
                entity_final_path = ped_config["job_parameter"][
                    "entities_ouput_path"]
                entity_output_path_uri, entity_formated_folder_name = ped_data_processor.get_batch_s3_output_uri(
                    entity_final_path,
                    s3_output_folder_name_format,
                    job_start_time,
                    TIMESTAMP_FORMAT
                )
                final_out_path = entity_output_path_uri
            ingest_success = ped_data_processor.ped_entities_transformation(
                glue_context,
                raw_bckt,
                ped_sourcing_entity_list,
                post_merge_delete_cols,
                process_tmp_output_path,
                final_out_path,
                is_transforms_exist
            )
        except Exception as e:
            ingest_success = False
            logger.error(f"Error Ingest {ped_obj_name} failed transformation {e}")
            s3_client.delete_folder_from_s3(tmp_raw_delete_path)
            sys.exit(1)
        s3_client.delete_folder_from_s3(tmp_raw_delete_path)
        ped_data_processor.update_last_updated(
            s3_client,
            ped_process_folder,
            ped_process_file,
            kms_key,
            api_start_time
            )
    else:
        logger.error(f"Ingestion Job for {ped_obj_name} failed transformation")
        logger.error("Stopping the {ped_obj_name} Ingestion job")
        s3_client.delete_folder_from_s3(tmp_raw_delete_path)
        sys.exit(1)
    try:
        if ingest_success:
            is_notification_required = ped_config["job_parameter"]["do_notify"]
            genai_project = ped_config["job_parameter"]["genai_project_name"]

            if is_notification_required:
                logger.info(
                    f"Notification for Ingestion Job for {ped_obj_name} is enabled in metadata")
                if "notification_parameter" in ped_config["job_parameter"]:
                    notification_config = ped_config["job_parameter"]["notification_parameter"]
                    notf_ddb_system_id = notification_config["SourceSystemId"]
                    notf_ddb_metatype = notification_config["MetadataType"]
                    s3_output_absolute_folder_uri = f"s3://{raw_bckt}/{s3_output_path_uri}"
                    success, response = invoke_webhook_notification_process(
                        job_run_id,
                        metadata_table_name,
                        source_name,
                        notf_ddb_system_id,
                        notf_ddb_metatype,
                        s3_output_absolute_folder_uri,
                        s3_formated_folder_name,
                        api_start_time,
                        genai_project
                    )
                    if not success:
                        logger.error(
                            f"Notification for Ingestion Job for {ped_obj_name} failed"
                        )
                        # TODO need to send sns for failure
                        sys.exit(1)
                    else:
                        logger.info(
                            f"Notification for Ingestion Job for {ped_obj_name} success"
                        )                   
                else:
                    logger.error(
                        f"Notification for Ingestion Job for {ped_obj_name} is enabled in metadata but notification_parameter is missing"
                    )
            else:
                logger.info(
                    f"Notification for Ingestion Job for {ped_obj_name} is disabled in metadata")
        else:
            logger.error(f"Ingestion Job for {ped_obj_name} failed transformation")
            logger.error("Stopping the {ped_obj_name} Ingestion job")
            s3_client.delete_folder_from_s3(tmp_raw_delete_path)
            sys.exit(1)

    except Exception as e:
        logger.error(f"Ingestion Job for {ped_obj_name} failed at json2parquet conversion with Error {e}")
        s3_client.delete_folder_from_s3(tmp_raw_delete_path)
        sys.exit(1)
    job.commit()


if __name__ == "__main__":
    main()
