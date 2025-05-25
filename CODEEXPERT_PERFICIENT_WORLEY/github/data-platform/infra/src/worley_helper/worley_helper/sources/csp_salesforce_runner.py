import os
import json
import pytz
from datetime import datetime
import logging
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from pyspark.sql.functions import col, explode

from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import S3
from worley_helper.utils.constants import DATETIME_FORMAT, TIMEZONE_SYDNEY, TIMEZONE_UTC
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.date_utils import generate_timestamp_string

# Init the logger
#logger = get_logger(__name__)
logger = logging.getLogger()

# Salesforce process class
class Salesforce:
    """A class to manage all Salesforce related data activities"""
    def __init__(self, env):
        self.run_env = env

    # Function to build api config based on Function names
    def api_config_builder(self, sf_base_api_config,
                           function_name, function_api_config,
                           **kwargs):
        """ Salesforce API builder for respective objects

        Args:
            sf_base_api_config (dict): Base auth/generic metadata in dynamodb
                                       and will be filled with object api info
            function_name (str): Salesforce Object identifier
            function_api_config (dict): function_name object api metadata

        Returns:
            status (boolean): indicator whether the api config is generated
            api_config (dict): fully formated api config for respective object
        """
        SUCCESS = True
        FAILURE = False
        logger.info(f"Building api parameters for {function_name} function.")
        try:
            sf_instance_url = sf_base_api_config['api_parameter']["instance_url"]
            endpoint = function_api_config['api_parameter']["endpoint"]
            func_api_version = sf_base_api_config['api_parameter']["api_version"]
            
            if function_name == "describe":
                endpoint = endpoint.format(
                    instance_url=sf_instance_url,
                    api_version=func_api_version,
                    sf_object_name=kwargs["sf_object_name"]
                    )
                
            else:
                full_incr_ind = kwargs["full_incremental"]
                obj_inc_key = kwargs["incremental_key"]
                api_fields_list = kwargs["fields_list"]
                obj_inc_default = kwargs["incremental_default"]
                obj_incremental_key_value = kwargs["incremental_key_value"]
                    
                success, function_soql_query = self.get_sf_soql_query(
                    function_name,
                    full_incremental=full_incr_ind,
                    incremental_key=obj_inc_key,
                    fields_list=api_fields_list,
                    incremental_default=obj_inc_default,
                    incremental_key_value=obj_incremental_key_value,
                    )

                if not success:
                    logger.error(f"Error during API config builder for {function_name}")
                    return FAILURE, None

                endpoint = endpoint.format(
                    instance_url=sf_instance_url,
                    api_version=func_api_version,
                    soql_query=function_soql_query
                    )
            
            sf_base_api_config['api_parameter']['endpoint'] = endpoint

            return SUCCESS, sf_base_api_config
        except Exception as e:
            logger.error(f"Error during the config builder \
                         for {function_name} and error {e}")
            return FAILURE, None

    def get_sf_soql_query(
            self,
            sf_object_name: str,
            **kwargs
            ):
        """Prepare the Salesforce Odata query for the
           query api param for the sf_object_name API object

        Args:
            sf_object_name (str): Salesforce API object name

        Returns:
            str: return the formated soql query for the API
        """
        SUCCESS = True
        FAILURE = False
        try:
            table_name = sf_object_name
            is_incremental = kwargs["full_incremental"]
            increment_key = kwargs["incremental_key"]
            fields_list = kwargs["fields_list"]
            # incremental_default = kwargs["incremental_default"]
            incremental_value = kwargs["incremental_key_value"]
            
            full_load_query = f"SELECT+{fields_list}+FROM+{table_name}"
            incremental_query = full_load_query
            if is_incremental:
                incremental_query = f"{full_load_query}+WHERE+{increment_key}>{incremental_value}"
            # soql_query = f"{soql_query}+ORDER+BY+{increment_key}+DESC"
            return SUCCESS, incremental_query
        except Exception as e:
            logger.error(f"Error soql_query builder {sf_object_name} is {e}")
            return FAILURE, None

    def get_fields_of_desc_api(self, desc_api_response: dict):
        """ Salesforce Describe API parser to extract all the fields
            associated for a salesforce Object in Salesforce CRM.
            Creates a named columns string that will be used for the
            respective Salesforce Object API.

        Args:
            desc_api_response (dict): Describe API response to be parsed

        Returns:
            str: Named columns string that will be used for
                 the respective Salesforce Object API.
        """
        fields_list = ",".join(field['name'] for field in desc_api_response['fields'])
        return fields_list

    def pre_process_soql_data(
            self,
            source_name: str,
            sf_object_name: str,
            input_tmp_folder: str,
            sf_api_response: dict,
            s3_client: S3,
            kms_key_id: str,
            **kwargs
            ):
        """ Salesforce Object Query(soql) API response parser
            for respective salesforce Object(sf_object_name) in Salesforce CRM.
            Extract the response and stores in json format in temp raw bucket
            in respective Salesforce Object folder.

        Args:
            source_name (str): Salesforce application is derived as
                               application_salesforce
            sf_object_name (str): Salesforce API object name
            input_tmp_folder (str): S3 temp folder uri in raw bucket
            sf_api_response (dict): response from API_OBJECT response for
                                    sf_object_name named object
            s3_client (S3): Raw Bucket S3 client
            kms_key_id (str): S3 bucket KMS_Key_id for activities

        Returns:
            s3_tmp_upload_complete (boolean): status of the resonse storage
            temp_full_path (str): S3 temp folder uri in raw bucket
            is_data_exist (boolean): pagination indicator for all data received
            next_record_url (str): Next pagination total get url
        """
        now = datetime.now()
        file_name_extension = now.strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f'{sf_object_name}_{file_name_extension}.json'
        temp_full_path = f"{input_tmp_folder}/{file_name}"
        
        sf_resp_json = sf_api_response
        sf_response_bytes = json.dumps(sf_resp_json).encode('utf-8')

        s3_tmp_upload_complete = s3_client.upload_to_s3(
            sf_response_bytes,
            temp_full_path,
            kms_key_id,
            is_gzip=False
            )
        
        if s3_tmp_upload_complete:
            logger.info(f"Uploaded {sf_object_name} data to {temp_full_path}")
        else:
            logger.info(f"{sf_object_name} API data in s3 temp not success")
            return s3_tmp_upload_complete, temp_full_path, True, None
        
        # Read from the S3 location to update if exist
        obj_process_folder = kwargs["process_folder"]
        obj_process_file = kwargs["process_file"]
        folder_name = f"{obj_process_folder}"
        file_name = f"{obj_process_file}"
        last_process_info = s3_client.read_s3_file_as_json(
            folder_name,
            file_name
            )

        if not last_process_info:
            last_process_info = {
            }
        
        done = sf_resp_json["done"]
        next_record_url = None if done else sf_resp_json["nextRecordsUrl"]
        is_data_exist = not done
        logger.info(f"is data done: {done},is data exist flg: {is_data_exist}")
        logger.info(f" next data url is {next_record_url}")
        return s3_tmp_upload_complete, temp_full_path, is_data_exist, next_record_url
    
    def json_to_parquet(self,
                        glue_context: GlueContext,
                        source_name: str,
                        bucket_name: str,
                        tmp_raw_folder: str,
                        table_name: str,
                        output_s3_path: str,
                        **kwargs
                        ):
        """Convertor from json to parquet format to further process

        Args:
            glue_context (GlueContext): glue context
            source_name (str): Salesforce application is derived as
                               application_salesforce
            bucket_name (str): S3 Raw Bucket name
            tmp_raw_folder (str): S3 temp folder uri in raw bucket
            table_name (str): Salesforce API object name use for prefix of
                              SF object data
            output_s3_path (str): Output path to store the parquet

        Returns:
            status (boolean): completion indicator of the conversion activity
        """
        try:
            stage_path = f"s3://{bucket_name}/{tmp_raw_folder}"
            logger.info(stage_path)

            # Convert JSON to a DataFrame
            tf_ctxt = source_name + table_name

            dynamic_frame = glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    'paths': [stage_path],
                    'recurse': True,
                    'groupFiles': 'inPartition',
                    'groupSize': '1048576',
                    'format_options': {
                        'jsonPath': '$[records]',
                        'multiline': True
                    }
                },
                format="json",
                transformation_ctx=tf_ctxt
            )       
            dynamic_frame.printSchema() 
            json_df = dynamic_frame.toDF()

            exploded_records_df = json_df.select(explode("records").alias(""))

            mapped_df = DynamicFrame.fromDF(exploded_records_df,
                                            glue_context,
                                            "dynamic_frame")

            total_count = mapped_df.count()
            logger.info(f" source total count from raw s3 {total_count}")
            # 1 million records per partition
            num_partition = round(mapped_df.count()/1000000)+1
            # will need to pass this through config for each functions
            repartitioned_df = mapped_df.repartition(
                num_partition)

            logger.info("==========start listing repartiioned schema======")

            repartitioned_df.printSchema()

            logger.info("=========End listing repartiioned schema========")

            logger.info("================ Start of Relationalize Trnasform===")
            relationalized_data_path = f"{source_name}/temp/{table_name}/relationalized_data"

            relationalized_df = Relationalize.apply(
                frame=repartitioned_df,
                staging_path=relationalized_data_path,
                name=f"{source_name}",
                transformation_ctx=f"relationalize__{source_name}__{table_name}"
            )
            logger.info("Relationalize Transform done")

            logger.info(relationalized_df)

            logger.info(relationalized_df.keys())

            tables = {}
            for name_df in relationalized_df.keys():
                if name_df != '':
                    logger.info("processing the tag --> " + name_df)
                    tables[name_df] = relationalized_df.select(name_df).toDF()

            logger.info("post relationalize tables below")
            logger.info(tables)

            for tables_name, df in tables.items():
                logger.info(f"Relation Table Name: {tables_name} and schema for it below ")
                df.printSchema()
            
            if source_name in tables:
                main_df = tables[source_name]
            else:
                main_df = None
            
            logger.info(f"Source {source_name} filtered df schema below")
            main_df = main_df.drop("attributes.type", "attributes.url")
            main_df.printSchema()

            dynamic_frame = DynamicFrame.fromDF(main_df, glue_context, "dynamic_frame")
            partition_date = generate_timestamp_string(
                timezone=TIMEZONE_SYDNEY).strftime(DATETIME_FORMAT)
            partition_str = f"{get_partition_str_mi(partition_date)}"

            # Path to where you want to store Parquet files
            output_parquet_path_uri = f"{output_s3_path}/{partition_str}/"
            output_parquet_path = f"s3://{bucket_name}/{output_parquet_path_uri}"

            logger.info(f"Writing to Parquet in S3 {output_parquet_path}")
            # Write partitioned data to S3
            success = write_glue_df_to_s3_with_specific_file_name(
                glue_context,
                dynamic_frame,
                bucket_name,
                output_parquet_path_uri,
                tf_ctxt,
                typecast_cols_to_string = True
            )

            sampling_fraction = float(
                kwargs['sampling_fraction'])
            sampling_seed = kwargs['sampling_seed']
            sampling_path = kwargs['sampling_path']

            sample_data = (
                dynamic_frame.toDF()
                .sample(withReplacement=False,
                        fraction=sampling_fraction,
                        seed=sampling_seed)
                )
            # adjust the fraction as needed
            logger.info(f"Selected sample data {source_name}")
            success = write_glue_df_to_s3_with_specific_file_name(
                glue_context,
                DynamicFrame.fromDF(sample_data, glue_context, "sample_data"),
                bucket_name,
                sampling_path,
                tf_ctxt
                )
            logger.info(f"completed for the sample data {table_name}")
            return success
        except Exception as e:
            logger.info(F"Error in proecessing data with error {e}")
            return False

    def build_next_chunk_api(self,
                             sf_metadata,
                             next_record_url
                             ):
        """Next Pagination API builder

        Args:
            sf_metadata (dict): Base API_OBJECT request for
                                sf_object_name named object
            next_record_url (str): next pagination url

        Returns:
            dict: Next pagination formated API config
        """
        sf_instance_url = sf_metadata['api_parameter']["instance_url"]
        sf_metadata["api_parameter"]["endpoint"] = f"{sf_instance_url}{next_record_url}"
        sf_metadata["api_parameter"]["api_method"] = "get"
        sf_metadata["api_parameter"]["api_query_params"] = {}

        return sf_metadata

    def get_incremental_value(self,
                              s3_client: S3,
                              folder_name: str,
                              process_file: str,
                              incremental_default=None
                              ):
        """Retrieve last successful value that supports incremental loadtype
           in API

        Args:
            s3_client (S3): Raw Bucket S3 client
            folder_name (str): process folder name
            process_file (str): SF object process file that
                                stores the incremental value
            incremental_default (str, optional): default incremental value
                                                 to start the full load.
                                                 Defaults to None.

        Returns:
            _type_: _description_
        """
        next_run_value = incremental_default
        try:
            # TODO Read from the S3 location to update if exist
            last_process_info = s3_client.read_s3_file_as_json(folder_name,
                                                               process_file)
            
            if last_process_info:
                next_run_value = last_process_info["lastSuccessProcessedTime"]
            
            if not next_run_value:
                next_run_value = incremental_default
            
            return next_run_value
        except Exception as e:
            logger.error(f"Error retrieving process audit file from s3 {e}")
            return incremental_default
    
    def update_last_updated(self,
                            s3_client: S3,
                            process_folder: str,
                            process_file: str,
                            kms_key_id: str,
                            last_update_time: str
                            ):
        """Store the last successful filter value that supports
           incremental loadtype in API

        Args:
            s3_client (S3): Raw bucket S3 client
            process_folder (str): process folder name
            process_file (str): SF object process file that
                                stores the incremental value
            kms_key_id (str): S3 bucket KMS_Key_id for activities
            last_update_time (str): successful incremental value

        Returns:
            boolean: status of storage of incremental value in s3
        """
        file_path = f"{process_folder}/{process_file}"
        
        FAILURE = False
        try:
            # TODO Read from the S3 location to update if exist
            last_process_info = s3_client.read_s3_file_as_json(
                process_folder,
                process_file)
            
            if not last_process_info:
                last_process_info = {}
            
            formated_increment_value = last_update_time
            last_process_info["lastSuccessProcessedTime"] = formated_increment_value
            
            return s3_client.upload_to_s3(json.dumps(last_process_info),
                                          file_path,
                                          kms_key_id,
                                          is_gzip=False
                                          )
        except Exception as e:
            logger.error(f"Error retrieving process audit file from s3 {e}")
            return FAILURE

    def get_formated_api_time(self, timez=TIMEZONE_UTC):
        """Salesforce supported incremental key values

        Args:
            timez (str, optional): time zone . Defaults to TIMEZONE_UTC.

        Returns:
            str: Salesforce supported formated incremental value
        """
        utc_tz = pytz.timezone('UTC')
        utc_time = datetime.now(pytz.utc).replace(tzinfo=utc_tz)
        salesforce_last_updated_time = "{}.{:03d}{}".format(
            utc_time.strftime('%Y-%m-%dT%H:%M:%S'),
            utc_time.microsecond//1000000,
            utc_time.strftime("%z"))
        last_updated_time = salesforce_last_updated_time.replace("+", "%2B")
        logger.info(f"formated incremental key value - {last_updated_time}")
        return last_updated_time
