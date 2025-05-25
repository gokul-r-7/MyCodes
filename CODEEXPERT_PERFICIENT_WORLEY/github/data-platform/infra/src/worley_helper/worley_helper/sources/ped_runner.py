import os
import json
import pytz
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from pyspark.sql.functions import lit, col, explode, broadcast
from pyspark.sql.types import *

from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import S3
from worley_helper.utils.constants import REGION, TIMESTAMP_FORMAT, TIMEZONE_UTC
from worley_helper.utils.helpers import write_glue_df_to_s3_with_specific_file_name

# Init the logger
logger = get_logger(__name__)
# include aws default region variable
os.environ["AWS_DEFAULT_REGION"] = REGION


# PED process class
class PED:
    """A class to manage all PED related data activities"""
    def __init__(self, env):
        self.run_env = env

    def pre_process_api_data(
            self,
            ped_obj_name: str,
            input_tmp_folder: str,
            ped_api_response: str,
            s3_client: S3,
            kms_key_id: str
            ):
        """ PED OData API response parser for respective
            PED Object(ped_obj_name).
            Extract the response and stores in json format
            in temp raw bucket in respective PED Object folder.

        Args:
            ped_obj_name (str): PED API object/table name
            input_tmp_folder (str): S3 temp folder uri in raw bucket
            ped_api_response (str): response from API_OBJECT response for
                                    ped_object_name named object
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
        file_name = f'{ped_obj_name}_{file_name_extension}.json'
        temp_full_path = f"{input_tmp_folder}/{file_name}"
        ped_response = json.dumps(ped_api_response).encode('utf-8')

        s3_complete = s3_client.upload_to_s3(
            ped_response,
            temp_full_path,
            kms_key_id,
            is_gzip=False
            )
        if s3_complete:
            logger.info(f"Uploaded {ped_obj_name} to {temp_full_path}")
        else:
            logger.info(f"{ped_obj_name} API data in temp s3 not successfull")
            return s3_complete, temp_full_path, True, None
        
        next_records_url = None
        is_next_exist = False
        if "@odata.nextLink" in ped_api_response:
            next_records_url = ped_api_response["@odata.nextLink"]
            is_next_exist = True
        
        logger.info(f"is data exist flg: {is_next_exist}")
        logger.info(f" next data url is {next_records_url}")
        return s3_complete, temp_full_path, is_next_exist, next_records_url
    
    def __to_list(self, a):
        """CombineByKey createCombiner function

        Args:
            a (dict): value of list of key, value

        Returns:
            list: convert to list
        """
        return [a]
    
    def __append(self, a, b):
        """CombineByKey mergeValue function
           a function to merge b into a
        Args:
            a (_type_): Previous createCombiner function output
            b (_type_): Append the new value to the previous output

        Returns:
            list: updated list post append operation
        """
        a.append(b)
        return a
    
    def __extend(self, a, b):
        """CombineByKey mergeCombiners function
           function to combine two combiners into a single one
           
        Args:
            a (_type_): Previous mergeValue function output
            b (_type_): the new mergevalue

        Returns:
            list: combined mergeValue into one.
        """
        a.extend(b)
        return a
    
    def process_api_data(
            self, glue_context: GlueContext,
            source_name: str,
            bucket_name: str,
            tmp_raw_folder: str,
            table_name: str,
            s3_output_path_uri: str,
            rename_cols: dict,
            transformations=None
            ):
        """ Take the PED data from the tmp_raw_folder
            from respective PED Object(source_name).
            Apply the transformation on the defined transformations
            which is maintained in metadata.

        Args:
            glue_context (GlueContext): glue context
            source_name (str): PED application name
            bucket_name (str): S3 Raw Bucket name
            tmp_raw_folder (str): S3 temp folder uri in raw bucket
            table_name (str): PED object/table name
            s3_output_path_uri (str): S3 path that will be used for Curation.
            rename_cols (dict): Rename the columns as per the requirement ouput
            transformations (_type_, optional): transformations like merge and
                                                controlled through metadata.
                                                Defaults to None.

        Returns:
            boolean: status post the transformations and stored in the raw S3.
        """
        try:
            stage_path = f"s3://{bucket_name}/{tmp_raw_folder}"
            logger.info(stage_path)

            # Convert JSON to a DataFrame
            tf_ctxt = source_name + table_name
            source_table_name = table_name

            dynamic_frame = glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    'paths': [stage_path],
                    'recurse': True,
                    'groupFiles': 'inPartition',
                    'groupSize': '1048576',
                    'format_options': {
                        'jsonPath': '$[value]',
                        'multiline': True
                    }
                },
                format="json",
                transformation_ctx=tf_ctxt
            )        
            dynamic_frame.printSchema()
            json_df = dynamic_frame.toDF()

            exploded_records_df = json_df.select(explode("value").alias(""))

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
            relationalized_data_path = f"temp/{table_name}/raw/relationalized_data"

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
                logger.info(f"Relation Table Name: {tables_name} schema below")
                df.printSchema()
            
            if source_name in tables:
                ped_modif_df = tables[source_name]
            else:
                ped_modif_df = None
            
            logger.info(f"Source {source_name} filtered df schema below")
            target_columns = []
            rename_col_mapping = []
            for src_col, rename_col in rename_cols.items():
                target_columns.append(
                    "{}".format(str(rename_col))
                )
                rename_col_mapping.append(
                    col(src_col).alias(rename_col)
                )
                if src_col not in ped_modif_df.columns:
                    ped_modif_df = ped_modif_df.withColumn(src_col, lit(None).cast("string"))
                if "project" in source_table_name:
                    if "sds_ProjectLocationState.sds_name" not in ped_modif_df.columns:
                        ped_modif_df = ped_modif_df.withColumn("sds_ProjectLocationState.sds_name", lit(None).cast("string"))
                    if "sds_ProjectLocation.sds_name" not in ped_modif_df.columns:
                        ped_modif_df = ped_modif_df.withColumn("sds_ProjectLocation.sds_name", lit(None).cast("string"))
            
            logger.info(f"rename mapper {rename_col_mapping} filtered df")
            
            ped_modif_df = ped_modif_df.select(
                [
                    col("`{}`".format(str(src_col))).alias(rename_col)
                    for src_col, rename_col in rename_cols.items()
                ]
                )
            
            ped_modif_df.printSchema()
            logger.info(f"Source {source_name} post drop of id below")

            # check for transformations
            if transformations:
                is_combine_key = transformations["is_combine_key"]
                if is_combine_key:
                    ped_obj_rdd = ped_modif_df.rdd
                    ped_comb_list = ped_obj_rdd.combineByKey(
                        self.__to_list,
                        self.__append,
                        self.__extend).collect()
                    print("combine by key rdd ")
                    spark = glue_context.spark_session
                    ped_modif_df = spark.createDataFrame(
                        ped_comb_list,
                        target_columns
                        )
                    print(f"combine by key df - {ped_modif_df}")
            logger.info(f"Source {source_name} post renamed columns below")

            renamed_dynamicframe = DynamicFrame.fromDF(
                ped_modif_df,
                glue_context,
                "dynamic_frame"
                )
            output_parquet_path = f"s3://{bucket_name}/{s3_output_path_uri}"

            logger.info(f"Writing to folder in S3 {output_parquet_path}")
            handle_voids_flag=False
            if "project" in source_table_name:
                handle_voids_flag=False
            # Write partitioned data to S3
            success = write_glue_df_to_s3_with_specific_file_name(
                glue_context,
                renamed_dynamicframe,
                bucket_name,
                s3_output_path_uri,
                tf_ctxt,
                s3_output_format='json',
                handle_voids=handle_voids_flag
            )
            return success
        except Exception as e:
            logger.info(F"Error in proecessing data with error {e}")
            return False
     
    def build_next_chunk_api(
            self,
            ped_metadata: dict,
            next_record_url: str
            ):
        """Next Pagination API builder

        Args:
            ped_metadata (dict): Base API_OBJECT request for
                                sf_object_name named object
            next_record_url (str): next pagination url

        Returns:
            dict: Next pagination formated API config
        """
        ped_metadata["api_parameter"]["endpoint"] = next_record_url
        ped_metadata["api_parameter"]["api_method"] = "get"
        ped_metadata["api_parameter"]["api_query_params"] = {}

        return ped_metadata

    def get_incremental_value(
            self,
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
            str: next incremental value for the API
        """
        next_run_value = incremental_default
        try:
            # Read from the S3 location to update if exist
            last_process_info = s3_client.read_s3_file_as_json(folder_name,
                                                               process_file)
            if last_process_info and ("lastSuccessProcessedTime" in last_process_info):
                next_run_value = last_process_info["lastSuccessProcessedTime"]
            else:
                logger.error("Error retrieving process audit file from s3")
            if not next_run_value:
                next_run_value = incremental_default
            return next_run_value
        except Exception as e:
            logger.error(f"Error retrieving process audit file from s3 {e}")
            return incremental_default
    
    def update_last_updated(
            self,
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
            process_file (str): PED object process file that
                                stores the incremental value
            kms_key_id (str): PED bucket KMS_Key_id for activities
            last_update_time (str): successful incremental value

        Returns:
            boolean: status of storage of incremental value in s3
        """
        file_path = f"{process_folder}/{process_file}"
        FAILURE = False
        try:
            # Read from the S3 location to update if exist
            last_process_info = s3_client.read_s3_file_as_json(
                process_folder,
                process_file)
            
            if not last_process_info:
                last_process_info = {}
            
            increment_value = last_update_time
            last_process_info["lastSuccessProcessedTime"] = increment_value
            
            return s3_client.upload_to_s3(json.dumps(last_process_info),
                                          file_path,
                                          kms_key_id,
                                          is_gzip=False
                                          )
        except Exception as e:
            logger.error(f"Error retrieving process audit file from s3 {e}")
            return FAILURE

    def get_formated_api_time(self, timez=TIMEZONE_UTC):
        """PED supported incremental key values

        Args:
            timez (str, optional): time zone . Defaults to TIMEZONE_UTC.

        Returns:
            str: PED supported formated incremental value
        """
        # yyyy-MM-ddTHH:mm:ssZ
        utc_tz = pytz.timezone('UTC')
        utc_time = datetime.now(pytz.utc).replace(tzinfo=utc_tz)
        ped_last_updated_time = utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info(f"formated incremental key value of ped - {ped_last_updated_time}")
        return ped_last_updated_time

    def get_batch_s3_output_uri(
            self,
            s3_unformated_folder_uri: str,
            s3_unformated_output_folder_format: str,
            start_time: str,
            curent_format=TIMESTAMP_FORMAT
    ):
        """ Output the formated S3 folder

        Args:
            s3_unformated_folder_uri (str): S3 folder URI which has parameter
            s3_unformated_output_folder_format (str): S3 output URI
            start_time (str): output folder name
            curent_format (_type_, optional): Format of the folder to store.
                                              Defaults to TIMESTAMP_FORMAT.

        Returns:
            str: format S3 folder uri based on the time in formated
        """
        current_datetime = datetime.strptime(start_time, curent_format)
        s3_formated_output_folder_name = current_datetime.strftime(
            s3_unformated_output_folder_format
            )
        s3_formated_folder_uri = s3_unformated_folder_uri.format(
            batch_foldername_format=s3_formated_output_folder_name
        )
        return s3_formated_folder_uri, s3_formated_output_folder_name
    
    def read_json_s3_folder_as_df(
            self,
            glue_context: GlueContext,
            raw_bckt: str,
            folder_path: str,
            tf_ctxt: str):
        """
        Read json from s3 folder
        :param glue_context: GlueContext wrapper
        :param tf_ctxt: transformation context
        :param raw_bckt: S3 raw bucket path
        :param folder_path: folder path
        :return: data as dict
        """
        SUCCESS = True
        FAILURE = False
        try:
            s3_client = S3(raw_bckt, REGION)
            if s3_client.s3_folder_exists(folder_path):
                stage_path = f"s3://{raw_bckt}/{folder_path}"
                dynamic_frame = glue_context.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        'paths': [stage_path],
                        'recurse': True,
                        'groupFiles': 'inPartition',
                        'groupSize': '1048576',
                        'format_options': {
                            'multiline': True
                            }
                        },
                    format="json",
                    transformation_ctx=tf_ctxt
                )        
                dynamic_frame.printSchema()
                proj_df = dynamic_frame.toDF()
                return SUCCESS, proj_df
            else:
                logger.error(
                    f"Folder {folder_path} not found"
                )
                return FAILURE, None
        except Exception as e:
            logger.error(
                f"Failure in reading {folder_path} with error {e}"
            )
            return FAILURE, None
    
    def ped_entities_transformation(
            self,
            glue_context: GlueContext,
            bucket_name: str,
            entity_list: list,
            delete_columns: list,
            tmp_output_generic_path: str,
            raw_final_out_uri: str,
            is_merge_needed=True
    ):
        """Apply required transformations for PED and is controlled
           through metadata.

        Args:
            glue_context (GlueContext): glue context wrapper
            bucket_name (str): S3 bucket name for the raw layer
            entity_list (list): List of tables/objects needs to be
                                pulled through API becoz of dependency
            delete_columns (list): to be deleted columns post transformations
            tmp_output_generic_path (str): S3 path of temporarly stored post
                                           pre-process of API response.
                                           This is maintained in metadata.
            raw_final_out_uri (str): S3 path that will be used for Curation.
            is_merge_needed (bool, optional): whether merge transformation
                                              needed and it is also maintained
                                              in metadata.Defaults to True.
        Returns:
            boolean: status whether the data is stored post tarnsformation.
        """
        SUCCESS = True
        FAILURE = False
        try:
            entities_df = []
            project_df = None
            mos_df = None
            tf_ctxt = "ped_merge"
            for entity_name in entity_list:
                tmp_output_formated_uri = tmp_output_generic_path.format(
                    ped_object_name=entity_name)
                success, formated_df = self.read_json_s3_folder_as_df(
                    glue_context,
                    bucket_name,
                    tmp_output_formated_uri,
                    tf_ctxt
                )
                if is_merge_needed:
                    if "projects" == entity_name:
                        project_df = formated_df
                    elif "mos" == entity_name:
                        mos_df = formated_df
                    
                    entities_df.append(formated_df)
                else:
                    final_out = raw_final_out_uri.format(
                        ped_object_name=entity_name
                        )
                    success = write_glue_df_to_s3_with_specific_file_name(
                        glue_context,
                        formated_df,
                        bucket_name,
                        final_out,
                        tf_ctxt,
                        s3_output_format='json'
                    )
                    if not success:
                        logger.info(f"Error creating final out {final_out} - src {entity_name}")
                        return FAILURE
    
            if is_merge_needed:
                ped_merged_df = project_df.join(
                    broadcast(mos_df),
                    mos_df.mos_projectid == project_df.projectid,
                    "left")
            
                ped_merged_df.printSchema()
                ped_df = ped_merged_df.drop(*delete_columns)
                logger.info("post del in merge trans --- below")
                ped_df.printSchema()
                ped_final_dataframe = DynamicFrame.fromDF(
                        ped_df,
                        glue_context,
                        "dynamic_frame"
                        )
                logger.info("Writing to folder in S3 ")
                # Write partitioned data to S3
                success = write_glue_df_to_s3_with_specific_file_name(
                    glue_context,
                    ped_final_dataframe,
                    bucket_name,
                    raw_final_out_uri,
                    tf_ctxt,
                    s3_output_format='json'
                )
                if not success:
                    logger.info(f"Error creating final {final_out} post-merge")
                    return FAILURE
            # Delete any existing staging data for this job
            logger.info("Delete staging data after job is finished")
            return SUCCESS
        except Exception as e:
            logger.info(F"Error in proecessing data with error {e}")
            return FAILURE
