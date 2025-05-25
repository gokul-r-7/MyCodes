import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from worley_helper.utils.logger import get_logger
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, month, dayofmonth, hour
import pytz
from dateutil.parser import parse
from dateutil.tz import gettz
import xml.etree.ElementTree as ET

import json
from datetime import datetime, timedelta
import base64


import boto3
from botocore.exceptions import ClientError

from urllib.parse import urlparse

# custom Utilities   #
from worley_helper.utils.constants import (
    DATE_FORMAT,
    TIMEZONE_SYDNEY,
    REGION,
    TIMESTAMP_FORMAT,
    DATETIME_FORMAT,
    TIMEZONE_UTC,
    AUDIT_DATE_COLUMN,
)
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.api_helpers import call_api
from worley_helper.utils.aws import DynamoDB , SecretsManager, S3
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.configuration.config import EcosysConfiguration
from worley_helper.sources.ecosys_runner  import Ecosys
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from pyspark.sql.types  import *
import awsglue.gluetypes as gt
from datetime import datetime, timezone

# from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
#from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

XML_FILE_TIMESTAMP_FORMAT: str = "%Y-%m-%d-%H-%M-%S"
import re

def get_namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''

def get_datatag_to_search(function_mapping_tag,current_namespace):
    return ''+current_namespace+function_mapping_tag if current_namespace else function_mapping_tag
    
def get_ecodata_formated_xml_data(data_xml_array,cur_nm,secorg_id,snapshot_id):
    secorg = ET.Element(f"{cur_nm}secorg_id")
    secorg.text = secorg_id
    snapshot_element = ET.Element(f"{cur_nm}snapshot_id")
    secorg.text = snapshot_id
    for eco_child in data_xml_array:
        eco_child.append(secorg)
        eco_child.append(snapshot_element)
    return data_xml_array

def process_xml_data(sc,glueContext,function_mapping, function_name, raw_data_location, s3_bucket_name, s3_key, s3_crawler_key,raw_context,function_root_tag, **kwargs):
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()
    FAILURE = False
    s3_complete = sample_complete = True
    func_root_node = function_mapping[function_name][0]
    print(f" read row tag - {function_root_tag}")
    spark_s3_src_xml_file = f"s3://{s3_bucket_name}/{raw_data_location}"
    no_array_data_functions = ["usersecorg","userproject"]
    func_format_options = {"rowTag": function_root_tag}
    if function_name in no_array_data_functions:
        standard_schema = kwargs.get('standard_schema',None)
        if standard_schema:
            func_format_options = {"rowTag": function_root_tag, "withSchema": json.dumps(standard_schema.jsonValue())}
            print(f"standard schema for {function_mapping} applied")
    root_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [spark_s3_src_xml_file],"recurse": True},
        format="xml",
        format_options=func_format_options,
        transformation_ctx=f"{raw_context}"
        )
    root_df.printSchema()
    total_count = root_df.count()
    if total_count < 1:
        print(f"{function_name} API has 0 total count from raw s3 path ")
        return FAILURE, FAILURE, 0
    
    df = root_df.toDF().select(explode(function_mapping[function_name][0]).alias(function_mapping[function_name][1]))
    if not df.rdd.isEmpty():
        sample_fraction = 0.5
        sample_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
        ecosys_df = DynamicFrame.fromDF(df, glue_ctx=glueContext, name="df")
        sample_dynamic_frame = DynamicFrame.fromDF(sample_df, glue_ctx=glueContext, name="sampledf")
        if function_name == "users":
            sample_dynamic_frame = sample_dynamic_frame.resolveChoice(specs=[("users.CostObjectSuperUser", "cast:string")])
            ecosys_df = ecosys_df.resolveChoice(specs=[("users.CostObjectSuperUser", "cast:string")])
        transformation_context = function_name

        relationalized_df = ecosys_df.relationalize("root", "s3://worley-test-bucket/temp-path/")
        sample_relationalized_df = sample_dynamic_frame.relationalize("root", "s3://worley-test-bucket/temp-path/")
        secorg_details_entities = ["ecodata","findatapcs","findata","snapshotfull","snapshotlog"]

        for node in ['root']:
            new_dynamic_frame = relationalized_df.select(node)
            new_sample_dynamic_frame = sample_relationalized_df.select(node)
        
        if function_name in secorg_details_entities:
            secorg_id = kwargs.get('secorg_id', "")
            src_snapshot_id = kwargs.get('snapshot_id', "")
            new_df = new_dynamic_frame.toDF()
            new_samp_df = new_sample_dynamic_frame.toDF()
            new_sec_df = new_df.withColumn('secorg_id',lit(secorg_id))
            snapshot_id = f"{src_snapshot_id}"
            new_sec_samp_df = new_samp_df.withColumn('secorg_id',lit(secorg_id))
            if function_name == "ecodata" and len(snapshot_id.strip()) > 0 :
                print(f"ecodata secorg-{secorg_id} and snapshot_id - {snapshot_id}")
                new_sec_df = new_sec_df.withColumn('snapshot_id',lit(snapshot_id))
                new_sec_samp_df = new_sec_samp_df.withColumn('snapshot_id',lit(snapshot_id))
            
            new_dynamic_frame = DynamicFrame.fromDF(new_sec_df, glue_ctx=glueContext, name="new_df")
            new_sample_dynamic_frame = DynamicFrame.fromDF(new_sec_samp_df, glue_ctx=glueContext, name="new_sampledf")
            

        s3_complete = write_glue_df_to_s3_with_specific_file_name(
            glueContext, new_dynamic_frame, s3_bucket_name, s3_key , transformation_context,typecast_cols_to_string = True
        )
        sample_complete = write_glue_df_to_s3_with_specific_file_name(
            glueContext, new_sample_dynamic_frame, s3_bucket_name, s3_crawler_key , transformation_context,typecast_cols_to_string = True
        )
        if function_name == "secorglist":
            filter_logic = kwargs.get('root_secorg_filter', None)
            secorg_list_s3_path = f"s3://{s3_bucket_name}/{s3_key}/"
            
            if filter_logic:
                root_secorg_dynf = glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": [secorg_list_s3_path]
                    },
                    format="parquet",
                    transformation_ctx = transformation_context
                )
                top_region_secorg_temp_view_name = kwargs.get('top_region_secorg_temp_view_name', None)
                root_secorg_dynf.toDF().createOrReplaceTempView(f"{top_region_secorg_temp_view_name}")
                toplevel_region_df = spark.sql(f"{filter_logic}")
                
                top_region_secorg_path = kwargs.get('root_secorg_raw_path', None)
                s3_path = f"s3://{s3_bucket_name}/{top_region_secorg_path}"
                # Write the DataFrame to S3 in text format
                toplevel_region_df.coalesce(1).write.mode("overwrite").text(s3_path)

    return s3_complete, sample_complete, total_count

def process_rawdatasampling(source_df,sampling_path,s3_bucket_name,sampl_cntxt,glueContext,sample_fraction = 0.5,sample_seed=42):
    sample_complete = False
    if not source_df.rdd.isEmpty():
        sample_df = source_df.sample(withReplacement=False, fraction=sample_fraction, seed=sample_seed)
        sample_dynamic_frame = DynamicFrame.fromDF(sample_df, glue_ctx=glueContext, name="sampledf")
        sample_complete = write_glue_df_to_s3_with_specific_file_name(
            glueContext, sample_dynamic_frame, s3_bucket_name, sampling_path , sampl_cntxt
            )
    return sample_complete


def get_incremental_date(metadata, s3_client, secorg_id, file_name):
    incremental_default_date = metadata['job_parameter']['default_start_date']
    incremental_criteria_folder_location = metadata['job_parameter']['incremental_criteria_folder_location']
    folder_name=f"{incremental_criteria_folder_location}".format(secorg_id=secorg_id)
    print("folder name", folder_name)
    print("file name", file_name)
    content,status=s3_client.read_s3_file(folder_name,file_name)
    if status:
        incremental_date_project_file=content.split(',')[3]
        incremental_from_date=incremental_date_project_file
        incremental_to_date=datetime.now(timezone.utc).strftime('%Y-%m-%d') 
        print("incremental_date_project_file --> " + incremental_date_project_file)
        return incremental_from_date, incremental_to_date
    else:
        incremental_from_date=incremental_default_date
        incremental_to_date=datetime.now(timezone.utc).strftime('%Y-%m-%d') 
        print("incremental_date --> " + incremental_from_date+ " TO "+ incremental_to_date)
        return incremental_from_date, incremental_to_date
       



def main():
    default_job_params = ["JOB_NAME", "source_name", "function_name","metadata_table_name","secorg_id","start_date", "end_date","environment","topic_arn"]
    # Extract the arguments passed from the Airflow DAGS into Glue Job
    optional_params = ["project_id"]
    is_project_based = False
    try:
        args = getResolvedOptions(sys.argv, default_job_params+optional_params)
        is_project_based = True
    except Exception:
        # If some optional params are missing, capture only required ones
        args = getResolvedOptions(sys.argv, default_job_params)
        is_project_based = False
    print(args)
    source_name = args.get("source_name")
    function_name = args.get("function_name")
    metadata_table_name = args.get("metadata_table_name")
    arg_secorg_ids = args.get("secorg_id")
    project_id = args.get("project_id", None)
    job_name = args["JOB_NAME"]
    job_run_id = args["JOB_RUN_ID"]
    delta_start_date = parse(args["start_date"])
    delta_end_date = parse(args["end_date"])
    sns_topic_arn = args.get("topic_arn")
    environment = args.get("environment")
    print(f" ARG SECORG ID :{arg_secorg_ids}")
    # Set up the logger info
    logger = get_logger(__name__)
    logger.info(
    "==================Setting up the Spark and Glue Context ============"
        )

    # Set up the spark context and glue context
    conf = SparkConf()
    conf.set(f"spark.rpc.message.maxSize", "512")
    sc = SparkContext(conf=conf)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"] + source_name + function_name + args["secorg_id"], args)
    logger.info("Script execution started.")

    # Convert to Australia/Melbourne timezone
    melbourne_tz = pytz.timezone("Australia/Melbourne")
    delta_start_date = delta_start_date.astimezone(melbourne_tz)
    delta_end_date = delta_end_date.astimezone(melbourne_tz)
    delta_start_date = delta_start_date.strftime("%Y-%m-%d %H:%M:%S")
    delta_end_date = delta_end_date.strftime("%Y-%m-%d %H:%M:%S")

    job_start_time = generate_timestamp_string(
        timezone=TIMEZONE_SYDNEY
    ).strftime(TIMESTAMP_FORMAT)
    
    logger.info(f"==================job Start time is :{job_start_time} ============")

    # Define the Sort Keys for DynamoDB Fetch
    input_keys = "api_xml#" + source_name + "#" + function_name

    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=input_keys
    )

    logger.info(f" Metadata Response :{metadata}")
    # Setup Configuration
    jobConfig = EcosysConfiguration(**metadata)
    print(f"Type of Job Config :{type(jobConfig)}")
    xml_api_s3_upload_complete = False

    try:
        # Create the dictionary containing arguments for query call
        function_mapping_p1 = {
            "project_list": ["Ecosys_ProjectList", "projectlist"],
            "costtypepcsmapping": ["CostObjectCategoryList", "costtypepcsmapping"],
            "calendar": ["INTa_Snowflake_Calendar_API", "calendar"],
            "header": ["INTa_SnowFlake_ECOHDR_CO_UpdatedResult", "header"],
            "archival": ["INTa_Snowflake_Project_List_All_Arch", "archival"],
            "snapshotfull": ["INTa_SnowFlake_Snapshot_List_SecOrg", "snapshotfull"],
            "snapshotlog" : ["INTa_SnowFlake_Snapshot_Log_SecOrg", "snapshotlog"],
            "actuallog": ["INTa_QualityData_Actuals_Log", "actuallog"],
            "workingforecastlog": ["INTa_QualityData_Working_Forecast_Log", "workingforecastlog"],
            "earnedvaluelog": ["INTa_QualityData_Earned_Value_Log", "earnedvaluelog"],
            "users": ["INTa_SnowFlake_Users_Updated", "users"],
            "findata": ["INTa_SnowFlake_FINDATA_SecOrg_V1", "findata"],
            "findatapcs": ["INTa_SnowFlake_FINDATA_CO_PCS_SecOrg_V2", "findatapcs"],
            "snapidlist": ["INTa_SnowFlake_SnapID", "snapiddlist"],
            "secorglist": ["INTa_SnowFlake_SecOrgs", "secorglist"],
            "exchange_rates": ["Exchange_Rates_API", "exchange_rates"],
            "projectKeyMembers": ["INTa_ProjectKeyMembersSecOrg", "projectKeyMembers"],
        }
        function_mapping_p2 = {
            "dcsMapping": ["INTa_DEL_EDSR_DCS_Mapping", "dcsmapping"],
            "gateData": ["INTa_DEL_EDSR_Gate_Data", "gatedata"],
            "TimePhased": ["Ecosys_ControlAccount_TimePhased", "timephased"],
            "deliverable": ["Ecosys_Deliverable", "deliverable"],
            "deliverable_source": ["Ecosys_Deliverable_Source", "deliverable_source"],
            "deliverable_gate": ["Ecosys_Deliverable_Gates", "deliverable_gate"],
            "change_deliverable": ["Ecosys_Change_Deliverables","change_deliverable"],
            "controlAccount":["Ecosys_ControlAccount", "controlAccount"],
            "controlAccount_Snapshot" : ["Ecosys_ControlAccount_Snapshot", "controlAccount_Snapshot"]
        }
        function_mapping_p3 = {"ecodata": ["INTa_SnowFlake_ECODATA_SecOrg_Updated", "ecodata"]}

        function_mapping_p4 = {
            "usersecorg": ["INTa_SnowFlake_User_SecOrgs", "usersecorg"],
            "userproject": ["INTa_SnowFlake_User_Projects", "userproject"]
        }
        function_mapping_p5 = {
            "deliverable": ["Ecosys_Deliverable", "deliverable"],
            "deliverable_source": ["Ecosys_Deliverable_Source", "deliverable_source"],
            "deliverable_gate": ["Ecosys_Deliverable_Gates", "deliverable_gate"],
        }

        function_mapping_p6 = {
            "wbs_list" : ["INT_PowerBI_Project_WBS_List", "wbs_list"]
        }
        logger.info(
    "==================Setting up the Spark and Glue Context ============"
        )
        snapshot_full_schema = gt.StructType([ 
            gt.Field("INTa_SnowFlake_Snapshot_List_SecOrg", 
                     gt.ArrayType(
                         gt.StructType([
                             gt.Field("CostObjectID", gt.StringType({}), {}),
                             gt.Field("CreateDate", gt.StringType({}), {}),
                             gt.Field("Exclude", gt.StringType({}), {}),
                             gt.Field("SnapshotID", gt.StringType({}), {}),
                             gt.Field("SnapshotIntID", gt.StringType({}), {})
                         ])
                     ))
        ])

        today = datetime.today()
        topic_arn = (
            f"{sns_topic_arn}"
        )   
        #    "arn:aws:sns:ap-southeast-2:891377181979:worley-data-platform-notification-topic-sydney-dev-sns-topic-data-platform-notification"
        #)

        secorg_ids = arg_secorg_ids.split(",")
        print(f"SECORG IDS :{secorg_ids}")

        # Set the API timeout value.
        logger.info(f"Http Timeout value for api call is set to {jobConfig.api_parameter.api_retry} seconds")

        logger.info("==================Fetching DB Secrets ============")
        sm = SecretsManager(default_region=REGION)
        secret_params = json.loads(sm.get_secret(jobConfig.api_parameter.secret_key))
        user = secret_params.get("user")
        password = secret_params.get("password")
        authData = f"{user}:{password}"
        base64AuthData = base64.b64encode(authData.encode()).decode()
        jobConfig.api_parameter.dynamic_api_headers.Authorization = f"{jobConfig.api_parameter.api_auth_type} {base64AuthData}"


        partition_date = generate_timestamp_string(
            timezone=TIMEZONE_SYDNEY
        ).strftime(DATETIME_FORMAT)
        # print(f"partition_date : {partition_date}")
        partition_str = f"{get_partition_str_mi(partition_date)}"



        if args["secorg_id"] == '""':
            print("===ARG SEC ORG IDS HERE")
            s3_key = "project_control/" + source_name + "/" + function_name + "/"  + partition_str
            tf_ctxt = source_name + function_name
        else:
            print("===ARG SEC ORG IDS IS IN ELSE PATH ")
            s3_key = "project_control/" + source_name + "/" + function_name + "/" + arg_secorg_ids + "/" + partition_str
            tf_ctxt = source_name + function_name + arg_secorg_ids+"_xml_api_nw"

        s3_crawler_key = "project_control/" + source_name + "/data_sampling/" + function_name + "/"
        s3_snapshot_processed_path = (
            "s3://" + jobConfig.job_parameter.bucket_name + "/project_control/" + source_name + "/snapshot_processed/" + arg_secorg_ids
        )
        s3_snapshot_processed_uri = "project_control/" + source_name + "/snapshot_processed/" + arg_secorg_ids  
        s3_snapshot_processed_crawler_path = "project_control/" + source_name + "/data_sampling/"+ "snapshot_processed/" 
        s3_ecosys_path = jobConfig.job_parameter.input_path + arg_secorg_ids
        secorg_details_entities = ["findatapcs","findata","snapshotfull","snapshotlog"]
        raw_data_location = metadata['job_parameter']['raw_data_location']
        #raw_api_root_node = metadata['job_parameter']['xml_api_root_node']
        xml_api_root_tag = metadata['job_parameter']['xml_api_root_node']
        xml_api_data_array_root_tag = metadata['job_parameter']['xml_api_array_root_name']
        xml_api_data_array_root_alias = metadata['job_parameter']['xml_api_array_root_alias_name']
        xml_api_read_context = metadata['job_parameter']['xml_api_read_context']
        #s3_client = boto3.client("s3")
        s3_client = S3(jobConfig.job_parameter.bucket_name, default_region=REGION)
        #S3(jobConfig.job_parameter.bucket_name, REGION)
        kms_key_id = metadata['job_parameter']['kms_key_id']
        raw_bckt_name = jobConfig.job_parameter.bucket_name
        
        
        
        if source_name == "ecosys":
            # Create the instance of Ecosys framework
            env = f"{environment}"
            ecosys = Ecosys(topic_arn,env)
            # Call the ecosys API from api_io_access.py
            logger.info(
                        f" ========================ECOSYS API Invocation Start=====================."
                    )
            if function_name in function_mapping_p1:
                # Call the Project List API for each SecOrg ID, retrying if needed
                for secorg_id in secorg_ids:
                    
                    logger.info(f"Calling Project List API for Sec Org Id - {secorg_id}.")
                    curent_load_dt=generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(XML_FILE_TIMESTAMP_FORMAT)
                    
                    if secorg_id and secorg_id.strip() == '""':
                        logger.info("secorg is empty")
                        raw_data_location = raw_data_location.format(partition_uri=partition_str)
                        name=f"{curent_load_dt}"                    
                    elif secorg_id:
                        xml_api_read_context = xml_api_read_context.format(secorg_id=secorg_id)
                        raw_data_location = raw_data_location.format(secorg_id=secorg_id,partition_uri=partition_str)
                        name=f"{secorg_id}_{curent_load_dt}"                        
                    
                    function_mapping_p1[function_name]=[xml_api_data_array_root_tag,xml_api_data_array_root_alias]

                    # Call the API for each SecOrg ID, retrying if needed
                    retry_count = 0
                    

                    if function_name in ('actuallog', 'workingforecastlog', 'earnedvaluelog'):
                        file_name=f"{source_name}_{function_name}_{secorg_id}.txt"
                        incremental_from_date, incremental_to_date = get_incremental_date(metadata, s3_client, secorg_id, file_name)
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=secorg_id,createdate=incremental_from_date,todate=incremental_to_date)
                    else:
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=secorg_id)
                    logger.info(f"Derived Query Params - {jobConfig.api_parameter.dynamic_api_query_param}.")

                    logger.info(f"Job Config Details  - {jobConfig}")

                    while retry_count < jobConfig.api_parameter.api_retry:
                        # ecosys_response, success = call_api(
                        #     jobConfig.api_parameter.endpoint, user, password, query_params, jobConfig.api_parameter.api_timeout
                        # )
                        jobconfig_dict = jobConfig.model_dump()
                        jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                        logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                        http_client = HTTPClient(jobconfig_dict)
                        ecosys_response , success , status_code = http_client.run()
                        # logger.info(f"{function_name} XML API response - {ecosys_response}")
                        
                        object_name = f"{raw_data_location}/{name}.xml"
                        if success:
                            if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                                logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{object_name}")
                                success = True
                                xml_api_s3_upload_complete = True
                                xml_data = root = ET.fromstring(ecosys_response)
                            else:
                                logger.error(f"{function_name} API successful but failed to upload to S3")
                                xml_api_s3_upload_complete = False
                                success = False
                        else:
                            if function_name == "archival" and status_code == 500:
                                logger.info(f"{function_name} has 500 and it needs from source so making it as success")
                                os._exit(os.EX_OK)
                            logger.error(f"{function_name} API unsuccessful")
                            success = False
                            sys.exit(1)  # Exit with failure code
                        
                        json_data = ecosys_response
                        s3_complete=False
                        if success:
                            if (
                                function_name == "archival"
                            ):
                                
                                if xml_data.find("INTa_Snowflake_Project_List_All_ArchError") is None:
                                        
                                    s3_complete, sample_complete, total_count = process_xml_data(
                                        sc,glueContext,function_mapping_p1, function_name, object_name, jobConfig.job_parameter.bucket_name, s3_key, s3_crawler_key,xml_api_read_context,xml_api_root_tag,
                                        )
                                    logger.info(f"S3 complete post response :{s3_complete} ")
                                    if total_count < 1:
                                        logger.info(f"{function_name} has 0 data from source so making it as success")
                                        if xml_api_s3_upload_complete:
                                            logger.info(f"Deleting folder path {raw_data_location}")
                                            s3_client.delete_folder_from_s3(raw_data_location)
                                        os._exit(os.EX_OK)
                                    
                                else:
                                    logger.info(
                                        f"Failed in generating outcome for {function_name} with 404 error INTa_Snowflake_Project_List_All_ArchError"
                                    )
                                    s3_complete = False
                                    success = False
                                    
                            elif function_name in secorg_details_entities:
                                stand_schema = None
                                if function_name == "snapshotfull":
                                    stand_schema = snapshot_full_schema
                                s3_complete, sample_complete, total_count = process_xml_data(
                                    sc,
                                    glueContext,
                                    function_mapping_p1,
                                    function_name,
                                    object_name, 
                                    jobConfig.job_parameter.bucket_name,
                                    s3_key, 
                                    s3_crawler_key,
                                    xml_api_read_context,
                                    xml_api_root_tag,
                                    secorg_id=secorg_id,
                                    standard_schema=stand_schema
                                )
                                if total_count < 1:
                                    logger.info(f"{function_name} has 0 data from source so making it as success")
                                    if xml_api_s3_upload_complete:
                                        logger.info(f"Deleting folder path {raw_data_location}")
                                        s3_client.delete_folder_from_s3(raw_data_location)
                                    os._exit(os.EX_OK)
                            else:
                                rootsecorg_filter = None
                                rootsecorg_tempview_name = None
                                rootsecorg_raw_path = None
                                if function_name == "secorglist":
                                    rootsecorg_filter = metadata['job_parameter']['root_secorg_filter']
                                    rootsecorg_tempview_name = metadata['job_parameter']['root_secorg_temp_view_name']
                                    if not rootsecorg_tempview_name:
                                        rootsecorg_tempview_name = "TopRegionSecOrgTempView"
                                    rootsecorg_raw_path = metadata['job_parameter']['root_secorg_raw_path']  
                                    
                                s3_complete, sample_complete, total_count = process_xml_data(
                                sc,glueContext,function_mapping_p1, function_name, 
                                object_name, jobConfig.job_parameter.bucket_name,
                                s3_key, s3_crawler_key,
                                xml_api_read_context,
                                xml_api_root_tag,
                                root_secorg_filter=rootsecorg_filter,
                                top_region_secorg_temp_view_name=rootsecorg_tempview_name,
                                root_secorg_raw_path=rootsecorg_raw_path
                                    )
                                if total_count < 1:
                                    logger.info(f"{function_name} has 0 data from source so making it as success")
                                    if xml_api_s3_upload_complete:
                                        logger.info(f"Deleting folder path {raw_data_location}")
                                        s3_client.delete_folder_from_s3(raw_data_location)
                                    os._exit(os.EX_OK)
                            if s3_complete:
                                logger.info(f"Sucessfull completion of {function_name}")
                                if function_name in ('actuallog', 'workingforecastlog', 'earnedvaluelog'):
                                    folder_name = metadata['job_parameter']['incremental_criteria_folder_location']
                                    incremental_criteria_folder_location= folder_name.format(secorg_id=secorg_id)

                                    file_name=f"{source_name}_{function_name}_{secorg_id}.txt"
                                    quality_audit_object = f"{incremental_criteria_folder_location}/{file_name}"
                                    print("mail_project_quality_audit_object --> " + quality_audit_object)
                                    
                                    audit_date_value = generate_today_date_string(TIMEZONE_SYDNEY,AUDIT_DATE_COLUMN)
                                    print("quality_audit_object --> " + audit_date_value)
                                    current_incremental_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')        
                                    project_audit_column=f"{secorg_id},{source_name},{function_name},{current_incremental_date},{audit_date_value}"  
                                    print("quality_audit_column --> " + project_audit_column)
                                    if project_audit_column:
                                        if s3_client.upload_to_s3(project_audit_column,quality_audit_object ,kms_key_id,is_gzip=False):
                                            logger.info(f"Uploaded Ecosys actuallog mail Project info to {raw_bckt_name}/{incremental_criteria_folder_location}")
                                            success = True
                                        else:
                                            logger.error("Ecosys actuallog mail Export API was successful but the gzip content could not be uploaded to S3")
                                            success = False
                                    else:
                                        logger.error("Failed to fetch Ecosys actuallog mail Export API payload")
                                        success = False
                            else:
                                
                                logger.info(f"Failed in generating outcome for {function_name}")
                                sys.exit(1)
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        if xml_api_s3_upload_complete:
                            logger.info(f"Deleting folder path {raw_data_location}")
                            s3_client.delete_folder_from_s3(raw_data_location)
                        sys.exit(1)  # Exit with failure code

            elif function_name in function_mapping_p2:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")
                api_success_with_error_code = "Ecosys_ControlAccount_TimePhasedError"
                if "gateData" == function_name:
                    api_success_with_error_code = "INTa_DEL_EDSR_Gate_DataError"
                elif "dcsMapping" == function_name:
                    api_success_with_error_code = "INTa_DEL_EDSR_Gate_DataError"
                else:
                    api_success_with_error_code = "Ecosys_ControlAccount_TimePhasedError"
                success = False
                success_projects = []
                # Loop through all Project Ids
                # for project in df_final.collect():
                retry_count = 0
                authentication_failure = False
                curent_load_dt=generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(XML_FILE_TIMESTAMP_FORMAT)
                proj_secorg = arg_secorg_ids
                curr_proj_id = project_id
                
                xml_api_read_context = xml_api_read_context.format(secorg_id=proj_secorg,project_id=curr_proj_id)
                raw_data_location = raw_data_location.format(secorg_id=proj_secorg,project_id=curr_proj_id,partition_uri=partition_str)
                name=f"{proj_secorg}_{curr_proj_id}_{curent_load_dt}"
                object_name = f"{raw_data_location}/{name}.xml"
                # Call the API for each SecOrg ID, retrying if needed
                logger.info(f"Project Id is :{curr_proj_id}")
                if function_name == "TimePhased":
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=proj_secorg,project_id=curr_proj_id,
                        PADate=ecosys.get_last_friday(today),
                    )
                elif function_name == "change_delivarable":
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,project_id=curr_proj_id,change=f"{curr_proj_id}.CHG-0001",
                    )
                elif function_name in ['controlAccount']:
                    dependent_path = f"{jobConfig.job_parameter.input_path}{arg_secorg_ids}/"
                    print(f"{function_name} dependent path {dependent_path}")
                    proj_list_df = spark.read.parquet(dependent_path)
                    selected_columns = ["projectlist_Project", "projectlist_SecOrg"]
                    proj_dedup_df = proj_list_df.dropDuplicates(["projectlist_Project", "projectlist_SecOrg"])
                    proj_count = proj_dedup_df.count()
                    if proj_count < 1:
                        logger.info(f" There is no new project for this secorg - {arg_secorg_ids} from the project list")
                        os._exit(os.EX_OK)
                        #sys.exit(1)
                    
                    proj_all_df = proj_dedup_df
                    proj_df=proj_all_df.where(f"projectlist_Project == '{curr_proj_id}'").select("projectlist_SecOrg")
                    if proj_df.count() < 1:
                        logger.info(f" For projectno {curr_proj_id} - There is no matching project details from the project list API. SO please check")
                        logger.info("So exiting with error")
                        sys.exit(1)
                    api_secorg = proj_df.first()
                    logger.info(f" secorg id {api_secorg} from project list for project - {curr_proj_id}")
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=api_secorg,project_id=curr_proj_id)
                elif function_name in ['controlAccount_Snapshot']:
                    dependent_path = f"{jobConfig.job_parameter.input_path}{arg_secorg_ids}/"
                    print(f"{function_name} dependent path {dependent_path}")
                    proj_list_df = spark.read.parquet(dependent_path)
                    proj_dedup_df = proj_list_df.dropDuplicates(["projectlist_Project", "projectlist_LatestSnapshotDate", "projectlist_SecOrg"])
                    proj_count = proj_dedup_df.count()
                    if proj_count < 1:
                        logger.info(f" There is no new project for this secorg - {arg_secorg_ids} from the project list")
                        os._exit(os.EX_OK)
                        #sys.exit(1)
                    #dynamic_frame = dynamic_frame.select_fields(selected_columns)
                    proj_all_df = proj_dedup_df
                    df_final=proj_all_df.where(f"projectlist_Project == '{project_id}'")
                    snap_df = df_final.select(max(col("projectlist_LatestSnapshotDate")))
                    proj_secorg_df = df_final.select("projectlist_SecOrg")
                    api_secorg = proj_secorg_df.first()
                    df_final.printSchema()
                    max_date = snap_df.collect()[0][0]

                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=api_secorg,project_id=curr_proj_id,snapshot_date=max_date)
                else:
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(
                        function_name, project_id=curr_proj_id)

                while retry_count < jobConfig.api_parameter.api_retry:

                    jobconfig_dict = jobConfig.model_dump()
                    jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                    logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                    http_client = HTTPClient(jobconfig_dict)
                    ecosys_response , success, status_code = http_client.run()
                    ecosys_xml_resp = ecosys_response
                    logger.info(f"{function_name} API for project={curr_proj_id} and status :{success}")
                    if success:
                        # logger.info(f"{function_name} XML API response - {ecosys_response}")
                        xml_resp_str = ecosys_xml_resp.decode('utf-8')
                        childtag = function_mapping_p2[function_name][0]
                        xml_data_et = ET.fromstring(xml_resp_str)
                        cur_nm = get_namespace(xml_data_et)
                        funct_dat_tag = ""+cur_nm+childtag
                        childs_objs = xml_data_et.findall(f"{funct_dat_tag}")
                        child_find_count = len(childs_objs)
                        logger.info(f"{function_name} API with tag {funct_dat_tag} has {child_find_count} records")
                        success_with_error_code = ""+cur_nm+api_success_with_error_code
                        if (xml_data_et.find(success_with_error_code) is None):
                            if child_find_count < 1:
                                logger.info(f"{funct_dat_tag} API - has 0 records as no data to write so moving ahead")
                                success = False
                                xml_api_s3_upload_complete = False
                                logger.info(f"please check the API as it has empty data.")
                                logger.info("So MOving on the to next API")
                                #job.commit()
                                #os._exit(0)
                                os._exit(os.EX_OK)
                                break

                            if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                                logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{raw_data_location}")
                                success = True
                                xml_api_s3_upload_complete = True
                            else:
                                logger.error(f"{function_name} API successful but failed to upload to S3")
                                xml_api_s3_upload_complete = False
                                success = False

                            if success and xml_api_s3_upload_complete:
                                s3_complete, sample_complete, total_count = process_xml_data(
                                    sc,
                                    glueContext,
                                    function_mapping_p2,
                                    function_name,
                                    object_name,
                                    jobConfig.job_parameter.bucket_name,
                                    s3_key, s3_crawler_key,
                                    xml_api_read_context,
                                    xml_api_root_tag,
                                    )
                                logger.info(f"S3 complete post response :{s3_complete} ")
                                if s3_complete:
                                    logger.info(
                                        f"Sucessfull completion of {function_name}")
                                    success_projects.append(curr_proj_id)
                                else:
                                    if total_count < 1:
                                        logger.info(f"{function_name} has 0 data from source so making it as success")
                                        if xml_api_s3_upload_complete:
                                            logger.info(f"Deleting folder path {raw_data_location}")
                                            s3_client.delete_folder_from_s3(raw_data_location)
                                        os._exit(os.EX_OK)
                                    logger.info(
                                        f"Failed in generating outcome for {function_name}")
                                    sys.exit(1)
                            else:
                                logger.info(f"Upload to raw path of API data failed for {function_name}")
                                success = False
                                xml_api_s3_upload_complete = False
                                logger.info(f"So exiting with failure fo f{curr_proj_id} and moving to other projects")
                                #sys.exit(1)  # Exit with failure code
                        break
                    else:
                        # Retry API
                        retry_count += 1
                # if not success:
                #     logger.info(f"Failed to retrieve project API {curr_proj_id} .")
                #     logger.info("moving to other projects in this secorg")
                if not success:
                    successful_proj_count = len(success_projects)
                    if successful_proj_count < 1:
                        logger.info(f"Failed to retrieve minimum one project. So failing the job")
                        sys.exit(1)  # Exit with failure code
                    logger.info(f"{arg_secorg_ids} secorg has completed - {successful_proj_count}")
            elif function_name in function_mapping_p3:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")
                logger.info(f" ========================ECOSYS S3 PATH ===== {s3_ecosys_path}=====================.")
                function_mapping_p3[function_name]=[xml_api_data_array_root_tag,xml_api_data_array_root_alias]
                ecodata_from_date = metadata['job_parameter']['from_date']
                xml_ns = metadata['job_parameter']['xml_api_read_namespace']
                if ecodata_from_date is None:
                    ecodata_from_date = "2024-01-01"
                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [s3_ecosys_path]
                                },
                                format="parquet",
                                transformation_ctx = f"{tf_ctxt}"
                                )
                new_data_count = dynamic_frame.count()
                if new_data_count < 1:
                    logger.info(f"{function_name} stopped as there is no new snapshot data,so stopping gracefully as it is depenedency")
                    job.commit()
                    os._exit(0)
                
                selected_columns = ["snapshotfull_CreateDate", "snapshotfull_SnapshotIntID"]
                dynamic_frame = dynamic_frame.select_fields(selected_columns)
                df_final = dynamic_frame.toDF()
                new_df_final = df_final.select("snapshotfull_SnapshotIntID",substring('snapshotfull_CreateDate', 1,10).alias('snapshotfull_CreateDate'))
                snapshot_processed_file_columns = StructType([
                    StructField('snapshotfull_CreateDate',StringType(), True),
                    StructField('snapshotfull_SnapshotIntID',StringType(), True),
                    StructField('Processed',StringType(), True),
                    StructField('secorg_id',StringType(), True)])

                # df_final = spark.read.parquet(jobConfig.job_parameter.input_path).select("snapshotfull_CostObjectID", "snapshotfull_SnapshotIntID")
                #df_delta = (spark.read.parquet(s3_snapshot_processed_path).select(substring("snapshotfull_CreateDate", 1,10).alias('snapshotfull_CreateDate'), "snapshotfull_SnapshotIntID").filter(col("Processed") == "success"))
                print(new_df_final.schema)
                logger.info(f"Count of available snapshots {new_df_final.count()}")
                #new_df_final=new_df_final.distinct()
                logger.info(f"Count of available distinct snapshots {new_df_final.count()}")
                new_df_final=new_df_final.withColumn('snapshotfull_CreateDate_temp',to_date(col('snapshotfull_CreateDate'),"yyyy-MM-dd"))
                # Per confirmation from Vikash needed atleast 2 years data for prod
                new_df_final_temp = new_df_final.filter(col('snapshotfull_CreateDate_temp') > ecodata_from_date)
                
                #new_df_final_temp=new_df_final_temp.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")
                #new_df_final_temp = new_df_final.filter(col('snapshotfull_CreateDate_temp') > "2024-12-10")
                new_df_final=new_df_final_temp.drop('snapshotfull_CreateDate_temp')
                logger.info(f"Historical with Latest Count snapshots from API - {new_df_final.count()}")
                
                if s3_client.s3_folder_exists(s3_snapshot_processed_uri):
                    df_delta = (spark.read.parquet(s3_snapshot_processed_path).select(substring("snapshotfull_CreateDate", 1,10).alias('snapshotfull_CreateDate'), "snapshotfull_SnapshotIntID").filter(col("Processed") == "success"))
                    df_delta=df_delta.distinct()
                    logger.info(f"Count of historically processed snapshots intill last run {df_delta.count()}")
                else:
                    schema=new_df_final.schema
                    df_delta=spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
                    print(df_delta.schema)
                unfiltered_df_diff = new_df_final.select("snapshotfull_CreateDate", "snapshotfull_SnapshotIntID").subtract(df_delta.select("snapshotfull_CreateDate", "snapshotfull_SnapshotIntID"))
                windowSpec = Window.partitionBy("snapshotfull_SnapshotIntID").orderBy(col("snapshotfull_CreateDate").desc())
                df_diff = unfiltered_df_diff.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")
                df_diff.show()
                
                snapshots_processed_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), snapshot_processed_file_columns)
                # Loop through all Project Ids
                for secorg_id in secorg_ids:
                    if secorg_id.strip():
                        xml_api_read_context = xml_api_read_context.format(secorg_id=secorg_id)
                        raw_data_location = raw_data_location.format(secorg_id=secorg_id,partition_uri=partition_str)
                    else:
                        raw_data_location = raw_data_location.format(partition_uri=partition_str)
                    latest_run_snapshot_count = df_diff.count()
                    logger.info(f" Latest Snapshots to be processed in current run - {latest_run_snapshot_count}")
                    for snapshot in df_diff.collect():
                        retry_count = 0
                        # Call the API for each SecOrg ID, retrying if needed
                        logger.info(
                            f"Snapshotid is :{snapshot['snapshotfull_SnapshotIntID']}"
                        )
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(
                            function_name,
                            secorg_id=secorg_id,
                            snapshotid=snapshot["snapshotfull_SnapshotIntID"],
                        )
                        logger.info(f"Query Params :{jobConfig.api_parameter.api_query_params}")
                        snapshot_id_process = snapshot["snapshotfull_SnapshotIntID"]
                        curent_datat_time=generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(XML_FILE_TIMESTAMP_FORMAT)
                        name=f"SNAPSHOT_ID={snapshot_id_process}_{curent_datat_time}"
                        object_name = f"{raw_data_location}/{name}.xml"
                        while retry_count < jobConfig.api_parameter.api_retry:
                            jobconfig_dict = jobConfig.model_dump()
                            jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                            logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                            http_client = HTTPClient(jobconfig_dict)
                            ecosys_response , success , status_code = http_client.run()
                            # logger.info(f"{function_name} XML API response - {ecosys_response}")
                            curr_snapid=snapshot["snapshotfull_SnapshotIntID"]
                            ecosys_xml_resp = ecosys_response
                            # logger.info(f"{function_name} XML API response - {ecosys_response}")
                            if success:
                                resp_stored_temp_status = s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False)
                                xml_resp_str = ecosys_xml_resp.decode('utf-8')
                                childtag = function_mapping_p3[function_name][0]
                                #eco_find_count = xml_resp_str.count("INTa_Snowflake_ECODATA_SecOrg_Updated")
                                
                                xml_data_et = ET.fromstring(xml_resp_str)
                                cur_nm = get_namespace(xml_data_et)
                                funct_dat_tag = '{http://ecosys.net/api/INTa_SnowFlake_ECODATA_SecOrg_Updated}INTa_Snowflake_ECODATA_SecOrg_Updated'
                                
                                ecodata_resp = xml_data_et.findall("{http://ecosys.net/api/INTa_SnowFlake_ECODATA_SecOrg_Updated}INTa_SnowFlake_ECODATA_SecOrg_Updated")
                                eco_find_count = len(ecodata_resp)
                                
                                logger.info(f" ecodata find all for {xml_api_root_tag} count - {eco_find_count}")
                                
                                if( xml_data_et.find(f"{cur_nm}INTa_SnowFlake_ECODATA_SecOrg_UpdatedError") is None):
                                    if eco_find_count < 1:
                                        logger.info(f"Secorg -{secorg_id} and snapshot - {snapshot_id_process} - has 0 records so moving ahead")
                                        success = True
                                        xml_api_s3_upload_complete = True
                                        break
                                    ''' 
                                    #get_ecodata_formated_xml_data(ecodata_resp,cur_nm,secorg_id,snapshot_id_process)
                                    secorg = ET.Element(f"{cur_nm}secorg_id")
                                    secorg.text = secorg_id
                                    snapshot_element = ET.Element(f"{cur_nm}snapshot_id")
                                    snapshot_element.text = snapshot_id_process
                                    for eco_child in ecodata_resp:
                                        eco_child.append(secorg)
                                        eco_child.append(snapshot_element)
                                    formated_xml_response = ET.tostring(xml_data_et, encoding='utf-8').decode('utf-8')
                                    '''
                                
                                #if xml_resp_str.find("INTa_SnowFlake_ECODATA_SecOrg_UpdatedError") == -1 and eco_find_count > 1:
                                    #if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                                    if resp_stored_temp_status:
                                        logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{object_name}")
                                        success = True
                                        xml_api_s3_upload_complete = True
                                        break
                                    else:
                                        logger.error(f"{function_name} API successful but failed to upload to S3")
                                        xml_api_s3_upload_complete = False
                                        success = False
                                else:
                                        logger.error(f"{function_name} API with success with error message")
                                        xml_api_s3_upload_complete = False
                                        success = False
                                        s3_client.delete_file_from_s3(object_name)
                                break        
                            else:
                                #logger.error(f"{function_name} API unsuccessful")
                                success = False
                                xml_api_s3_upload_complete = False
                                # Retry API
                                retry_count += 1
                        if success and xml_api_s3_upload_complete:
                            s3_complete, sample_complete, total_count = process_xml_data(
                                sc,
                                glueContext,
                                function_mapping_p3, 
                                function_name, 
                                object_name, 
                                jobConfig.job_parameter.bucket_name,
                                s3_key, 
                                s3_crawler_key,
                                xml_api_read_context,
                                xml_api_root_tag,
                                secorg_id=secorg_id,
                                snapshot_id=snapshot["snapshotfull_SnapshotIntID"]
                                )
                            if s3_complete:
                                all_snapshot_data = unfiltered_df_diff.filter(unfiltered_df_diff.snapshotfull_SnapshotIntID.contains(snapshot_id_process)).collect()
                                snapshot_df = spark.createDataFrame(all_snapshot_data, df_final.schema)
                                df_snapshot = snapshot_df.withColumn("Processed", lit("success")).withColumn("secorg_id", lit(secorg_id))
                                df_snapshot.write.mode("append").parquet(s3_snapshot_processed_path)
                                logger.info(f"Sucessfull completion of {function_name}")
                                s3_sample_complete = process_rawdatasampling(df_snapshot,s3_snapshot_processed_crawler_path,jobConfig.job_parameter.bucket_name,tf_ctxt,glueContext)
                                if s3_sample_complete:
                                    logger.info(f"Sucessfull completion of {function_name} in datasampling")
                                else:
                                    logger.info(f"Failed in generating of {function_name} in datasampling")
                                    if xml_api_s3_upload_complete:
                                        logger.info(f"Deleting folder path {raw_data_location}")
                                        s3_client.delete_folder_from_s3(raw_data_location)
                                    sys.exit(1)
                        if not success:
                            logger.info("Failed to retrieve ecodata post retries list.")
                            if xml_api_s3_upload_complete:
                                logger.info(f"Deleting folder path {raw_data_location}")
                                s3_client.delete_folder_from_s3(raw_data_location)
                                xml_api_s3_upload_complete = False
                            sys.exit(1)  # Exit with failure code
                    
            elif function_name in function_mapping_p4:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")

                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [jobConfig.job_parameter.input_path]
                                },
                                format="parquet",
                                transformation_ctx = F"{tf_ctxt}"# need to remove this extra one
                                )
                selected_columns = ["users_LoginName"]
                new_users_count = dynamic_frame.count()
                if new_users_count < 1:
                    logger.info(f"{function_name} stopped as there is no new Users data,so stopping gracefullyas it is depenedency")
                    job.commit()
                    os._exit(0)
                    
                dynamic_frame = dynamic_frame.select_fields(selected_columns)
                all_user_df = dynamic_frame.toDF()
                dist_df_final = all_user_df.dropDuplicates(selected_columns)
                dist_df_final.printSchema()
                function_mapping_p4[function_name]=[xml_api_data_array_root_tag,xml_api_data_array_root_alias]
                raw_data_location = raw_data_location.format(partition_uri=partition_str)
                stand_schema = None
                if "usersecorg" == function_name:
                    stand_schema = gt.StructType([ 
                        gt.Field("INTa_SnowFlake_User_SecOrgs", gt.ArrayType(
                            gt.StructType([
                                gt.Field("SecOrgIDList", gt.StringType({}), {}),
                                gt.Field("UserUserName", gt.StringType({}), {})
                                ])
                                ))
                                ])
                if "userproject" == function_name:
                    stand_schema = gt.StructType([ 
                        gt.Field("INTa_SnowFlake_User_Projects", gt.ArrayType(
                            gt.StructType([
                                gt.Field("ProjectIDList", gt.StringType({}), {}),
                                gt.Field("UserUserName", gt.StringType({}), {})
                                ])
                                ))
                                ])
                user_counter = 1                
                for user in dist_df_final.collect():
                    retry_count = 0
                    is_no_data_from_api = False
                    success = False
                    xml_api_s3_upload_complete = False
                    authentication_failure = False
                    curent_load_dt=generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(XML_FILE_TIMESTAMP_FORMAT)
                    name=f"{curent_load_dt}"                  
                    # Call the API for each User ID, retrying if needed
                    # logger.info(f"User Id is :{user['users_LoginName']}")
                    curr_user = user['users_LoginName']
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name, UserID=user["users_LoginName"])
                    object_name = f"{raw_data_location}/{name}.xml"
                    while retry_count < jobConfig.api_parameter.api_retry:

                        jobconfig_dict = jobConfig.model_dump()
                        jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                        logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                        http_client = HTTPClient(jobconfig_dict)
                        ecosys_response , success, status_code = http_client.run()
                        if status_code == 500:
                            logger.info(f"{function_name} API for above User has API status - {status_code}")
                            logger.info("So absorbing it and moving to other users")
                            success = False
                            xml_api_s3_upload_complete = False
                            is_no_data_from_api = True
                            break
                        
                        ecosys_xml_resp = ecosys_response
                        # logger.info(f"{function_name} XML API response - {ecosys_response}")
                        # xml_resp_str = ecosys_xml_resp.decode('utf-8')
                        #childtag = function_mapping_p4[function_name][0]
                        #xml_data_et = ET.fromstring(xml_resp_str)
                        #cur_nm = get_namespace(xml_data_et)
                        #funct_dat_tag = ""+cur_nm+childtag
                        #childs_objs = xml_data_et.findall(f"{funct_dat_tag}")
                        #child_find_count = len(childs_objs)
                        
                        #json_data = ecosys_response.json()
                        if success:
                            xml_resp_str = ecosys_xml_resp.decode('utf-8')
                            childtag = function_mapping_p4[function_name][0]
                            xml_data_et = ET.fromstring(xml_resp_str)
                            cur_nm = get_namespace(xml_data_et)
                            funct_dat_tag = ""+cur_nm+childtag
                            childs_objs = xml_data_et.findall(f"{funct_dat_tag}")
                            child_find_count = len(childs_objs)
                            logger.info(f"{function_name} API with tag {funct_dat_tag} has {child_find_count} records")
                            if (
                                (xml_data_et.find("INTa_SnowFlake_ECODATA_SecOrg_Updated") is None
                                or xml_data_et.find("INTa_SnowFlake_User_Projects") is None)
                            ):
                                
                                if child_find_count < 1:
                                    logger.info(f"{funct_dat_tag} API for User - {curr_user} - has 0 records as no datat to write so moving ahead")
                                    success = True
                                    xml_api_s3_upload_complete = False
                                    break
                                
                                if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                                    logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{raw_data_location}")
                                    success = True
                                    xml_api_s3_upload_complete = True
                                else:
                                    logger.error(f"{function_name} API successful but failed to upload to S3")
                                    xml_api_s3_upload_complete = False
                                    success = False
                            else:
                                logger.error(f"{function_name} API successful but failed to upload to S3")
                                xml_api_s3_upload_complete = False
                                success = False
                            break;    
                        else:
                            # Retry API
                            retry_count += 1
                            logger.error(f"{function_name} API unsuccessful")
                            
                    logger.info(f"API status :{success}")
                    if success and xml_api_s3_upload_complete:
                        s3_complete, sample_complete, total_count = process_xml_data(
                            sc,
                            glueContext,
                            function_mapping_p4, 
                            function_name, 
                            object_name, 
                            jobConfig.job_parameter.bucket_name,
                            s3_key, 
                            s3_crawler_key,
                            xml_api_read_context,
                            xml_api_root_tag,
                            standard_schema=stand_schema,
                            )
                        logger.info(f"S3 complete post response :{s3_complete} ")
                        if s3_complete:
                            logger.info(
                                f"Sucessfull completion of {function_name}")
                            logger.info(f"User count processed is at:  {user_counter}")
                            user_counter += 1
                        else:
                            logger.info(
                                f"Failed in generating outcome for {function_name} for user {curr_user}"
                                )
                            if xml_api_s3_upload_complete:
                                logger.info(f"deleting the data from {object_name}")
                                s3_client.delete_file_from_s3(object_name)
                            
                            logger.info("so moving to other users")
                                #sys.exit(1)
                    if not success:
                        if is_no_data_from_api:
                            continue
                        logger.info(f"Failed in generating outcome for {function_name} for user {curr_user}")
                        if xml_api_s3_upload_complete:
                            logger.info(f"Deleting folder path {object_name}")
                            s3_client.delete_file_from_s3(object_name)
                        sys.exit(1)  # Exit with failure code
            elif function_name in function_mapping_p5:
                logger.info(f" ========================ECOSYS {function_name} New project API Invocation Start=====================.")
                # TODO - need to apply the xml related changes
                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [jobConfig.job_parameter.input_path]
                                },
                                format="parquet",
                                transformation_ctx = tf_ctxt
                                )
                selected_columns = ["projectlist_SecOrg", "projectlist_Project"]
                dynamic_frame = dynamic_frame.select_fields(selected_columns)
                df_final = dynamic_frame.toDF()
                df_final.show()

                # Loop through all Project Ids
                for project in df_final.collect():
                    retry_count = 0
                    authentication_failure = False
                    # Call the API for each SecOrg ID, retrying if needed
                    print(f"Project Id is :{project['projectlist_Project']}")
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(
                            function_name, project_id=project["projectlist_Project"])

                    while retry_count < jobConfig.api_parameter.api_retry:

                        jobconfig_dict = jobConfig.model_dump()
                        jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                        logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                        http_client = HTTPClient(jobconfig_dict)
                        ecosys_response , success, status_code = http_client.run()
                        ecosys_xml_resp = ecosys_response
                        logger.info(f"{function_name} API for project={curr_proj_id} and status :{success}")
                        if success:
                            # logger.info(f"{function_name} XML API response - {ecosys_response}")
                            xml_resp_str = ecosys_xml_resp.decode('utf-8')
                            childtag = function_mapping_p5[function_name][0]
                            xml_data_et = ET.fromstring(xml_resp_str)
                            cur_nm = get_namespace(xml_data_et)
                            funct_dat_tag = ""+cur_nm+childtag
                            childs_objs = xml_data_et.findall(f"{funct_dat_tag}")
                            child_find_count = len(childs_objs)
                            logger.info(f"{function_name} API with tag {funct_dat_tag} has {child_find_count} records")
                            success_with_error_code = ""+cur_nm+api_success_with_error_code
                            if (xml_data_et.find(success_with_error_code) is None):
                                if child_find_count < 1:
                                    logger.info(f"{funct_dat_tag} API for User - {curr_user} - has 0 records as no datat to write so moving ahead")
                                    success = False
                                    xml_api_s3_upload_complete = False
                                    logger.info(f"please check the API as it has empty data.")
                                    logger.info("So Exiting gracefully with success")
                                    job.commit()
                                    os._exit(0)
                                
                                if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                                    logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{raw_data_location}")
                                    success = True
                                    xml_api_s3_upload_complete = True
                                else:
                                    logger.error(f"{function_name} API successful but failed to upload to S3")
                                    xml_api_s3_upload_complete = False
                                    success = False
                                
                                if success and xml_api_s3_upload_complete:
                                    s3_complete, sample_complete, total_count = process_xml_data(
                                        sc,
                                        glueContext,
                                        function_mapping_p5, 
                                        function_name, 
                                        object_name, 
                                        jobConfig.job_parameter.bucket_name, 
                                        s3_key, s3_crawler_key,
                                        xml_api_read_context,
                                        xml_api_root_tag,
                                        )
                                    logger.info(f"S3 complete post response :{s3_complete} ")
                                    if s3_complete:
                                        logger.info(
                                            f"Sucessfull completion of {function_name}")
                                    else:
                                        logger.info(
                                            f"Failed in generating outcome for {function_name}")
                                        #sys.exit(1)
                                else:
                                    logger.info(f"Upload to raw path of API data failed for {function_name}")
                                    success = False
                                    xml_api_s3_upload_complete = False
                                    logger.info(f"So exiting with failure fo f{curr_proj_id} and moving to other projects")
                                    #sys.exit(1)  # Exit with failure code 
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        sys.exit(1)  # Exit with failure code
            elif function_name in function_mapping_p6:
                # Call the WBS_List API for each Project ID, retrying if needed

                logger.info(f"Calling WBS List API for Project Id - {project_id}.")
                curent_load_dt=generate_timestamp_string(timezone=TIMEZONE_UTC).strftime(XML_FILE_TIMESTAMP_FORMAT)
                raw_data_location = raw_data_location.format(project_id=project_id,partition_uri=partition_str)
                name=f"{project_id}_{curent_load_dt}"
                xml_api_read_context = xml_api_read_context.format(project_id=project_id)

                # if secorg_id and secorg_id.strip() == '""':
                #     logger.info("secorg is empty")
                #     raw_data_location = raw_data_location.format(partition_uri=partition_str)
                #     name=f"{curent_load_dt}"
                # elif secorg_id:
                #     xml_api_read_context = xml_api_read_context.format(secorg_id=secorg_id)
                #     raw_data_location = raw_data_location.format(secorg_id=secorg_id,partition_uri=partition_str)
                #     name=f"{secorg_id}_{curent_load_dt}"

                function_mapping_p6[function_name]=[xml_api_data_array_root_tag,xml_api_data_array_root_alias]

                # Call the API for each SecOrg ID, retrying if needed
                retry_count = 0
                jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,project_id=project_id)

                logger.info(f"Derived Query Params - {jobConfig.api_parameter.dynamic_api_query_param}.")

                logger.info(f"Job Config Details  - {jobConfig}")

                while retry_count < jobConfig.api_parameter.api_retry:
                    # ecosys_response, success = call_api(
                    #     jobConfig.api_parameter.endpoint, user, password, query_params, jobConfig.api_parameter.api_timeout
                    # )
                    jobconfig_dict = jobConfig.model_dump()
                    jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                    logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                    http_client = HTTPClient(jobconfig_dict)
                    ecosys_response , success , status_code = http_client.run()
                    # logger.info(f"{function_name} XML API response - {ecosys_response}")

                    object_name = f"{raw_data_location}/{name}.xml"
                    if success:
                        if s3_client.upload_to_s3(ecosys_response,object_name,kms_key_id,is_gzip=False):
                            logger.info(f"Uploaded {function_name} API xml to {raw_bckt_name}/{object_name}")
                            success = True
                            xml_api_s3_upload_complete = True
                            xml_data = root = ET.fromstring(ecosys_response)
                        else:
                            logger.error(f"{function_name} API successful but failed to upload to S3")
                            xml_api_s3_upload_complete = False
                            success = False
                    else:
                        if function_name == "archival" and status_code == 500:
                            logger.info(f"{function_name} has 500 and it needs from source so making it as success")
                            os._exit(os.EX_OK)
                        logger.error(f"{function_name} API unsuccessful")
                        success = False
                        sys.exit(1)  # Exit with failure code

                    json_data = ecosys_response
                    s3_complete=False
                    if success:
                        rootsecorg_filter = None
                        rootsecorg_tempview_name = None
                        rootsecorg_raw_path = None
                        if function_name == "secorglist":
                            rootsecorg_filter = metadata['job_parameter']['root_secorg_filter']
                            rootsecorg_tempview_name = metadata['job_parameter']['root_secorg_temp_view_name']
                            if not rootsecorg_tempview_name:
                                rootsecorg_tempview_name = "TopRegionSecOrgTempView"
                            rootsecorg_raw_path = metadata['job_parameter']['root_secorg_raw_path']

                        s3_complete, sample_complete, total_count = process_xml_data(
                        sc,glueContext,function_mapping_p6, function_name,
                        object_name, jobConfig.job_parameter.bucket_name,
                        s3_key, s3_crawler_key,
                        xml_api_read_context,
                        xml_api_root_tag,
                        root_secorg_filter=rootsecorg_filter,
                        top_region_secorg_temp_view_name=rootsecorg_tempview_name,
                        root_secorg_raw_path=rootsecorg_raw_path
                            )
                        if total_count < 1:
                            logger.info(f"{function_name} has 0 data from source so making it as success")
                            if xml_api_s3_upload_complete:
                                logger.info(f"Deleting folder path {raw_data_location}")
                                s3_client.delete_folder_from_s3(raw_data_location)
                            os._exit(os.EX_OK)
                        if s3_complete:
                            logger.info(f"Sucessfull completion of {function_name}")
                        else:

                            logger.info(f"Failed in generating outcome for {function_name}")
                            sys.exit(1)
                        break
                    else:
                        # Retry API
                        retry_count += 1
                if not success:
                    logger.info("Failed to retrieve project list.")
                    if xml_api_s3_upload_complete:
                        logger.info(f"Deleting folder path {raw_data_location}")
                        s3_client.delete_folder_from_s3(raw_data_location)
                    sys.exit(1)  # Exit with failure code

    except Exception as e:
        logger.error(f"Script failed unexpectedly with error", e)
        if xml_api_s3_upload_complete:
            logger.info(f"Deleting folder path {raw_data_location}")
            s3_client.delete_folder_from_s3(raw_data_location)
        # Exit with error code
        sys.exit(1)
    job.commit()


if __name__ == "__main__":
    main()
