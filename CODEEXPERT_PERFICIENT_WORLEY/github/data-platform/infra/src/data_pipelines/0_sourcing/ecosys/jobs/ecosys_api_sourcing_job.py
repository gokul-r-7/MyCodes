import sys
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
# from pyspark.sql.functions import year, month, dayofmonth, hour
import pytz
from dateutil.parser import parse
from dateutil.tz import gettz

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
)
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.api_helpers import call_api
from worley_helper.utils.aws import DynamoDB , SecretsManager, S3
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.configuration.config import EcosysConfiguration
from worley_helper.sources.ecosys_runner  import Ecosys
from worley_helper.utils.http_api_client import HTTPClient

def process_rawdatasampling(source_df,sampling_path,s3_bucket_name,sampl_cntxt,glueContext,sample_fraction = 0.5,sample_seed=42):
    sample_complete = False
    if not source_df.rdd.isEmpty():
        sample_df = source_df.sample(withReplacement=False, fraction=sample_fraction, seed=sample_seed)
        sample_dynamic_frame = DynamicFrame.fromDF(sample_df, glue_ctx=glueContext, name="sampledf")
        sample_complete = write_glue_df_to_s3_with_specific_file_name(
            glueContext, sample_dynamic_frame, s3_bucket_name, sampling_path , sampl_cntxt
            )
    return sample_complete

def main():

    # Extract the arguments passed from the Airflow DAGS into Glue Job
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "source_name", "function_name","metadata_table_name","secorg_id","start_date", "end_date","environment","topic_arn"]
    )
    print(args)
    source_name = args.get("source_name")
    function_name = args.get("function_name")
    metadata_table_name = args.get("metadata_table_name")
    arg_secorg_ids = args.get("secorg_id")
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
    input_keys = "api#" + source_name + "#" + function_name

    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=input_keys
    )

    logger.info(f" Metadata Response :{metadata}")
    # Setup Configuration
    jobConfig = EcosysConfiguration(**metadata)
    print(f"Type of Job Config :{type(jobConfig)}")

    try:
        # Create the dictionary containing arguments for query call
        function_mapping_p1 = {
            "project_list": ["CostObjectList", "projectlist"],
            "costtypepcsmapping": ["CostObjectCategoryList", "costtypepcsmapping"],
            "calendar": ["TransactionCategoryList", "calendar"],
            "header": ["CostObjectList", "header"],
            "archival": ["CostObjectList", "archival"],
            "snapshotfull": ["WorkingForecastTransactionList", "snapshotfull"],
            "snapshotlog" : ["WorkingForecastTransactionList", "snapshotlog"],
            "users": ["UserList", "users"],
            "findata": ["TransactionSummaryList", "findata"],
            "findatapcs": ["TransactionSummaryList", "findatapcs"],
            "snapidlist": ["TransactionCategoryList", "snapiddlist"],
            "secorglist": ["OrganizationList", "snapiddlist"],
            
        }
        function_mapping_p2 = {
            "dcsMapping": ["WorkingForecastTransactionList", "dcsmapping"],
            "gateData": ["WorkingForecastTransactionList", "gatedata"],
            "TimePhased": ["TransactionSummaryList", "timephased"],
        }
        function_mapping_p3 = {"ecodata": ["TransactionSummaryList", "ecodata"]}

        function_mapping_p4 = {
            "usersecorg": ["UserOrganizationSecurityGroupConfigurationList", "usersecorg"],
            "userproject": ["UserCostObjectSecurityGroupConfigurationList", "userproject"]
        }
        function_mapping_p5 = {
            "deliverable": ["TaskList", "deliverable"],
            "deliverable_source": ["TransactionSummaryList", "deliverable_source"],
            "deliverable_gate": ["WorkingForecastTransactionList", "deliverable_gate"],
        }
        logger.info(
    "==================Setting up the Spark and Glue Context ============"
        )

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
            tf_ctxt = source_name + function_name + arg_secorg_ids

        s3_crawler_key = "project_control/" + source_name + "/data_sampling/" + function_name + "/"
        s3_snapshot_processed_path = (
            "s3://" + jobConfig.job_parameter.bucket_name + "/project_control/" + source_name + "/snapshot_processed/" + arg_secorg_ids
        )
        s3_snapshot_processed_uri = "project_control/" + source_name + "/snapshot_processed/" + arg_secorg_ids  
        s3_snapshot_processed_crawler_path = "project_control/" + source_name + "/data_sampling/"+ "snapshot_processed/" 
        s3_ecosys_path = jobConfig.job_parameter.input_path + arg_secorg_ids
        secorg_details_entities = ["findatapcs","findata","snapshotfull","snapshotlog"]
        
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

                    # Call the API for each SecOrg ID, retrying if needed
                    retry_count = 0
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
                        json_data = ecosys_response.json()
                        s3_complete=False
                        if success:
                            if (
                                function_name == "archival"
                            ):
                                if json_data.get("INTa_Snowflake_Project_List_All_ArchError") is None:
                                    s3_complete, sample_complete = ecosys.process_data(
                                        sc,glueContext,function_mapping_p1, function_name, ecosys_response, jobConfig.job_parameter.bucket_name, s3_key, s3_crawler_key
                                        )
                                    print(f"S3 complete post response :{s3_complete} ")
                                else:
                                    logger.info(
                                        f"Failed in generating outcome for {function_name} with 404 error INTa_Snowflake_Project_List_All_ArchError"
                                    )
                                    s3_complete = False
                                    success = False
                            elif function_name in secorg_details_entities:
                                s3_complete, sample_complete = ecosys.process_data(
                                    sc,
                                    glueContext,
                                    function_mapping_p1,
                                    function_name,
                                    ecosys_response, 
                                    jobConfig.job_parameter.bucket_name,
                                    s3_key, 
                                    s3_crawler_key,
                                    secorg_id=secorg_id
                                )
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
                                    
                                s3_complete, sample_complete = ecosys.process_data(
                                sc,glueContext,function_mapping_p1, function_name, 
                                ecosys_response, jobConfig.job_parameter.bucket_name,
                                s3_key, s3_crawler_key,
                                root_secorg_filter=rootsecorg_filter,
                                top_region_secorg_temp_view_name=rootsecorg_tempview_name,
                                root_secorg_raw_path=rootsecorg_raw_path
                                    )
                            if s3_complete:
                                    logger.info(
                                        f"Sucessfull completion of {function_name}"
                                    )
                            else:
                                    logger.info(
                                        f"Failed in generating outcome for {function_name}"
                                    )
                                    sys.exit(1)
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        sys.exit(1)  # Exit with failure code

            elif function_name in function_mapping_p2:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")
                # df_final = spark.read.parquet(jobConfig.job_parameter.input_path).select("projectlist_SecOrg", "projectlist_Project")
                # df_final.show()

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
                    if function_name == "TimePhased":
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name,secorg_id=project["projectlist_SecOrg"],project_id=project["projectlist_Project"],
                            PADate=ecosys.get_last_friday(today),
                        )
                    else:
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(
                            function_name, project_id=project["projectlist_Project"])

                    while retry_count < jobConfig.api_parameter.api_retry:

                        jobconfig_dict = jobConfig.model_dump()
                        jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                        logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                        http_client = HTTPClient(jobconfig_dict)
                        ecosys_response , success, status_code = http_client.run()
                        json_data = ecosys_response.json()

                        # ecosys_response, success = call_api(jobConfig.api_parameter.endpoint, user, password, query_params, jobConfig.api_parameter.api_timeout)
                        # json_data = ecosys_response.json()

                        print(f"success :{success}")
                        if success:
                            if (
                                json_data.get("INTa_DEL_EDSR_DCS_MappingError") is None
                                and json_data.get("INTa_DEL_EDSR_Gate_DataError") is None
                                and json_data.get("Ecosys_ControlAccount_TimePhasedError") is None
                            ):
                                s3_complete, sample_complete = ecosys.process_data(
                                sc,glueContext,function_mapping_p2, function_name, ecosys_response, jobConfig.job_parameter.bucket_name, s3_key, s3_crawler_key
                                )
                                print(f"S3 complete post response :{s3_complete} ")
                                if s3_complete:
                                    logger.info(
                                        f"Sucessfull completion of {function_name}")
                                else:
                                    logger.info(
                                            f"Failed in generating outcome for {function_name}"
                                        )
                                    sys.exit(1)
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        sys.exit(1)  # Exit with failure code
            elif function_name in function_mapping_p3:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")
                logger.info(f" ========================ECOSYS S3 PATH ===== {s3_ecosys_path}=====================.")
                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [s3_ecosys_path]
                                },
                                format="parquet",
                                transformation_ctx = tf_ctxt
                                )
                selected_columns = ["snapshotfull_CreateDate", "snapshotfull_SnapshotIntID"]
                dynamic_frame = dynamic_frame.select_fields(selected_columns)
                df_final = dynamic_frame.toDF()
                new_df_final = df_final.select("snapshotfull_SnapshotIntID",substring('snapshotfull_CreateDate', 1,10).alias('snapshotfull_CreateDate'))

                # df_final = spark.read.parquet(jobConfig.job_parameter.input_path).select("snapshotfull_CostObjectID", "snapshotfull_SnapshotIntID")
                #df_delta = (spark.read.parquet(s3_snapshot_processed_path).select(substring("snapshotfull_CreateDate", 1,10).alias('snapshotfull_CreateDate'), "snapshotfull_SnapshotIntID").filter(col("Processed") == "success"))
                print(new_df_final.schema)
                print(f"Count of available snapshots {new_df_final.count()}")
                #new_df_final=new_df_final.distinct()
                print(f"Count of available distinct snapshots {new_df_final.count()}")
                new_df_final=new_df_final.withColumn('snapshotfull_CreateDate_temp',to_date(col('snapshotfull_CreateDate'),"yyyy-MM-dd"))
                # Per confirmation from Vikash needed atleast 2 years data for prod
                new_df_final_temp = new_df_final.filter(col('snapshotfull_CreateDate_temp') > "2023-01-01")
                
                #new_df_final_temp=new_df_final_temp.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")
                #new_df_final_temp = new_df_final.filter(col('snapshotfull_CreateDate_temp') > "2024-12-10")
                new_df_final=new_df_final_temp.drop('snapshotfull_CreateDate_temp')
                print(f"Count of available distinct snapshots {new_df_final.count()}")
                s3_client = S3(jobConfig.job_parameter.bucket_name, default_region=REGION)
                if s3_client.s3_folder_exists(s3_snapshot_processed_uri):
                    df_delta = (spark.read.parquet(s3_snapshot_processed_path).select(substring("snapshotfull_CreateDate", 1,10).alias('snapshotfull_CreateDate'), "snapshotfull_SnapshotIntID").filter(col("Processed") == "success"))
                    df_delta=df_delta.distinct()
                    print(f"Count of available distinct processed snapshots {df_delta.count()}")
                else:
                    schema=new_df_final.schema
                    df_delta=spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
                    print(df_delta.schema)
                unfiltered_df_diff = new_df_final.select("snapshotfull_CreateDate", "snapshotfull_SnapshotIntID").subtract(df_delta.select("snapshotfull_CreateDate", "snapshotfull_SnapshotIntID"))
                windowSpec = Window.partitionBy("snapshotfull_SnapshotIntID").orderBy(col("snapshotfull_CreateDate_temp").desc())
                df_diff = unfiltered_df_diff.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")
                df_diff.show()
                # Loop through all Project Ids
                for secorg_id in secorg_ids:
                    for snapshot in df_diff.collect():
                        retry_count = 0
                        # Call the API for each SecOrg ID, retrying if needed
                        print(
                            f"Snapshotid is :{snapshot['snapshotfull_SnapshotIntID']}"
                        )
                        jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(
                            function_name,
                            secorg_id=secorg_id,
                            snapshotid=snapshot["snapshotfull_SnapshotIntID"],
                        )
                        print(f"Query Params :{jobConfig.api_parameter.api_query_params}")


                        while retry_count < jobConfig.api_parameter.api_retry:


                            jobconfig_dict = jobConfig.model_dump()
                            jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                            logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                            http_client = HTTPClient(jobconfig_dict)
                            ecosys_response , success , status_code = http_client.run()
                            json_data = ecosys_response.json()

                            # ecosys_response, success = call_api(jobConfig.api_parameter.endpoint, user, password, query_params, jobConfig.api_parameter.api_timeout)
                            # json_data = ecosys_response.json()
                            # print(f"success :{success}")
                            snapshot_id_process = snapshot["snapshotfull_SnapshotIntID"]
                            if success:
                                if (
                                    json_data.get("INTa_SnowFlake_ECODATA_SecOrg_UpdatedError") is None
                                    and not len(json_data[function_mapping_p3[function_name][0]]) == 0
                                    ):
                                    
                                    s3_complete, sample_complete = ecosys.process_data(
                                        sc,
                                        glueContext,
                                        function_mapping_p3, 
                                        function_name, 
                                        ecosys_response, 
                                        jobConfig.job_parameter.bucket_name, 
                                        s3_key, 
                                        s3_crawler_key,
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
                                            logger.info(
                                                f"Failed in generating outcome for {function_name}"
                                            )
                                            sys.exit(1)
                                else:
                                    all_snapshot_data = unfiltered_df_diff.filter(unfiltered_df_diff.snapshotfull_SnapshotIntID.contains(snapshot_id_process)).collect()
                                    snapshot_df = spark.createDataFrame(all_snapshot_data, df_final.schema)
                                    df_snapshot = snapshot_df.withColumn("Processed", lit("success")).withColumn("secorg_id", lit(secorg_id))
                                    df_snapshot.write.mode("append").parquet(s3_snapshot_processed_path)
                                    logger.info(f"Sucessfull completion of {function_name}")
                                    s3_sample_complete = process_rawdatasampling(df_snapshot,s3_snapshot_processed_crawler_path,jobConfig.job_parameter.bucket_name,tf_ctxt,glueContext)
                                    if s3_sample_complete:
                                        logger.info(f"Sucessfull completion of {function_name} in datasampling")

                                break
                            else:
                                # Retry API
                                retry_count += 1
                            if not success:
                                logger.info("Failed to retrieve project list.")
                                sys.exit(1)  # Exit with failure code

            elif function_name in function_mapping_p4:
                logger.info(f" ========================ECOSYS {function_name} API Invocation Start=====================.")

                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={
                                "paths": [jobConfig.job_parameter.input_path]
                                },
                                format="parquet",
                                transformation_ctx = tf_ctxt
                                )
                selected_columns = ["users_LoginName"]
                dynamic_frame = dynamic_frame.select_fields(selected_columns)
                df_final = dynamic_frame.toDF()
                df_final.show()
                for user in df_final.collect():
                    retry_count = 0
                    user_counter = 1
                    authentication_failure = False
                    # Call the API for each User ID, retrying if needed
                    print(f"User Id is :{user['users_LoginName']}")
                    jobConfig.api_parameter.api_query_params = ecosys.build_query_parm(function_name, UserID=user["users_LoginName"])
                    
                    while retry_count < jobConfig.api_parameter.api_retry:

                        jobconfig_dict = jobConfig.model_dump()
                        jobconfig_dict ['api_parameter']['api_headers'] ['Authorization'] = jobConfig.api_parameter.dynamic_api_headers.Authorization

                        logger.info(f"Job Config Dictionary Details  - {jobconfig_dict}")

                        http_client = HTTPClient(jobconfig_dict)
                        ecosys_response , success, status_code = http_client.run()
                        json_data = ecosys_response.json()
                        print(f"success :{success}")
                        if success:
                            if (
                                json_data.get("INTa_SnowFlake_ECODATA_SecOrg_Updated") is None
                                and json_data.get("INTa_SnowFlake_User_Projects") is None
                            ):
                                s3_complete, sample_complete = ecosys.process_data(
                                sc,glueContext,function_mapping_p4, function_name, ecosys_response, jobConfig.job_parameter.bucket_name, s3_key, s3_crawler_key
                                )
                                print(f"S3 complete post response :{s3_complete} ")
                                if s3_complete:
                                    logger.info(
                                        f"Sucessfull completion of {function_name}")
                                    print(f"User count processed is at:  {user_counter}")
                                    user_counter += 1
                                else:
                                    logger.info(
                                            f"Failed in generating outcome for {function_name}"
                                        )
                                    sys.exit(1)
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        sys.exit(1)  # Exit with failure code
            elif function_name in function_mapping_p5:
                logger.info(f" ========================ECOSYS {function_name} New project API Invocation Start=====================.")
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
                        json_data = ecosys_response.json()

                        # ecosys_response, success = call_api(jobConfig.api_parameter.endpoint, user, password, query_params, jobConfig.api_parameter.api_timeout)
                        # json_data = ecosys_response.json()

                        print(f"success :{success}")
                        if success:
                            s3_complete, sample_complete = ecosys.process_data(
                                sc,glueContext,function_mapping_p2, function_name, ecosys_response, jobConfig.job_parameter.bucket_name, s3_key, s3_crawler_key
                            )
                            print(f"S3 complete post response :{s3_complete} ")
                            if s3_complete:
                                logger.info(
                                    f"Sucessfull completion of {function_name}")
                            else:
                                logger.info(
                                        f"Failed in generating outcome for {function_name}"
                                    )
                                sys.exit(1)
                            break
                        else:
                            # Retry API
                            retry_count += 1
                    if not success:
                        logger.info("Failed to retrieve project list.")
                        sys.exit(1)  # Exit with failure code                                                    
    except Exception as e:
        logger.error(f"Script failed unexpectedly with error", e)
        # Exit with error code
        sys.exit(1)
    job.commit()


if __name__ == "__main__":
    main()
