import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, date_format,current_timestamp,unix_timestamp,to_timestamp
from pyspark.sql import functions as Func
from worley_helper.utils.logger import get_logger

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
from worley_helper.utils.helpers import get_partition_str_mi , write_spark_df_to_s3_with_specific_file_name
from worley_helper.utils.aws import DynamoDB , SecretsManager , get_parameter
from worley_helper.utils.date_utils import generate_timestamp_string
from worley_helper.configuration.config import DbConfiguration
from worley_helper.sources.db_runner  import DB


def main():

    # Extract the arguments passed from the Airflow DAGS into Glue Job
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "source_name","table_name","metadata_table_name","start_date", "end_date","environment","domain_name"]
    )
    print(args)
    source_name = args.get("source_name")
    table_name = args.get("table_name")
    metadata_table_name = args.get("metadata_table_name")
    job_name = args["JOB_NAME"]
    job_run_id = args["JOB_RUN_ID"]
    delta_start_date = parse(args["start_date"])
    delta_end_date = parse(args["end_date"])
    domain_name = args.get("domain_name")
  
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
    job.init(args["JOB_NAME"] + source_name + table_name, args)
 


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
    input_keys = "db#" + source_name + "#" + table_name

    # Read Metadata
    ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
    metadata = ddb.get_metadata_from_ddb(
        source_system_id=source_name, metadata_type=input_keys
    )
    
    logger.info(f" Metadata Response :{metadata}")
    # Setup Configuration
    jobConfig =DbConfiguration(**metadata)
    print(f"Type of Job Config :{type(jobConfig)}")

    try:
        # Create the dictionary containing arguments for query call
        
        rows = str(0)       
        today = datetime.today()
        topic_arn = (
            "arn:aws:sns:ap-southeast-2:891377181979:worley-data-platform-notification-topic-sydney-dev-sns-topic-data-platform-notification"
        )



        logger.info("==================Fetching DB Secrets ============")

        sm = SecretsManager(default_region=REGION)
        secret_params = json.loads(sm.get_secret(jobConfig.db_parameter.secret_key))
        user = secret_params.get("username")
        password = secret_params.get("password")

        partition_date = generate_timestamp_string(
            timezone=TIMEZONE_SYDNEY
        ).strftime(DATETIME_FORMAT)
        # print(f"partition_date : {partition_date}")
        partition_str = f"{get_partition_str_mi(partition_date)}"
        
        
        if args["environment"] == "Prod":
            db_name = jobConfig.db_parameter.db_name_prod
        else:
            db_name = jobConfig.db_parameter.db_name_nonprod


        partition_column_value = jobConfig.db_parameter.partition_column
        s3_key = domain_name + "/" + source_name + "/" + table_name + "/"  + partition_str

        s3_crawler_key = domain_name + "/" + source_name + "/data_sampling/" + table_name + "/" + "sample_" + table_name + ".parquet" 
        s3_output_path = (
            "s3://" + jobConfig.job_parameter.bucket_name + "/" + s3_key
        )
        s3_sample_output_file = (
            "s3://" + jobConfig.job_parameter.bucket_name + "/" + s3_crawler_key  
        )

        if partition_column_value != "None" :
            column_expression = "*"
            jdbc_part_column = jobConfig.db_parameter.partition_column
        else:
            column_expression = "*"
            jdbc_part_column = None

        print(f"output_s3_value :{s3_key}")

        partition_date = generate_timestamp_string(
            timezone=TIMEZONE_SYDNEY
        ).strftime(DATETIME_FORMAT)
        # print(f"partition_date : {partition_date}")
        partition_str = f"{get_partition_str_mi(partition_date)}"


        logger.info("==================Retrieving DDB Config Parameters ============")


        print("fetching DB URL")    
        db_host_url  = get_parameter(jobConfig.db_parameter.connection_parms)
        date_delta_column_value = jobConfig.db_parameter.date_delta_column

        db = DB(topic_arn,args["environment"],jobConfig.db_parameter.db_type)

        logger.info("==================Formulating Query ============")

        
        
        tbl_query, sample_tbl_query, min_max_tbl_query = db.get_select_query(db_name, table_name, jobConfig.job_parameter.full_incr, column_expression, date_delta_column_value, delta_start_date, delta_end_date, partition_column_value)

        
        logger.info(f"==================Table Query  :{tbl_query} ============")
        logger.info(f"==================Table Query  :{min_max_tbl_query} ============")
        logger.info(f"==================Table Query  :{sample_tbl_query} ============")


        # dataframe to check the record count 
        src_data_frame_cnt_jdbc_options = db.create_jdbc_options(db_host_url, min_max_tbl_query, user, password)
        src_data_frame_cnt = db.connect_jdbc(spark,src_data_frame_cnt_jdbc_options)
        src_data_frame_cnt = src_data_frame_cnt.collect()

        if partition_column_value != "None":
            row_count, min_pk, max_pk = src_data_frame_cnt[0][0], src_data_frame_cnt[0][1], src_data_frame_cnt[0][2]
        else:
            row_count = src_data_frame_cnt[0][0]

        
        logger.info(f"==================incremental records to be processed :{row_count} ============")

        #row_count = src_data_frame_cnt.count()

        if row_count > 0:
            rows = str(row_count)

            if row_count > 1000000:
                num_partitions = round(row_count/1000000)*10

            if partition_column_value != "None" :

                print(f"Partition key exists : {jobConfig.db_parameter.partition_column}")

                print(f"lower bound :{min_pk} , upper bound :{max_pk}")

                if num_partitions > 0:

                    # dataframe to read incremental data 
                    print(f"reading data from source db in {num_partitions} number of parititons")
                    src_data_frame_jdbc_options = db.create_jdbc_options(db_host_url, tbl_query, user, password,jdbc_part_column, min_pk, max_pk, num_partitions)       
                else:
                    print(f"reading data from source db with default options")
                    src_data_frame_jdbc_options = db.create_jdbc_options(db_host_url, tbl_query, user, password)
            else:
                # dataframe to read incremental data
                print(f"Partition key does not exists")
                print(f"reading data from source db with default options")
                src_data_frame_jdbc_options = db.create_jdbc_options(db_host_url, tbl_query, user, password)
            
            src_data_frame = db.connect_jdbc(spark,src_data_frame_jdbc_options)

            src_data_frame = src_data_frame.drop("part_col")
            
            print(f"Processing the incremental records : {rows}")

            timestamp_columns = [col_name for col_name, col_type in src_data_frame.dtypes if "timestamp" in col_type.lower()]

            print(f"timestamp_columns : {timestamp_columns}")

            timestamp_format = "yyyy-MM-dd HH:mm:ss"

            for timestamp_col in timestamp_columns:
                src_data_frame = src_data_frame.withColumn(timestamp_col, to_timestamp(Func.when(src_data_frame[timestamp_col] < "1900-01-01 00:00:00", "1900-01-01 00:00:00").otherwise(src_data_frame[timestamp_col]),timestamp_format))

            print("Printing source data schema")

            src_data_frame.printSchema()

            logger.info(f"==================Writing Records to S3 ============")
            src_data_frame.write.mode("append").parquet(s3_output_path)

            
            src_data_frame_sample_jdbc_options = db.create_jdbc_options(db_host_url, sample_tbl_query, user, password)
            src_data_frame_sample = db.connect_jdbc(spark,src_data_frame_sample_jdbc_options)

            for timestamp_col in timestamp_columns:
                src_data_frame_sample = src_data_frame_sample.withColumn(timestamp_col, to_timestamp(Func.when(src_data_frame_sample[timestamp_col] < "1900-01-01 00:00:00", "1900-01-01 00:00:00").otherwise(src_data_frame_sample[timestamp_col]),timestamp_format))

            logger.info(f"==================Writing Sample Records to S3 ============")
            
            write_spark_df_to_s3_with_specific_file_name(src_data_frame_sample, s3_sample_output_file)

            job_end_time = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(TIMESTAMP_FORMAT)

            duration = (datetime.strptime(job_end_time, TIMESTAMP_FORMAT) - datetime.strptime(job_start_time, TIMESTAMP_FORMAT)).total_seconds()

            duration = str(duration)

            # put_items =  dynamo_utils.create_put_item_input(job_run_id,job_name,jobinfo,job_start_time,job_end_time,str(delta_end_date),status,duration,rows,error_message)

            # dynamo_utils.execute_put_item(dynamodb_client, put_items)

        else:

            print(f"No records to process, rows : {rows}")

            job_end_time = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(TIMESTAMP_FORMAT)

            duration = (datetime.strptime(job_end_time, TIMESTAMP_FORMAT) - datetime.strptime(job_start_time, TIMESTAMP_FORMAT)).total_seconds()

            duration = str(duration)

            # put_items =  dynamo_utils.create_put_item_input(job_run_id,job_name,jobinfo,job_start_time,job_end_time,str(delta_end_date),status,duration,rows,error_message)

            # dynamo_utils.execute_put_item(dynamodb_client, put_items) 

    except Exception as e:
        logger.error(f"Script failed unexpectedly with error", e)
        print(f"Status: failed due to the mentioned exception -  {str(e)}")
        error_message = str(e)
        job_end_time = generate_timestamp_string(timezone=TIMEZONE_SYDNEY).strftime(TIMESTAMP_FORMAT)
        duration = (datetime.strptime(job_end_time, TIMESTAMP_FORMAT) - datetime.strptime(job_start_time, TIMESTAMP_FORMAT)).total_seconds()
        duration = str(duration)
        rows = str(row_count)
        # put_items =  dynamo_utils.create_put_item_input(job_run_id,job_name,jobinfo,job_start_time,job_end_time,str(delta_end_date),status,duration,rows,error_message)
        # dynamo_utils.execute_put_item(dynamodb_client, put_items)
        # Exit with error code
        sys.exit(1)
    job.commit()


if __name__ == "__main__":
    main()
