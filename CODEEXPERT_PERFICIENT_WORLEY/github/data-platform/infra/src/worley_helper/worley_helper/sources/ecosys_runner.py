import boto3
import logging
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from worley_helper.utils.helpers import write_glue_df_to_s3_with_specific_file_name



client = boto3.client("glue")
logger = logging.getLogger()
# include aws default region variables
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
dynamodb_client = boto3.client("dynamodb")
region_name = boto3.session.Session().region_name
print(region_name)
sns_client = boto3.client("sns", region_name=region_name)
account_id = boto3.client("sts").get_caller_identity()["Account"]
print(account_id)


class Ecosys:
    def __init__(self,topic_arn,env):
        self.run_env = env
        self.sns_topic_arn = topic_arn
        self.emailSubject = "Ecosys Job Failed"
        self.emailBody = "Ecosys Job Failed with below error:"

    @staticmethod
    def get_iam_role_arn(role_name):
        iam_client = boto3.client("iam")
        response = iam_client.get_role(RoleName=role_name)
        return response["Role"]["Arn"]

    # Function to build query parameters based on Function names
    def build_query_parm(self, function_name, **kwargs):
        try:
            logging.info(f"Building query parameters for {function_name} function.")
            query_parm = {}
            if (
                function_name == "project_list"
                or function_name == "header"
                or function_name == "snapshotfull"
                or function_name == "findata"
                or function_name == "findatapcs"
                or function_name == "snapshotlog"
                or function_name == "archival"
                or function_name == "projectKeyMembers"
            ):
                query_parm = {"SecOrg": kwargs["secorg_id"]}
            elif function_name == "TimePhased":
                query_parm = {
                    "SecOrg": kwargs["secorg_id"],
                    "RootCostObject": kwargs["project_id"],
                    "PADate": kwargs["PADate"],
                }
                if "wbs" in kwargs:
                    query_parm["WBS"] = kwargs["wbs"]
            elif function_name in ("actuallog", "workingforecastlog","earnedvaluelog"):
                query_parm = {
                    "SecOrg": kwargs["secorg_id"],
                    "CreateDate": kwargs["createdate"],
                    "ToDate": kwargs["todate"]
                }
            elif function_name == "dcsMapping" or function_name == "gateData":
                query_parm = {"RootCostObject": kwargs["project_id"]}
            elif function_name == "ecodata":
                query_parm = {
                    "SecOrg": kwargs["secorg_id"],
                    "Snapshot": kwargs["snapshotid"],
                }
            elif (
                function_name == "usersecorg"
                or function_name == "userproject"
            ):
                query_parm = {"UserID": kwargs["UserID"]}
            elif function_name in ["deliverable","deliverable_gate","deliverable_source", "change_deliverable", "wbs_list"]:
                query_parm = {"RootCostObject": kwargs["project_id"]}
            elif function_name in ['controlAccount_Snapshot']:
                query_parm = {
                    "RootCostObject": kwargs["project_id"],
                    "SecOrg" : kwargs["secorg_id"]
                }
                if function_name == 'controlAccount_Snapshot':
                    query_parm["SnapShotPeriod"] = kwargs["snapshot_date"]
            elif function_name in ['projectwise_snapshot']:
                query_parm = {
                    "RootCostObject": kwargs["project_id"],
                    "LastUpdateDate" : kwargs["LastUpdateDate"]
                }
            elif function_name in ['controlAccount']:
                query_parm = {
                    "SecOrg" : kwargs["secorg_id"]
                }                

            logging.info(f"Resultant Query Parameters are : {query_parm} function.")
            return query_parm
        except ClientError as e:
            logger.error(str(e))
            self.sendSNSNotification(str(e))

    # Function to calculate last Friday date when today date is being passed
    def get_last_friday(self, today):

        if today.weekday() == 4:  # 4 represents Friday
            last_friday = today - timedelta(days=7)
        else:
            last_friday = today - timedelta((today.weekday() - 4) % 7)

        current_date = datetime.now()

        if current_date.day > 10:
            PADate = last_friday.strftime("%#d-%b-%y")

        else:
            PADate = last_friday.strftime("%-d-%-b-%y")

        return PADate


    def process_data(self,sc,glueContext,function_mapping, function_name, ecosys_response, s3_bucket_name, s3_key, s3_crawler_key, **kwargs):

        spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

        s3_complete = sample_complete = True

        df = spark.read.json(sc.parallelize([ecosys_response.text])).select(explode(function_mapping[function_name][0]).alias(function_mapping[function_name][1]))

        if not df.rdd.isEmpty():
            sample_fraction = 0.5
            sample_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
            ecosys_df = DynamicFrame.fromDF(df, glue_ctx=glueContext, name="df")
            sample_dynamic_frame = DynamicFrame.fromDF(sample_df, glue_ctx=glueContext, name="sampledf")
            transformation_context = function_name

            relationalized_df = ecosys_df.relationalize("root", "s3://worley-test-bucket/temp-path/")
            sample_relationalized_df = sample_dynamic_frame.relationalize("root", "s3://worley-test-bucket/temp-path/")

            secorg_details_entities = ["ecodata","findatapcs","findata","snapshotfull","snapshotlog"]

            for node in ['root']:
                new_dynamic_frame = relationalized_df.select(node)
                new_sample_dynamic_frame = sample_relationalized_df.select(node)
            if function_name in secorg_details_entities:
                secorg_id = kwargs.get('secorg_id', "")
                snapshot_id = kwargs.get('snapshot_id', "")
                new_df = new_dynamic_frame.toDF()
                new_samp_df = new_sample_dynamic_frame.toDF()
                new_sec_df = new_df.withColumn('secorg_id',lit(secorg_id))
                new_sec_samp_df = new_samp_df.withColumn('secorg_id',lit(secorg_id))
                if function_name == "ecodata" and len(snapshot_id.strip()) > 0 :
                    logging.info(f"ecodata secorg-{secorg_id} and snapshot_id - {snapshot_id}")
                    new_sec_df = new_sec_df.withColumn('snapshot_id',lit(snapshot_id))
                    new_sec_samp_df = new_sec_samp_df.withColumn('snapshot_id',lit(snapshot_id))
                
                new_dynamic_frame = DynamicFrame.fromDF(new_sec_df, glue_ctx=glueContext, name="new_df")
                new_sample_dynamic_frame = DynamicFrame.fromDF(new_sec_samp_df, glue_ctx=glueContext, name="new_sampledf")
                

            s3_complete = write_glue_df_to_s3_with_specific_file_name(
                glueContext, new_dynamic_frame, s3_bucket_name, s3_key , transformation_context
            )
            sample_complete = write_glue_df_to_s3_with_specific_file_name(
                glueContext, new_sample_dynamic_frame, s3_bucket_name, s3_crawler_key , transformation_context
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

        return s3_complete, sample_complete


    def sendSNSNotification(self, err_msg):
        """
        Sends SNS email notification when an exception is encountered.
        :err_msg: Error message thrown when exception encountered in any method
        :return: Dictionary. Check the structure at
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        """
        try:
            sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=self.emailBody + "\n" + str(err_msg),
                Subject=self.emailSubject,
            )
        except Exception as e:
            logger.error(str(e))
