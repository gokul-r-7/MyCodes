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


client = boto3.client("glue")
logger = logging.getLogger()
# include aws default region variable
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
dynamodb_client = boto3.client("dynamodb")
region_name = boto3.session.Session().region_name
print(region_name)
sns_client = boto3.client("sns", region_name=region_name)
account_id = boto3.client("sts").get_caller_identity()["Account"]
print(account_id)


class DB:
    def __init__(self,topic_arn,env,db_type):
        self.run_env = env
        self.sns_topic_arn = topic_arn
        self.db_type = db_type
        self.emailSubject = "Ecosys Job Failed"
        self.emailBody = "Ecosys Job Failed with below error:"

    @staticmethod
    def get_iam_role_arn(role_name):
        iam_client = boto3.client("iam")
        response = iam_client.get_role(RoleName=role_name)
        return response["Role"]["Arn"]

    def create_jdbc_options(self,db_host_url, tbl_query, user, password,jdbc_part_column=None, min_pk=None, max_pk=None, num_partitions=None):
        jdbc_options = {
          "url": db_host_url,
          "query": tbl_query,
          "user": user,
          "password": password,
          "ssl": False,
          "zeroDateTimeBehavior": "convertToNull",
          "autoReconnect": "true",
          "characterEncoding": "UTF-8",
          "characterSetResults": "UTF-8"
         }
    # Add MSSQL Driver if Provided
        if self.db_type == "MSSQL":
            jdbc_options.update({
              "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
             })

    # Add partitioning options only if jdbc_part_column is provided
        if jdbc_part_column:
            jdbc_options.update({
            "partitionColumn": jdbc_part_column,
            "lowerBound": min_pk,
            "upperBound": max_pk,
            "numPartitions": num_partitions
             })
    #print(f"jdbc_options : {jdbc_options}")
        return jdbc_options



    def get_select_query(self,db_name, table_name, full_incr_value, column_expression, date_delta_column_value, delta_start_date, delta_end_date, partition_column_value):
        if full_incr_value == "i":
            
            
            
            if self.db_type == "MSSQL":
                tbl_query = f"SELECT {column_expression} FROM {db_name}.[{table_name}] t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
                sample_tbl_query = f"SELECT TOP 100 {column_expression} FROM {db_name}.[{table_name}] t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
            elif self.db_type == "ORACLE":
                tbl_query = f"SELECT {column_expression} FROM {db_name}.{table_name} t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
                sample_tbl_query = f"SELECT {column_expression} FROM {db_name}.{table_name} t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}' WHERE ROWNUM <= 100"
            else:
                tbl_query = f"SELECT {column_expression} FROM {db_name}.{table_name} t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
                sample_tbl_query = "select * from " + db_name + "." + table_name + " t where " + date_delta_column_value + " > '" + str(delta_start_date) + "' and " + date_delta_column_value + " <= '" + str(delta_end_date) + "' limit 100"


            if partition_column_value != "None":
                if self.db_type == "MSSQL":
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt, MIN({partition_column_value}) AS min_pc, MAX({partition_column_value}) AS max_pc FROM {db_name}.[{table_name}] t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
                else:
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt, MIN({partition_column_value}) AS min_pc, MAX({partition_column_value}) AS max_pc FROM {db_name}.{table_name} t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
            else:
                if self.db_type == "MSSQL":
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt FROM {db_name}.[{table_name}] t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
                else:
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt FROM {db_name}.{table_name} t WHERE {date_delta_column_value} > '{delta_start_date}' AND {date_delta_column_value} <= '{delta_end_date}'"
        else:
            if self.db_type == "MSSQL":
                tbl_query = f"SELECT {column_expression} FROM {db_name}.[{table_name}] t"
            else:
                tbl_query = f"SELECT {column_expression} FROM {db_name}.{table_name} t"
            
            if self.db_type == "MSSQL":
                sample_tbl_query = f"SELECT TOP 100 {column_expression} FROM {db_name}.[{table_name}]"
            elif self.db_type == "ORACLE":
                sample_tbl_query = f"SELECT  {column_expression} FROM {db_name}.{table_name} WHERE ROWNUM <= 100"
            else:
                sample_tbl_query = "select * from " + db_name + "." + table_name + " limit 100"


            if partition_column_value != "None":
                if self.db_type == "MSSQL":
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt, MIN({partition_column_value}) AS min_pc, MAX({partition_column_value}) AS max_pc FROM {db_name}.[{table_name}] t"
                else:
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt, MIN({partition_column_value}) AS min_pc, MAX({partition_column_value}) AS max_pc FROM {db_name}.{table_name} t"
                
            else:
                if self.db_type == "MSSQL":
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt FROM {db_name}.[{table_name}] t"
                else:
                    min_max_tbl_query = f"SELECT COUNT(*) AS cnt FROM {db_name}.{table_name} t"

        return tbl_query, sample_tbl_query, min_max_tbl_query

    def connect_jdbc(self,spark,jdbc_options):
        df  = spark.read.format("jdbc").options(**jdbc_options).load()
        return df


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
