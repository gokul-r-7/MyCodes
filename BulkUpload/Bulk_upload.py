import io
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'OBJECT_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BucketName = args['BUCKET_NAME']
ObjectName = args['OBJECT_NAME']

column_names = ["Id", "First Name", "Last Name", "Gender", "Country", "Age", "Date", "Salary"]

s3 = boto3.client('s3')



postgresurl = "jdbc:postgresql://postgresql.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:5432/postgres"
postgrestablename = "sample_excel_sheet"
postgresdbname = "postgres"
username = "postgres_admin"
password = "password"

def postgres():
    postgres_node = DynamicFrame.fromDF(Excel_Sparkdf, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
        #                    "database": postgresdbname,
                            "dbtable": postgrestablename,
                            "user": username,
                            "password": password
                        }
    postgres = glueContext.write_dynamic_frame.from_options(postgres_node, connection_type = "postgresql", connection_options=write_postgre_options,transformation_ctx = "postgres")

s3_object = s3.get_object(Bucket=BucketName, Key=ObjectName)
Excel_df = pd.read_excel(io.BytesIO(s3_object['Body'].read()))

df=Excel_df.applymap(str)
Excel_Sparkdf = spark.createDataFrame(df)
Excel_Sparkdf.printSchema()
Excel_Sparkdf.show()

Record_count = Excel_Sparkdf.count()
column_list = list(Excel_Sparkdf.columns)
column_set = set(column_list)

if "xlsx" not in ObjectName:
    print("Error: File format is invalid. Please convert the file to excel (xlsx) and resubmit the file.")
elif Record_count > 90000:
    print("Maximum number of rows exceeded in file submitted. Maximum allowed is 30,000 rows or records per file")
elif Record_count == 0:
    print("Error: Submitted file is empty, please upload a completed file")
elif len(column_list) != len(column_names):
    print("Error: Number of columns are not correct. Please refer to the excel template link above for the correct number of columns and resubmit the file.")
elif column_list != column_names:
    print("Error: Column heading(s) does not match the template for this source. Please refer to the excel template link above for the correct column headers and resubmit the file")
elif len(column_list) != len(column_set):
    print("Error: Column heading(s) has duplicates, it doesn't match template for this source Please refer the excel template link above for correct column headers & resubmit the file")
else:
    postgres()
    







job.commit()