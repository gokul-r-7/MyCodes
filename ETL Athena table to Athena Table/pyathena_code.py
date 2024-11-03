import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from pyathena import connect

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_athena_results = "s3://gokul-test-bucket-07/temporary_path/"
conn = connect(
    s3_staging_dir = s3_athena_results,
    region_name = "us-east-1"
)
query = 'SELECT * FROM "sample-datebase"."test_folder_1" limit 10'
df = pd.read_sql(query, conn)
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100) 
df.head()
print(df)
print(df.head())
print(df.columns)
job.commit()
