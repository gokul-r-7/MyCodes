import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#SQL Queries
sample_query = 'SELECT * FROM "AwsDataCatalog"."sample-datebase"."test_folder_1"'
account_dim_sum = ""
account_dim_sum = ""
transaction_adobe_fact = ""
transaction_okta_user_agg_fact = ""
transcation_okta_day_agg = ""

# Athena Query Results Temporary files s3 path
sample_temp_path = "s3://gokul-test-bucket-07/temporary_files/"
account_dim_sum_temp = ""
account_dim_sum_temp = ""
transaction_adobe_fact_temp = ""
transaction_okta_user_agg_fact_temp = ""
transcation_okta_day_agg_temp = ""

#Output s3 path
sample_output_path = "s3://gokul-test-bucket-07/output_files/"
account_dim_sum_output = ""
account_dim_sum_output = ""
transaction_adobe_fact_output = ""
transaction_okta_user_agg_fact_output = ""
transcation_okta_day_agg_output = ""

#PartitionKeys for the target files
sample_partition_keys = ["index"]
account_dim_sum_partitionkeys = []
account_dim_sum_partitionkeys = []
transaction_adobe_fact_partitionkeys = []
transaction_okta_user_agg_fact_partitionkeys = []
transcation_okta_day_agg_partitionkeys = []

def read_athena_tables(sql_query,temp_path):
    read_df = (
    glueContext.read.format("jdbc")
    .option("driver", "com.simba.athena.jdbc.Driver")
    .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
    .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
    .option("dbtable", f"({sql_query})")
    .option("S3OutputLocation",temp_path)
    .load()
    )
    return read_df
    
def write_csv_files_into_s3(df,output_path,partitionkey):
    write_df = DynamicFrame.fromDF(df, glueContext, "athena_table_source")
    csv_output = glueContext.write_dynamic_frame.from_options(
    frame=write_df,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": partitionkey
    },
    format="csv",
    format_options={
        "separator": ",",
        "quoteChar": "\"",
        "withHeader": True,
        "escapeChar": "\\" 
    }
    )
    return csv_output


sample_read_df = read_athena_tables(sample_query,sample_temp_path)
sample_read_df.printSchema()
sample_read_df.show()
sample_read_df.count()

sample_write_df = write_csv_files_into_s3(sample_read_df,sample_output_path,sample_partition_keys)
sample_write_df.printSchema()
sample_write_df.show()
sample_write_df.count()


job.commit()
