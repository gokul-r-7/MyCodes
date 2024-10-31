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

sql_query = 'SELECT * FROM "AwsDataCatalog"."sample-datebase"."test_folder_1"'

athena_table_dataframe = (
    glueContext.read.format("jdbc")
    .option("driver", "com.simba.athena.jdbc.Driver")
    .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
    .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
    .option("dbtable", f"({sql_query})")
    .option("S3OutputLocation","s3://gokul-test-bucket-07/demo-test/")
    .load()
    )

athena_table_dataframe.printSchema()
athena_table_dataframe.show()
athena_table_datasource = DynamicFrame.fromDF(athena_table_dataframe, glueContext, "athena_table_source")

# Specify S3 output path
output_path = "s3://gokul-test-bucket-07/final-target-folder/"

# Write the DynamicFrame to S3 as partitioned CSV files
csv_output = glueContext.write_dynamic_frame.from_options(
    frame=athena_table_datasource,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["index"]
    },
    format="csv",
    format_options={
        "separator": ",",
        "quoteChar": "\"",
        "withHeader": True,
        "escapeChar": "\\" 
    }
)


job.commit()