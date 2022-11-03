import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
import pandas as pd

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
#S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
#    format_options={},
#    connection_type="s3",
#    format="parquet",
#    connection_options={
#        "paths": ["s3://lambda-layers-g/Test folder/samplefile.parquet"],
#        "recurse": True,
#    },
#    transformation_ctx="S3bucket_node1",
#)

path = "s3://lambda-layers-g/Test folder/samplefile.parquet"
#s3node = S3bucket_node1.toDF()
pandas_df = pd.read_parquet(path, engine = 'pyarrow')
df=pandas_df.applymap(str)
s3node = spark.createDataFrame(df)
s3node.show()
recordcount = s3node.count()
datatypes = s3node.dtypes

print("Record Count = ", recordcount)
print("Data types =", datatypes)
newdf = s3node.select([col(i).cast("string") for i in s3node.columns])
newdf.show()
newdatatypes = newdf.dtypes
print("newdatatypes = ", newdatatypes)

def ingest_data_in_postgres():
    postgres_node = DynamicFrame.fromDF(newdf, glueContext, "dynamicdf")
    write_postgre_options = {
                            "url": postgresurl,
        #                    "database": postgresdbname,
                            "dbtable": postgrestablename,
                            "user": username,
                            "password": password
                        }
    postgres = glueContext.write_dynamic_frame.from_options(postgres_node, connection_type = "postgresql", connection_options=write_postgre_options,transformation_ctx = "postgres")
postgresurl = "jdbc:postgresql://postgresql.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:5432/postgres"
username = "postgres_admin"
password = "password"
postgrestablename = "inventorytable"
ingest_data_in_postgres()


job.commit()

job.commit()
