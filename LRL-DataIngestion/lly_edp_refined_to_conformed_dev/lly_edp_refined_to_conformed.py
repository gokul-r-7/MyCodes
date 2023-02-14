"""Module Provides required functions"""
import sys
import json
import base64
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from botocore.exceptions import ClientError
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
secret_manager = boto3.client("secretsmanager")
#Getting Credentials from SecretManager
def get_secrets(secretname):
    """This function gets data from secret manager"""
    try:
        get_secret_value_response = secret_manager.get_secret_value(
            SecretId=secretname
        )
    except ClientError as secret_error:
        raise secret_error
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether secret string or binary one of fields populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret
def get_refined_tables(postgrestablename):
    """This Function reads source transaction table"""
    get_refined_tables_data_df = spark.read \
            .format("jdbc") \
            .option("url", refined_postgresurl) \
            .option("dbtable", postgrestablename) \
            .option("user", refined_username) \
            .option("password", refined_password) \
            .load()
    return get_refined_tables_data_df
def get_lrl_edb_tables(postgrestablename):
    """This Function reads source transaction table"""
    get_lrl_edb_tables_data_df = spark.read \
            .format("jdbc") \
            .option("url", lrl_postgresurl) \
            .option("dbtable", postgrestablename) \
            .option("user", lrl_username) \
            .option("password", lrl_password) \
            .load()
    return get_lrl_edb_tables_data_df
def ingest_refined_and_lrl_data_to_postgres(postgrestablename, source_dataframe):
    """This Function ingestion data to postgresdb"""
    postgres_ingestion = source_dataframe.write.format("jdbc") \
        .option("url", conformed_postgresurl) \
        .option("dbtable", postgrestablename) \
        .option("user", conformed_username) \
        .option("password", conformed_password) \
        .option("truncate", "true") \
        .mode("overwrite").save()
    return postgres_ingestion
#Fetching Credentials from SecretManager
refined_secret_manager_data = get_secrets( "lly_edp_refined_secret")
refined_rdssecrets= json.loads(refined_secret_manager_data)
refined_username = refined_rdssecrets['RDS_USERNAME']
refined_password = refined_rdssecrets['RDS_PASSWORD']
refined_hostname = refined_rdssecrets['RDS_HOST']
refined_schema =  refined_rdssecrets['RDS_SCHEMA']
refined_postgresdbname = refined_rdssecrets['RDS_DBNAME']
refined_postgresurl = "jdbc:postgresql://" + refined_hostname + ":" + \
    str(refined_rdssecrets['RDS_PORT']) + "/" + refined_postgresdbname
lrl_secret_manager_data = get_secrets( "lrl_edb")
lrl_rdssecrets= json.loads(lrl_secret_manager_data)
lrl_username = lrl_rdssecrets['RDS_USERNAME']
lrl_password = lrl_rdssecrets['RDS_PASSWORD']
lrl_hostname = lrl_rdssecrets['RDS_HOST']
lrl_schema = lrl_rdssecrets['RDS_SCHEMA']
lrl_postgresdbname = lrl_rdssecrets['RDS_DBNAME']
lrl_postgresurl = "jdbc:postgresql://" + lrl_hostname + ":" + \
    str(lrl_rdssecrets['RDS_PORT']) + "/" + lrl_postgresdbname
conformed_secret_manager_data = get_secrets("lly_edp_conformed_secret")
conformed_rdssecrets= json.loads(conformed_secret_manager_data)
conformed_username = conformed_rdssecrets['RDS_USERNAME']
conformed_password = conformed_rdssecrets['RDS_PASSWORD']
conformed_hostname = conformed_rdssecrets['RDS_HOST']
conformed_schema = conformed_rdssecrets['RDS_SCHEMA']
conformed_postgresdbname = conformed_rdssecrets['RDS_DBNAME']
conformed_postgresurl = "jdbc:postgresql://" + conformed_hostname + ":" + \
    str(conformed_rdssecrets['RDS_PORT']) + "/" + conformed_postgresdbname
#Checking the FileType in S3 Bucket
def main():
    """This is main function"""
    lrlcoursequeue_df = get_refined_tables( "lrl_edb_refined.lrlcoursequeue")
    lrlcoursequeue_df.show()
    curriculumprefixfunctionmapping_df = get_refined_tables \
        ("lrl_edb_refined.curriculumprefixfunctionmapping")
    curriculumprefixfunctionmapping_df.show()
    curriculum_data_df = get_lrl_edb_tables("lrl_edb.curriculum_data")
    curriculum_data_df.show()
    lrlcoursequeue_ingestion = ingest_refined_and_lrl_data_to_postgres \
        ("lrl_edb_conformed.lrlcoursequeue", lrlcoursequeue_df)
    print(lrlcoursequeue_ingestion)
    curriculumprefixfunctionmapping_ingestion = ingest_refined_and_lrl_data_to_postgres \
    ("lrl_edb_conformed.curriculmprefixfunctionmapping", curriculumprefixfunctionmapping_df)
    print(curriculumprefixfunctionmapping_ingestion)
    curriculum_data_ingestion = ingest_refined_and_lrl_data_to_postgres \
        ("lrl_edb_conformed.curriculum_data", curriculum_data_df)
    print(curriculum_data_ingestion)
main()
job.commit()
