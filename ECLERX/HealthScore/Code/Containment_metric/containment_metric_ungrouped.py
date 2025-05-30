"""
AWS Glue PySpark Job for Containment Metric

This script reads containment metric data from an S3 bucket, processes it using PySpark,
and writes the transformed data back to S3. It uses AWS Glue for ETL and logs
processing steps to AWS CloudWatch.
"""



import sys
import os
import json
import functools
import boto3
import numpy as np
import pandas as pd
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import IntegerType, FloatType, DateType
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a CloudWatch log handler
handler = logging.StreamHandler(sys.stdout)  # Use stdout for CloudWatch
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# -------- AWS Glue and Spark Initialization -------- #
# Set up Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
glueContext._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions

job = Job(glueContext)

# Start the job
logger.info("Starting Glue job: %s", args['JOB_NAME'])
job.init(args['JOB_NAME'], args)


# -------- AWS S3 Configuration -------- #
# Initialize the S3 client for data access
s3_client = boto3.client('s3')


# Define your S3 bucket name and the file path within the bucket
bucket_name = 'cci-dig-aicoe-data-sb'
file_key = 'processed/containment_metric/containment_metric_config_json/containment_metric.json'






# Read the JSON file from S3
logger.info("Reading config json file")
read_json = s3_client.get_object(Bucket=bucket_name, Key=file_key)

# Load the file content into the config_data variable
config_data = json.loads(read_json['Body'].read().decode('utf-8'))


logger.info("Entering function: create_dataframe_from_json")   
# Function to parse the nested dictionary
def create_dataframe_from_json(data):
    # Flatten the JSON and create a list of dictionaries
    flattened_data = []

    for feature in data["features"]:
        feature_name = feature["feature_name"]
        feature_id = feature["feature_id"]

        for metric in feature["metrics"]:
            flattened_data.append({
                "hs_feature_name": feature_name,
                "hs_feature_id": feature_id,
                "primary_intent": metric["Primary_intent"],
                "primary_intent_detail": metric["Primary_intent_detail"],
                "cont_display_metric_name": metric["cont_display_metric_name"],
                "cont_display_metric_seq": metric["cont_display_metric_seq"]
            })

    # Create the DataFrame
    df = pd.DataFrame(flattened_data)

    # Add the 'Containment_metric_id' column as a sequence number
    df["id"] = range(1, len(df) + 1)
    df['create_dt'] = pd.to_datetime('today').normalize().date()

    # Reorder the columns if needed
    df = df[["id", "hs_feature_name", "hs_feature_id", "primary_intent", "primary_intent_detail", "cont_display_metric_name", "cont_display_metric_seq", "create_dt"]]

    return df

# Call the function with JSON data
df = create_dataframe_from_json(config_data)

first_table_spark_df = spark.createDataFrame(df)
first_table_spark_df.show()

# Cast the columns to integer
first_table_spark_df_transformed = first_table_spark_df.withColumn("id", col("id").cast("int")) \
       .withColumn("hs_feature_id", col("hs_feature_id").cast("int")) \
       .withColumn("cont_display_metric_seq", col("cont_display_metric_seq").cast("int"))

# Show the schema to verify the changes
first_table_spark_df_transformed.printSchema()

logger.info("Writing First table data into s3 path") 
first_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_master_test/"
firsttable_writedf = first_table_spark_df_transformed.coalesce(1).write.format("parquet").mode("append").save(first_output_path)

# Get unique values from the columns
primary_intent_values = df['primary_intent'].unique()
primary_intent_detail_values = df['primary_intent_detail'].unique()

# Convert arrays to strings formatted for SQL IN clauses
primary_intent_str = "', '".join(primary_intent_values)
primary_intent_detail_str = "', '".join(primary_intent_detail_values)

second_table_query = f"""
SELECT 
    primary_intent, primary_intent_detail,initial_channel,lob,  
    CAST(contact_dt AS DATE) AS contact_dt, 
    COUNT(DISTINCT sub_contact_id) AS sub_contact_id, 
    COUNT(DISTINCT CASE WHEN selfservice_containment = 1 THEN sub_contact_id END) AS selfservice_containment,
    CASE 
        WHEN COUNT(DISTINCT sub_contact_id) > 0 THEN
            ROUND(CAST(SUM(CASE WHEN selfservice_containment = 1 THEN 1 ELSE 0 END) AS DOUBLE) 
            / COUNT(DISTINCT sub_contact_id) * 100, 2)
        ELSE
            0
    END AS containment_rate
FROM 
    ota_data_assets_temp.omni_intent_cntct_fact 
WHERE 
    CAST(contact_dt AS DATE) BETWEEN 
        date_add((SELECT max(CAST(contact_dt AS DATE)) FROM ota_data_assets_temp.omni_intent_cntct_fact), -60) 
        AND (SELECT max(CAST(contact_dt AS DATE)) FROM ota_data_assets_temp.omni_intent_cntct_fact)
    AND primary_intent IN ('{primary_intent_str}')
    AND initial_channel = 'CoxApp'
    AND lob = 'R'
    AND primary_intent_detail IN ('{primary_intent_detail_str}')
GROUP BY 
    primary_intent, primary_intent_detail, contact_dt,initial_channel,lob
ORDER BY 
    contact_dt DESC
"""
logger.info("Reading Second table data frpm the query") 
second_table_df = spark.sql(second_table_query)

# Assuming you have a Spark DataFrame called spark_df
second_table_pandas_df = second_table_df.toPandas()

# Merge the two dataframes on 'primary_intent_detail' using a left join
combined_df = pd.merge(second_table_pandas_df, df, on='primary_intent_detail', how='left')

second_table_columns = ['id', 'primary_intent_x', 'primary_intent_detail', 'sub_contact_id', 'selfservice_containment', 'initial_channel', 'lob', 'contact_dt', 'containment_rate', 'hs_feature_name', 'cont_display_metric_name','create_dt']
second_tabledf = combined_df[second_table_columns]


second_table_spark_df = spark.createDataFrame(second_tabledf)
second_table_spark_df.show()

# Change column types and rename columns
second_table_spark_df_transformed = second_table_spark_df \
    .withColumn("id", col("id").cast("int")) \
    .withColumn("sub_contact_id", col("sub_contact_id").cast("int")) \
    .withColumn("selfservice_containment", col("selfservice_containment").cast("int")) \
    .withColumnRenamed("primary_intent_x", "primary_intent")

# Show the transformed dataframe
second_table_spark_df_transformed.printSchema()

logger.info("Writing Second table data into s3 path") 
second_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_data_test/"
secondtable_writedf = second_table_spark_df_transformed.coalesce(1).write.format("parquet").mode("append").save(second_output_path)

third_table_columns = ['id', 'primary_intent_x', 'primary_intent_detail', 'contact_dt', 'containment_rate']
third_tabledf = second_tabledf[third_table_columns]

# Pivot the data
df_pivot = third_tabledf.pivot_table(index=['id', 'primary_intent_x', 'primary_intent_detail'],
                          columns='contact_dt',
                          values='containment_rate',
                          aggfunc='first').reset_index()



logger.info("Entering function: calculate_last7_and_30_days") 
def calculate_last7_and_30_days(df):
    # Check the actual columns in the dataframe to ensure we are extracting date columns correctly
    print("Columns in df:", df.columns)

    # Try to detect the date columns using the column names
    datecolumn = []
    for col in df.columns:
        try:
            # Try converting the column name to a date if it's a string
            pd.to_datetime(col, format='%Y-%m-%d', errors='raise')
            datecolumn.append(col)
        except (ValueError, TypeError):
            # Ignore columns that cannot be converted to datetime
            continue
    
    print("Date columns detected:", datecolumn)

    if not datecolumn:
        raise ValueError("No valid date columns found in the dataframe.")

    # Exclude the latest date column (the first one)
    last_7_columns = datecolumn[-8:-1]  # Get the last 7 columns excluding the latest date
    last_30_columns = datecolumn[-31:-1]
    print("Last 7 columns:", last_7_columns)
    print("Last 30 columns:", last_30_columns)

    # Convert the date columns to datetime objects
    date_objects = [pd.to_datetime(col, format='%Y-%m-%d') for col in datecolumn]
    print("Date objects:", date_objects)

    # Get the column corresponding to the latest date
    latest_date = max(date_objects)
    latest_date_column = datecolumn[date_objects.index(latest_date)]

    # Create a new column 'Yesterday' with the values from the latest date column
    df.loc[:, 'Yesterday'] = df[latest_date_column]
    
    # Ensure numeric columns before performing mean calculation
    df[last_7_columns] = df[last_7_columns].apply(pd.to_numeric, errors='coerce')
    df[last_30_columns] = df[last_30_columns].apply(pd.to_numeric, errors='coerce')

    # Calculate the mean for each row across the last 7 and 30 date columns
    df.loc[:, 'last_7_days'] = df[last_7_columns].mean(axis=1)
    df.loc[:, 'last_30_days'] = df[last_30_columns].mean(axis=1)

    # Replace 0 and NaN values in 'last_7_days' and 'last_30_days' with NaN to avoid division by zero
    df['last_7_days'] = df['last_7_days'].replace(0, np.nan)
    df['last_30_days'] = df['last_30_days'].replace(0, np.nan)

    # Calculate the percentage change for 'Last 7 Days' with a check for NaN
    df.loc[:, '% Change Last 7 Days'] = np.where(
        df['last_7_days'].isna(), 0, 
        (df['Yesterday'] - df['last_7_days']) / df['last_7_days'] * 100
    )

    # Calculate the percentage change for 'Last 30 Days' with a check for NaN
    df.loc[:, '% Change Last 30 Days'] = np.where(
        df['last_30_days'].isna(), 0, 
        (df['Yesterday'] - df['last_30_days']) / df['last_30_days'] * 100
    )

    # Round the percentage changes to 1 decimal place
    df.loc[:, '% Change Last 7 Days'] = df['% Change Last 7 Days'].round(1)
    df.loc[:, '% Change Last 30 Days'] = df['% Change Last 30 Days'].round(1)
    
    return df
    
last_7_and_30_days_df = calculate_last7_and_30_days(df_pivot)
 

last_7_and_30_days_df = last_7_and_30_days_df[['id', 'primary_intent_x', 'primary_intent_detail', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']]

result_df = pd.merge(df, last_7_and_30_days_df, on="id", how="inner")

result_df = result_df[['id', 'hs_feature_name', 'cont_display_metric_name', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days', 'create_dt']]

result_df[[ 'last_7_days', 'last_30_days']] = result_df[[ 'last_7_days', 'last_30_days']].round(3)

third_table_spark_df = spark.createDataFrame(result_df)
third_table_spark_df.show()


# Change the data type of 'containment_metric_id' from long to integer
third_table_spark_df = third_table_spark_df.withColumn(
    "id", col("id").cast("int")
)

# Show the schema to confirm the change
third_table_spark_df.printSchema()

logger.info("writing fourth table data into s3 path")
third_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_calculation_test/"
third_df_table_write_df = third_table_spark_df.coalesce(1).write.format("parquet").mode("append").save(third_output_path)
job.commit()






job.commit()