

import sys
import os
import json
import boto3
import logging
import functools
import numpy as np
import pandas as pd
from datetime import datetime
from awsglue.transforms import *
from pyspark.storagelevel import StorageLevel
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, DateType
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
 
glueContext._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
 
spark = glueContext.spark_session
spark.catalog.clearCache()
job = Job(glueContext)
# -------- AWS S3 Configuration -------- #
# Initialize the S3 client for data access
s3_client = boto3.client('s3')
# Define your S3 bucket name and the file path within the bucket
bucket_name = 'cci-dig-aicoe-data-sb'
file_key = 'processed/containment_metric/containment_metric_config_json/containment_metric.json'
read_json = s3_client.get_object(Bucket=bucket_name, Key=file_key)

# Load the file content into the config_data variable
config_data = json.loads(read_json['Body'].read().decode('utf-8'))
print(config_data)
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

# Print the DataFrame
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 500)
print(df.head())
first_table_spark_df = spark.createDataFrame(df)
first_table_spark_df.show()
first_table_spark_df.printSchema()
first_table_spark_df.count()
from pyspark.sql.functions import col

# Cast the columns to integer
first_table_spark_df_transformed = first_table_spark_df.withColumn("id", col("id").cast("int")) \
       .withColumn("hs_feature_id", col("hs_feature_id").cast("int")) \
       .withColumn("cont_display_metric_seq", col("cont_display_metric_seq").cast("int"))

# Show the schema to verify the changes
first_table_spark_df_transformed.printSchema()

#first_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_master_sampletest/"
#firsttable_writedf = first_table_spark_df_transformed.coalesce(1).write.format("parquet").mode("overwrite").save(first_output_path)
omni_raw_table_query = f"""
    SELECT 
        primary_intent,
        primary_intent_detail,
        sub_contact_id,	
        contact_id,
        selfservice_containment,
        initial_channel,
        lob,
        contact_dt 
    FROM 
        ota_data_assets_temp.omni_intent_cntct_fact 
    WHERE 
        CAST(contact_dt AS DATE) > date_add((SELECT max(CAST(contact_dt AS DATE)) FROM ota_data_assets_temp.omni_intent_cntct_fact), -90)
        AND initial_channel = 'CoxApp'
        AND lob = 'R'
    """
# Execute the query (assuming using Spark SQL)
omni_raw_table_df = spark.sql(omni_raw_table_query)
from pyspark.sql.functions import lit, current_date

# Add the "create_dt" column with today's date
omni_raw_table_df = omni_raw_table_df.withColumn("create_dt", current_date())

# Add the "is_active" column with True
omni_raw_table_df = omni_raw_table_df.withColumn("is_active", lit(True))
from pyspark.sql.functions import to_date

# Assuming df is your DataFrame
omni_raw_table_df = omni_raw_table_df.withColumn('contact_dt', to_date(omni_raw_table_df['contact_dt'], 'yyyy-MM-dd'))
omni_raw_table_df.printSchema()
omni_raw_table_df.count()
#omni_raw_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_raw_data/"
#omnitable_writedf = omni_raw_table_df.coalesce(1).write.format("parquet").mode("append").save(omni_raw_output_path)
omni_raw_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_raw_data_sample_test/"

# Writing the data partitioned by 'contant_dt' column
omnitable_writedf = omni_raw_table_df.coalesce(1).write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("contact_dt") \
    .save(omni_raw_output_path)
from pyspark.sql.functions import lit  # Import the lit function
# Register the omni_raw_table_df as a temporary view
omni_raw_table_df.createOrReplaceTempView("omni_raw_table_view")
# Create an empty list to store the result DataFrames
result_dfs = []

# Loop over each unique cont_display_metric_name in the DataFrame
for metric_name in df['cont_display_metric_name'].unique():
    # Filter the DataFrame to get the corresponding primary_intent and primary_intent_detail values
    filtered_df = df[df['cont_display_metric_name'] == metric_name]

    
    # Get unique primary_intent and primary_intent_detail for the current cont_display_metric_name
    primary_intent_values = filtered_df['primary_intent'].unique()
    primary_intent_detail_values = filtered_df['primary_intent_detail'].unique()
    
    # Convert arrays to strings formatted for SQL IN clauses
    primary_intent_str = "', '".join(primary_intent_values)
    primary_intent_detail_str = "', '".join(primary_intent_detail_values)
    
    # Create the query
    second_table_query = f"""
    SELECT 
        initial_channel, lob,  
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
        omni_raw_table_view 
    WHERE 
        CAST(contact_dt AS DATE) > date_add((SELECT max(CAST(contact_dt AS DATE)) FROM omni_raw_table_view), -90)
        AND primary_intent IN ('{primary_intent_str}')
        AND initial_channel = 'CoxApp'
        AND lob = 'R'
        AND primary_intent_detail IN ('{primary_intent_detail_str}')
    GROUP BY 
        contact_dt, initial_channel, lob
    ORDER BY 
        contact_dt DESC
    """
    
    # Execute the query (assuming using Spark SQL)
    second_table_df = spark.sql(second_table_query)
    
    # Add the cont_display_metric_name column to the second_table_df using withColumn
    second_table_df = second_table_df.withColumn("cont_display_metric_name", 
                                                 lit(metric_name))
    
    # Append the result DataFrame to the list
    result_dfs.append(second_table_df.toPandas())

# Combine all the DataFrames into a single DataFrame
final_df = pd.concat(result_dfs, ignore_index=True)

# Display the final result
final_df.head()

final_df.shape
first_table_unique_df = df[[ 'hs_feature_name', 'cont_display_metric_name', 'create_dt']].drop_duplicates(subset = 'cont_display_metric_name')
first_table_unique_df.head()
first_table_unique_df.shape
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 300)      # Show 100 rows
first_table_unique_df.head(20)
# Merge the two dataframes on 'primary_intent_detail' using a left join
combined_df = pd.merge(final_df, first_table_unique_df, on='cont_display_metric_name', how='left')
combined_df.head()
combined_df.shape
combined_df.columns
second_table_columns = ['sub_contact_id', 'selfservice_containment', 'initial_channel', 'lob', 'contact_dt', 'containment_rate', 'hs_feature_name', 'cont_display_metric_name','create_dt']
second_tabledf = combined_df[second_table_columns]
second_tabledf.head()
second_table_spark_df = spark.createDataFrame(second_tabledf)
second_table_spark_df.show()
second_table_spark_df.printSchema()
second_table_spark_df.count()
from pyspark.sql import functions as F

# Assuming second_table_spark_df is your DataFrame
second_table_transformed_df = second_table_spark_df \
    .withColumn("sub_contact_id", F.col("sub_contact_id").cast("int")) \
    .withColumn("selfservice_containment", F.col("selfservice_containment").cast("int")) 
    #.withColumn("create_dt", F.to_date(F.col("create_dt"), "yyyy-MM-dd HH:mm:ss"))

# Show the transformed DataFrame
second_table_transformed_df.show()

second_table_transformed_df.printSchema()
second_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_data_sample_test/"
secondtable_writedf = second_table_transformed_df.coalesce(1).write.format("parquet").mode("overwrite").save(second_output_path)
third_table_columns = ['cont_display_metric_name', 'contact_dt', 'containment_rate']
third_tabledf = second_tabledf[third_table_columns]
third_tabledf.head()
# Pivot the data
df_pivot = third_tabledf.pivot_table(index=['cont_display_metric_name'],
                          columns='contact_dt',
                          values='containment_rate',
                          aggfunc='first').reset_index()

# Display the result
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 200)      # Show 100 rows
df_pivot.head()
print(len(df_pivot.columns))
df_pivot.shape
df_pivot.dtypes
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 300)      # Show 100 rows
df_pivot.head(20)
import numpy as np
import pandas as pd

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
print(last_7_and_30_days_df)
last_7_and_30_days_df.head()
last_7_and_30_days_df = last_7_and_30_days_df[['cont_display_metric_name', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']]
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)
last_7_and_30_days_df.head()
last_7_and_30_days_df.shape
first_table_unique_df.shape
result_df = pd.merge(last_7_and_30_days_df, first_table_unique_df, on="cont_display_metric_name", how="left")
result_df.head()
result_df.shape
result_df.head(20)
result_df = result_df[['hs_feature_name', 'cont_display_metric_name', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days', 'create_dt']]
result_df.head()
result_df[[ 'last_7_days', 'last_30_days']] = result_df[[ 'last_7_days', 'last_30_days']].round(3)
result_df.head()
third_table_spark_df = spark.createDataFrame(result_df)
third_table_spark_df.show()
third_table_spark_df.printSchema()
third_table_spark_df.count()
third_table_spark_df.printSchema()
third_output_path = "s3://cci-dig-aicoe-data-sb/processed/containment_metric/containment_metric_calculation_sample_test/"
third_df_table_write_df = third_table_spark_df.coalesce(1).write.format("parquet").mode("overwrite").save(third_output_path)
job.commit()