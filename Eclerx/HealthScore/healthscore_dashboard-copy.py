import sys
import os
import json
import boto3
import numpy as np
import pandas as pd
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import logging


def initialize_logging():
    """
    Initialize logging with CloudWatch stream handler for AWS Glue job logging.
    """
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error initializing logging: %s", e)
        raise


def initialize_glue_job():
    """
    Initialize the AWS Glue job and Spark context.
    """
    try:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        return job, spark
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error initializing Glue job: %s", e)
        raise


def read_s3_file(file_path, file_type='csv'):
    """
    Reads a file from S3 into a Pandas DataFrame.
    Supports CSV by default but can be adapted to other formats.
    
    :param file_path: The S3 file path
    :param file_type: The type of the file (csv, json, etc.)
    :return: Pandas DataFrame
    """
    try:
        if file_type == 'csv':
            return spark.read.option("header", "true").csv(file_path).toPandas()
        elif file_type == 'json':
            return s3_client.get_object(Bucket=bucket_name, Key=file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error reading S3 file %s: %s", file_path, e)
        raise


def parse_data(data):
    """
    Parses the input data (JSON format) to extract mappings for features, display names, and metric nature.
    
    :param data: Parsed JSON data
    :return: Mappings for feature groups, feature sequence numbers, display names, metric nature, and metric sequence numbers.
    """
    try:
        feature_groups = {}
        feature_seqno_mapping = {}
        display_name_mapping = {}
        metric_nature_mapping = {}
        metric_sequence_number_mapping = {}

        for feature in data['features']:
            feature_name = feature['feature_name']
            feature_seq_num = feature['feature_seq_num']
            feature_groups[feature_name] = []
            feature_seqno_mapping[feature_name] = feature_seq_num

            for metric in feature['metrics']:
                metric_name = metric['metrics']
                display_name = metric['display_names']
                metric_nature = metric['metric_nature']
                metric_seq_num = metric['metric_sequence_num']
                display_name_mapping[metric_name] = display_name
                metric_nature_mapping[display_name] = metric_nature
                metric_sequence_number_mapping[display_name] = metric_seq_num
                feature_groups[feature_name].append(metric_name)

        return feature_groups, feature_seqno_mapping, display_name_mapping, metric_nature_mapping, metric_sequence_number_mapping
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error parsing data: %s", e)
        raise


def map_display_name(df, display_name_mapping):
    """
    Adds a display name and other relevant columns to the DataFrame.
    
    :param df: The input DataFrame
    :param display_name_mapping: A dictionary mapping metrics to display names
    :return: Updated DataFrame with new columns
    """
    try:
        df['display_names'] = df['Metrics'].map(display_name_mapping)
        return df
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error mapping display names: %s", e)
        raise


def map_metric_nature_and_levels(df, level_metrics):
    """
    Maps the display name to various levels (1 to 5) and adds those as columns.
    
    :param df: The input DataFrame
    :param level_metrics: Dictionary containing level mappings
    :return: Updated DataFrame with level columns
    """
    try:
        for level, metrics in level_metrics.items():
            df[level] = df['display_names'].apply(lambda x: x if x in metrics else None)
        return df
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error mapping metric nature and levels: %s", e)
        raise


def move_row(df, from_idx, to_idx):
    """
    Moves a row within a DataFrame from a source index to a destination index.
    
    :param df: DataFrame
    :param from_idx: The index from which the row should be moved
    :param to_idx: The index to which the row should be moved
    :return: DataFrame with the row moved
    """
    try:
        row = df.iloc[from_idx]
        df = df.drop(from_idx).reset_index(drop=True)
        df = pd.concat([df.iloc[:to_idx], row.to_frame().T, df.iloc[to_idx:]]).reset_index(drop=True)
        return df
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error moving row: %s", e)
        raise


def generate_hierarchy_id(df, grouped_by_columns):
    """
    Generates a hierarchy ID based on a set of grouped columns in the DataFrame.
    
    :param df: Input DataFrame
    :param grouped_by_columns: Columns to group by (e.g., OS type)
    :return: DataFrame with generated hierarchy IDs
    """
    try:
        df['Hierarchy_ID'] = None
        df['Feature_Order'] = df.groupby(grouped_by_columns)['unique_identifier'].transform(lambda x: pd.Series(range(len(x)), index=x.index))

        grouped = df.groupby(grouped_by_columns + ['unique_identifier'], group_keys=False)
        results = []

        for (os_name, unique_id), group in grouped:
            current_ids = {'level_1': 0, 'level_2': 0, 'level_3': 0, 'level_4': 0, 'level_5': 0}

            group = group.sort_values('Feature_Order')

            for index, row in group.iterrows():
                if pd.isna(row['display_names']):
                    continue
                for level in ['level_1', 'level_2', 'level_3', 'level_4', 'level_5']:
                    if not pd.isna(row[level]):
                        current_ids[level] += 1
                        for next_level in ['level_2', 'level_3', 'level_4', 'level_5']:
                            current_ids[next_level] = 0
                        hierarchy_str = ".".join([str(current_ids[l]) for l in ['level_1', 'level_2', 'level_3', 'level_4', 'level_5'] if current_ids[l] > 0])
                        group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{hierarchy_str}"

            results.append(group)

        df = pd.concat(results, ignore_index=True)
        return df
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error generating hierarchy ID: %s", e)
        raise


def assign_parent_id(row):
    """
    Assigns the parent ID based on the hierarchy ID.
    
    :param row: DataFrame row
    :return: Parent ID for the row
    """
    try:
        if pd.isna(row['Hierarchy_ID']):
            return None
        hierarchy_parts = row['Hierarchy_ID'].rsplit('.', 1)
        if len(hierarchy_parts) > 1:
            return hierarchy_parts[0]
        return None
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error assigning parent ID: %s", e)
        raise


def calculate_level_no(row):
    """
    Calculates the level number of a given row based on the hierarchy ID.
    
    :param row: DataFrame row
    :return: Level number for the row
    """
    try:
        if pd.isna(row['Hierarchy_ID']):
            return 1
        return row['Hierarchy_ID'].count('.') + 1
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error calculating level number: %s", e)
        raise


def assign_feature_id_and_seq(df, feature_mapping, feature_seqno_mapping):
    """
    Assigns feature ID and sequence number to the DataFrame based on the feature mappings.
    
    :param df: The DataFrame to process
    :param feature_mapping: A dictionary mapping feature names to IDs
    :param feature_seqno_mapping: A dictionary mapping feature names to sequence numbers
    :return: Updated DataFrame with feature IDs and sequence numbers
    """
    try:
        df['feature_id'] = df['Feature'].map(feature_mapping)
        df['feature_seq_num'] = df['Feature'].map(feature_seqno_mapping)
        return df
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error assigning feature ID and sequence number: %s", e)
        raise


def melt_and_transform_data(df, date_columns):
    """
    Melts the DataFrame and transforms the columns as necessary (e.g., converting dates).
    
    :param df: The DataFrame to melt
    :param date_columns: The date columns to filter and include in the melted DataFrame
    :return: Melted and transformed DataFrame
    """
    try:
        id_vars = ['metric_id', 'create_dt']
        second_table_df_melted = df.melt(id_vars=id_vars, value_vars=date_columns, var_name='Date', value_name='Value')
        second_table_spark_df = spark.createDataFrame(second_table_df_melted)

        second_table_spark_df_transformed = second_table_spark_df \
            .withColumn("metric_id", F.col("metric_id").cast("int")) \
            .withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy")) \
            .withColumn("Value", F.col("Value").cast("int"))

        return second_table_spark_df_transformed
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error melting and transforming data: %s", e)
        raise


def write_to_s3(df, path):
    """
    Writes the DataFrame to S3 in Parquet format.
    
    :param df: DataFrame to write
    :param path: S3 output path
    """
    try:
        df.coalesce(1).write.format("parquet").mode("append").save(path)
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error writing to S3 path %s: %s", path, e)
        raise


# Main Job Execution
if __name__ == "__main__":
    try:
        # Initialize job and context
        job, spark = initialize_glue_job()
        logger = initialize_logging()

        logger.info("Starting Glue job: %s", args['JOB_NAME'])
        job.init(args['JOB_NAME'], args)

        # Read data from S3
        raw_file_path = 's3://cci-dig-aicoe-data-sb/processed/healthscore/healthscore_raw_file/HS_DataClean_Nov.csv'
        json_file_path = 'processed/healthscore/healthscore_json/healthscore_dashboard.json'
        df = read_s3_file(raw_file_path, file_type='csv')

        # Read config JSON
        read_json = read_s3_file(json_file_path, file_type='json')
        config_data = json.loads(read_json['Body'].read().decode('utf-8'))

        # Parse data and extract mappings
        feature_groups, feature_seqno_mapping, display_name_mapping, metric_nature_mapping, metric_sequence_number_mapping = parse_data(config_data)

        # Map display names to DataFrame
        df = map_display_name(df, display_name_mapping)

        # Define level mappings and map to DataFrame
        level_metrics = {
            'level_1': config_data['level_1_metrics'],
            'level_2': config_data['level_2_metrics'],
            'level_3': config_data['level_3_metrics'],
            'level_4': config_data['level_4_metrics'],
            'level_5': config_data['level_5_metrics']
        }
        df = map_metric_nature_and_levels(df, level_metrics)

        # Move rows based on predefined indices
        move_rows_indices = [(50, 46), (51, 47), (52, 48), (53, 49), (164, 158), (165, 159), (180, 174), (181, 175), 
                             (218, 206), (219, 207), (220, 208), (221, 209), (280, 268), (281, 269), (282, 270), (283, 271)]
        for from_idx, to_idx in move_rows_indices:
            df = move_row(df, from_idx, to_idx)

        df['unique_identifier'] = df['Feature'].apply(lambda x: ''.join([word[0].upper() for word in x.split()]))

        # Generate hierarchy IDs based on the feature groups
        df = generate_hierarchy_id(df, ['Operating System Type'])

        # Assign parent ID based on the hierarchy
        df['Parent_ID'] = df.apply(assign_parent_id, axis=1)

        # Calculate level number
        df['level_no'] = df.apply(calculate_level_no, axis=1)

        # Assign feature ID and sequence number
        feature_mapping = {feature_name: idx + 1 for idx, feature_name in enumerate(df['Feature'].unique())}
        df = assign_feature_id_and_seq(df, feature_mapping, feature_seqno_mapping)

        # Write the first table to S3
        first_output_path = "s3://cci-dig-aicoe-data-sb/processed/healthscore/healthscore_first_table/"
        write_to_s3(df, first_output_path)

        # Melt and transform the second table data
        second_table_columns = ['metric_id', 'create_dt', 'metrics', 'operating_system_type']
        second_table_df = df[second_table_columns + [col for col in df.columns if is_date_column(col)]]
        date_columns = second_table_df.columns[4:].tolist()
        second_table_spark_df = melt_and_transform_data(second_table_df, date_columns)

        # Write the second table to S3
        second_output_path = "s3://cci-dig-aicoe-data-sb/processed/healthscore/healthscore_actual_data_test/"
        write_to_s3(second_table_spark_df, second_output_path)

        logger.info("Glue job completed.")
        job.commit()
    
    except Exception as e:
        logger = logging.getLogger()
        logger.error("Error during Glue job execution: %s", e)
        sys.exit(1)









import pandas as pd
import logging
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType

# Logger setup for debugging purposes
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Entering function: calculate_percentage")

def calculate_percentage(df, date_columns, metric_id, parent_id):
    """
    Calculate percentage of date values in the DataFrame relative to the parent_id.

    Args:
    - df (DataFrame): The input DataFrame.
    - date_columns (list): List of date column names to calculate percentages for.
    - metric_id (str): The column name representing the metric ID.
    - parent_id (str): The column name representing the parent ID.

    Returns:
    - DataFrame: Updated DataFrame with calculated percentages for each date column.
    """
    try:
        # Ensure the metric_id and parent_id columns exist in the DataFrame
        if metric_id not in df.columns or parent_id not in df.columns:
            raise ValueError(f"'{metric_id}' or '{parent_id}' columns not found in DataFrame.")
        
        # Create a copy of the DataFrame to avoid modifying the original
        df = df.copy()

        # Iterate through each row and calculate the percentage for each date column
        for idx, row in df.iterrows():
            for date_col in date_columns:
                # Get the numerator (DATE value) and ensure it's numeric
                numerator = pd.to_numeric(row[date_col], errors='coerce')

                # Get the parent hierarchy ID
                parent_id_value = row[parent_id]
                
                # If the parent ID is missing or empty, assume 100% (same value)
                if pd.isna(parent_id_value) or parent_id_value == "":
                    percentage = numerator  # No parent found
                else:
                    # Find the parent's corresponding date value
                    parent_row = df[df[metric_id] == parent_id_value]
                    
                    # If the parent exists, fetch its value, else set denominator to 0
                    if not parent_row.empty:
                        denominator = pd.to_numeric(parent_row[date_col].values[0], errors='coerce')
                    else:
                        denominator = 0
                    
                    # Calculate percentage and handle division by zero
                    percentage = (numerator / denominator * 100) if denominator != 0 else 0
                
                # Round percentage to 3 decimal places
                percentage = round(percentage, 3) if isinstance(percentage, (int, float)) else 0

                # Create the new column for percentage and add it to the DataFrame
                percentage_column_name = f"{date_col} PERCENTAGE"
                if percentage_column_name not in df.columns:
                    df[percentage_column_name] = None  # Initialize if not already present

                df.loc[idx, percentage_column_name] = percentage

        return df

    except Exception as e:
        logger.error(f"Error calculating percentage: {e}")
        raise


# Apply the function to calculate percentage
percentage_df = calculate_percentage(df, date_columns, 'metric_id', 'parent_id')


# Process percentage columns
percentage_columns = [col for col in percentage_df.columns if 'PERCENTAGE' in col]
percent_df = percentage_df[id_columns + percentage_columns]

# Remove the word "PERCENTAGE" from column names
percent_df.columns = [col.replace(" PERCENTAGE", "") if "PERCENTAGE" in col else col for col in percent_df.columns]


logger.info("Entering function: calculate_both")

def calculate_both(df, metric_id, parent_id):
    """
    Generate new rows based on unique display_name and parent-child relationships.

    Args:
    - df (DataFrame): Input DataFrame.
    - metric_id (str): Metric column name.
    - parent_id (str): Parent ID column name.

    Returns:
    - DataFrame: DataFrame with new rows generated based on conditions.
    """
    try:
        new_rows = []
        seq_num = 1  # Sequential ID for metric_id

        # Iterate through unique display_names
        for display_name in df['display_names'].unique():
            display_df = df[df['display_names'] == display_name]
            
            # Handle case where parent_id is null or empty
            if display_df[parent_id].isnull().any() or display_df[parent_id].eq('').any():
                # Combine rows for 'Apple iOS' and 'Google Android'
                ios_rows = display_df[display_df["operating_system_type"] == "Apple iOS"]
                android_rows = display_df[display_df["operating_system_type"] == "Google Android"]
                
                # Sum the date columns for iOS and Android
                numerator = ios_rows[date_columns].apply(pd.to_numeric, errors='coerce') + android_rows[date_columns].apply(pd.to_numeric, errors='coerce')
                
                # Sum the numerator and round to 3 decimal places
                new_row_values = numerator.sum(axis=0).fillna(0).apply(lambda x: round(x, 3)).to_dict()
            else:
                # If parent_id is not null, calculate the percentage
                ios_rows = display_df[(display_df["operating_system_type"] == "Apple iOS") & display_df[parent_id].notna()]
                android_rows = display_df[(display_df["operating_system_type"] == "Google Android") & display_df[parent_id].notna()]

                # Sum the date columns for iOS and Android
                numerator = ios_rows[date_columns].apply(pd.to_numeric, errors='coerce') + android_rows[date_columns].apply(pd.to_numeric, errors='coerce')

                # Get filtered data based on parent_id
                filtered_data = df[df[metric_id].isin(display_df[parent_id])]
                ios_data = filtered_data[filtered_data['operating_system_type'] == "Apple iOS"]
                android_data = filtered_data[filtered_data['operating_system_type'] == "Google Android"]

                # Calculate the denominator
                denominator = ios_data[date_columns].apply(pd.to_numeric, errors='coerce') + android_data[date_columns].apply(pd.to_numeric, errors='coerce')

                # Calculate percentage
                result = (numerator / denominator) * 100 if not denominator.empty else pd.DataFrame(0, columns=date_columns, index=numerator.index)
                result = result.round(3)

                # Sum the results
                new_row_values = result.sum(axis=0).fillna(0).apply(lambda x: round(x, 3)).to_dict()

            # Create a new row for each display_name
            new_row = {
                'display_names': display_name,
                'operating_system_type': 'Both',
                'metric_id': f'100000{seq_num}'
            }

            # Update new row with calculated values
            new_row.update(new_row_values)
            new_rows.append(new_row)
            seq_num += 1

        # Convert new rows to DataFrame and append to original DataFrame
        new_rows_df = pd.DataFrame(new_rows)
        result_df = pd.concat([df, new_rows_df], ignore_index=True)

        return new_rows_df

    except Exception as e:
        logger.error(f"Error calculating both: {e}")
        raise


# Apply function to calculate both
both_calc_df = calculate_both(df, 'metric_id', 'parent_id')


# Prepare for merge operations
unique_display_names_df = df.drop_duplicates(subset='display_names')
id_columns_both = ['create_dt', 'feature_id', 'feature_seq_num', 'metric_sequence_num', 'metric_seqno', 'parent_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names', 'metric_nature']
id_column_both_df = unique_display_names_df[id_columns_both]

# Perform left merge on 'display_names'
both_df = pd.merge(both_calc_df, id_column_both_df, on='display_names', how='inner')

# Combine data frames and prepare for Spark
third_table_df = pd.concat([percent_df, both_df], ignore_index=True)

# Melt DataFrame and convert types
third_table_melted_df = pd.melt(third_table_df, id_vars=id_columns, value_vars=date_columns, var_name='Date', value_name='Value')
third_table_final_df = third_table_melted_df[['create_dt', 'metric_id', 'feature_id', 'feature_name', 'metrics', 'display_names', 'operating_system_type', 'Date', 'Value']]

# Convert 'Value' and 'Date' columns to numeric and datetime types respectively
third_table_final_df['Value'] = pd.to_numeric(third_table_final_df['Value'], errors='coerce')
third_table_final_df['Date'] = pd.to_datetime(third_table_final_df['Date'], format='%d-%m-%Y')
third_table_final_df['create_dt'] = pd.to_datetime(third_table_final_df['create_dt'])

# Convert to Spark DataFrame
third_table_spark_df = spark.createDataFrame(third_table_final_df)

# Convert columns to appropriate types in Spark
third_table_spark_df_transformed = third_table_spark_df \
    .withColumn("create_dt", F.to_date(F.col("create_dt"))) \
    .withColumn("metric_id", F.col("metric_id").cast(IntegerType())) \
    .withColumn("feature_id", F.col("feature_id").cast(IntegerType())) \
    .withColumn("Date", F.to_date(F.col("Date"))) \
    .withColumn("Value", F.col("Value").cast(FloatType()))

# Show the schema of the transformed DataFrame
third_table_spark_df_transformed.printSchema()

# Write data to S3
logger.info("Writing third table data into s3")
third_output_path = "s3://cci-dig-aicoe-data-sb/processed/healthscore/healthscore_percent_both_test/"
third_table_spark_df_transformed.coalesce(1).write.format("parquet").mode("append").save(third_output_path)








import pandas as pd
import numpy as np
from pyspark.sql.types import IntegerType

# Function to safely convert columns to datetime type
def convert_to_datetime(df, columns):
    """Converts columns in the dataframe to datetime, ignoring errors."""
    try:
        for col in columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    except Exception as e:
        print(f"Error converting columns to datetime: {e}")
    return df

# Convert date columns to datetime
date_columns = ['create_dt', 'Date']
third_table_melted_df = convert_to_datetime(third_table_melted_df, date_columns)

# Function to convert columns to appropriate numeric types with exception handling
def convert_to_numeric(df, columns, dtype):
    """Convert specified columns to given numeric dtype."""
    try:
        for col in columns:
            df[col] = df[col].astype(dtype)
    except Exception as e:
        print(f"Error converting columns to {dtype}: {e}")
    return df

# Convert integer columns
integer_columns = ['feature_id', 'feature_seq_num', 'metric_sequence_num', 'level_no', 'parent_id']
third_table_melted_df = convert_to_numeric(third_table_melted_df, integer_columns, 'Int64')

# Convert string columns
string_columns = ['metric_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names', 'operating_system_type', 'metric_nature']
third_table_melted_df = convert_to_numeric(third_table_melted_df, string_columns, str)

# Convert 'Value' to numeric (float)
third_table_melted_df['Value'] = pd.to_numeric(third_table_melted_df['Value'], errors='coerce')

# Calculate 1st and 3rd quartiles and IQR
def calculate_quartiles(df):
    """Calculate 1st and 3rd quartiles, and IQR for 'Value' grouped by 'operating_system_type' and 'display_names'."""
    try:
        quartiles = df.groupby(['operating_system_type', 'display_names'])['Value'].quantile([0.25, 0.75]).unstack()
        quartiles.reset_index(inplace=True)
        quartiles.columns.name = None
        quartiles.columns = ['operating_system_type', 'display_names', '1st Quartile', '3rd Quartile']
        quartiles['IQR'] = quartiles['3rd Quartile'] - quartiles['1st Quartile']
        return quartiles
    except Exception as e:
        print(f"Error calculating quartiles: {e}")
        return pd.DataFrame()

quartiles = calculate_quartiles(third_table_melted_df)

# Merge quartiles with original data
quartiles_iqr_df = pd.merge(third_table_df, quartiles, on=['display_names', 'operating_system_type'], how='outer')

# Function to calculate upper and lower bounds based on IQR
def calculate_upper_lower(df, parent_column='parent_id'):
    """
    Calculate upper and lower bounds based on IQR for each row in the dataframe. 
    Adjust the calculation based on whether the 'parent_column' is empty or not.
    """
    try:
        if parent_column not in df.columns:
            raise ValueError(f"{parent_column} not found in the dataframe")
        
        def calculate_row(row):
            if pd.isna(row[parent_column]) or row[parent_column] == '':
                upper = row['3rd Quartile'] + (1.25 * row['IQR'])
                lower = row['1st Quartile'] - (1.25 * row['IQR'])
            else:
                upper = row['3rd Quartile'] + (0.75 * row['IQR'])
                lower = row['1st Quartile'] - (0.75 * row['IQR'])
            return pd.Series({'Upper': upper, 'Lower': lower})
        
        # Apply row-wise function to calculate upper and lower bounds
        df[['Upper', 'Lower']] = df.apply(calculate_row, axis=1)
        return df
    except Exception as e:
        print(f"Error calculating upper and lower bounds: {e}")
        return df

upper_lower_df = calculate_upper_lower(quartiles_iqr_df)

# Function to calculate the mean for the last 7 and 30 days
def calculate_last7_and_30_days(df):
    """Calculate average values for the last 7 and 30 days, and calculate percentage changes."""
    try:
        # Extract date columns
        date_columns = [col for col in df.columns if '-' in col]
        last_7_columns = date_columns[-8:-1]
        last_30_columns = date_columns[-31:-1]

        # Extract date objects
        date_objects = [pd.to_datetime(col.split()[0], format='%d-%m-%Y') for col in date_columns]

        # Identify latest date
        latest_date = max(date_objects)
        latest_date_column = date_columns[date_objects.index(latest_date)]

        # Create 'Yesterday' column
        df['Yesterday'] = df[latest_date_column]

        # Ensure numeric columns for mean calculation
        df[last_7_columns] = df[last_7_columns].apply(pd.to_numeric, errors='coerce')
        df[last_30_columns] = df[last_30_columns].apply(pd.to_numeric, errors='coerce')

        # Calculate averages for last 7 and 30 days
        df['last_7_days'] = df[last_7_columns].mean(axis=1)
        df['last_30_days'] = df[last_30_columns].mean(axis=1)

        # Handle 0 and NaN values
        df['last_7_days'] = df['last_7_days'].replace(0, np.nan)
        df['last_30_days'] = df['last_30_days'].replace(0, np.nan)

        # Calculate percentage changes
        df['% Change Last 7 Days'] = np.where(df['last_7_days'].isna(), 0, (df['Yesterday'] - df['last_7_days']) / df['last_7_days'] * 100)
        df['% Change Last 30 Days'] = np.where(df['last_30_days'].isna(), 0, (df['Yesterday'] - df['last_30_days']) / df['last_30_days'] * 100)

        # Round percentage changes
        df['% Change Last 7 Days'] = df['% Change Last 7 Days'].round(1)
        df['% Change Last 30 Days'] = df['% Change Last 30 Days'].round(1)

        return df
    except Exception as e:
        print(f"Error calculating last 7 and 30 days: {e}")
        return df

last_7_and_30_days_df = calculate_last7_and_30_days(upper_lower_df)

# Round relevant columns
columns_to_round = ['1st Quartile', '3rd Quartile', 'IQR', 'Upper', 'Lower', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']
last_7_and_30_days_df[columns_to_round] = last_7_and_30_days_df[columns_to_round].round(3)

# Function to assign colour indicators
def add_colour_indicators(df):
    """Assign colour indicators based on metric nature and calculated bounds."""
    try:
        # Assigning metric_nature_indicator
        def assign_colour_indicator(row):
            if row['metric_nature'] == 'Positive':
                if row['Yesterday'] > row['Upper']:
                    return 'Positive Above'
                elif row['Yesterday'] < row['Lower']:
                    return 'Positive Below'
                else:
                    return 'Positive Within'
            elif row['metric_nature'] == 'Negative':
                if row['Yesterday'] > row['Upper']:
                    return 'Negative Above'
                elif row['Yesterday'] < row['Lower']:
                    return 'Negative Below'
                else:
                    return 'Negative Within'
            return None

        # Assigning colour_indicator based on metric_nature_indicator
        def assign_colour_indicator1(row):
            if row['metric_nature_indicator'] in ['Positive Above', 'Negative Below']:
                return 'Green'
            elif row['metric_nature_indicator'] in ['Positive Below', 'Negative Above']:
                return 'Red'
            elif row['metric_nature_indicator'] in ['Positive Within', 'Negative Within']:
                return 'White'
            return None

        df['metric_nature_indicator'] = df.apply(assign_colour_indicator, axis=1)
        df['colour_indicator'] = df.apply(assign_colour_indicator1, axis=1)

        return df
    except Exception as e:
        print(f"Error assigning colour indicators: {e}")
        return df

colour_indicators_df = add_colour_indicators(last_7_and_30_days_df)

# Convert relevant columns to numeric type
colour_indicators_df['Yesterday'] = pd.to_numeric(colour_indicators_df['Yesterday'], errors='coerce')
colour_indicators_df['% Change Last 7 Days'] = pd.to_numeric(colour_indicators_df['% Change Last 7 Days'], errors='coerce')
colour_indicators_df['% Change Last 30 Days'] = pd.to_numeric(colour_indicators_df['% Change Last 30 Days'], errors='coerce')

# Create Spark dataframe and adjust types
fourth_table_spark_df = spark.createDataFrame(colour_indicators_df)
fourth_table_spark_df_transformed = fourth_table_spark_df.withColumn("metric_id", fourth_table_spark_df["metric_id"].cast(IntegerType()))

# Writing the data to S3
try:
    logger.info("Writing fourth table data to s3")
    fourth_output_path = "s3://cci-dig-aicoe-data-sb/processed/healthscore/healthscore_calculations_test/"
    fourth_df_table_write_df = fourth_table_spark_df_transformed.coalesce(1).write.format("parquet").mode("append").save(fourth_output_path)
except Exception as e:
    print(f"Error writing data to S3: {e}")

# Commit the job
job.commit()
