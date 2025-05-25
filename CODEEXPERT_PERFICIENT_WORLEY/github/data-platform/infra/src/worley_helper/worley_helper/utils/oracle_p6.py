import requests
import time
from pyspark.sql.functions import lit
from worley_helper.utils.helpers import flatten_nested_attributes
from worley_helper.utils.logger import get_logger
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from worley_helper.utils.helpers import standardize_columns


logger = get_logger(__name__)



def transform_and_store_in_parquet(
    glueContext: GlueContext,
    s3_input_path: str,
    s3_output_path: str,
    s3_schema_location: str,
    sampling_fraction,
    sampling_seed, 
    project_id
) -> None:
    """
    Function to process JSON data and write it to S3 in Parquet format.

    Args:
        glueContext (GlueContext): The Glue context object.
        s3_input_path (str): The S3 path where the raw JSON data is stored.
        s3_output_path (str): The S3 path where the processed Parquet data will be stored.
        s3_schema_location (str): The S3 path where the sample Parquet data will be stored for schema change detection.

    Raises:
        Exception: If an error occurs during the data processing and storage.
    """
    try:
        logger.info(
            f"Reading raw JSON from {s3_input_path} before converting to glueparquet"
        )
        # Read data from S3
        json_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_input_path]},
            format="json",
            format_options={
                "jsonPath": "$[*]"
            },  # Specify the path within the JSON structure
            transformation_ctx="json_dynamic_frame",
        )

        current_columns = [field.name for field in json_dynamic_frame.schema()]
        # Rename the columns
        renamed_columns = [standardize_columns(col) for col in current_columns]
        logger.info(f"Renamed columns {renamed_columns}")

        # Create a new DataFrame with renamed columns
        renamed_df = json_dynamic_frame.toDF().select(
            [
                col("`{}`".format(c)).alias(renamed_columns[i])
                for i, c in enumerate(current_columns)
            ]
        )
        
        renamed_df = renamed_df.withColumn("projectid", lit(project_id))

        # Flatten nested attributes
        data_df = flatten_nested_attributes(renamed_df, "Period")

        # Write the DataFrame to S3 in Parquet format
        logger.info(f"Writing Parquet data to {s3_output_path}")
        data_df.write.mode("append").parquet(s3_output_path)

        logger.info(f"Data written to {s3_output_path} in Parquet format")

        # Sample some data and store it in S3, for schema change detection
        sample_data = data_df.sample(withReplacement=False, fraction=sampling_fraction, seed=sampling_seed)
        sample_data = sample_data.repartition(1)
        sample_data.write.mode("append").parquet(s3_schema_location)

    except Exception as e:
        logger.error(f"An error occurred in transform_and_store_in_parquet: {e}")


def get_spread_ids(spark, glue_context, s3_bucket, s3_prefix, id_column_name):
    # Split the column names into list
    id_cols=id_column_name.split(',')

    # Read the Parquet file from S3 using GlueContext
    logger.info(f"Reading Parquet data from s3://{s3_bucket}/{s3_prefix}")
    parquet_data_source = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{s3_bucket}/{s3_prefix}/"],
            "recurse": True,
        },
        format="parquet",
        transformation_ctx=f"{id_cols[0]}_parquet_data_source",
    )

    # Convert the DynamicFrame to a Spark DataFrame
    parquet_df = parquet_data_source.toDF()

    # Filter condition to get only project related records
    filter_cond=f"{id_cols[1]}='{project_id}'"

    # Select only the specified ID column
    object_id_df = parquet_df.select(col(f"`{id_column_name}`")).where(filter_cond)

    # Convert the DataFrame to an RDD
    object_id_rdd = object_id_df.rdd

    # Extract the unique IDs from the RDD
    unique_ids = object_id_rdd.map(lambda row: row[0]).distinct().collect()

    # Convert the resulting RDD to a list
    unique_ids_list = list(unique_ids)

    logger.info(f"get_spread_ids returned {unique_ids_list}")

    return unique_ids_list
