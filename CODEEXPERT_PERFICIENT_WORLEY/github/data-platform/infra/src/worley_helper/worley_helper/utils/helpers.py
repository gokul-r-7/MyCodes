import pyspark
import boto3
import sys
from datetime import datetime
import pytz
from typing import Optional, Dict, List, Any, Union
import pyspark.sql.functions as f
import pyspark.sql as DataFrame
from pyspark.sql.functions import explode, col, concat_ws, sha2, lit, to_date, row_number, max
from pyspark.sql.types import TimestampType, DateType, TimestampNTZType
from pyspark.sql.window import Window
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT
from worley_helper.configuration.transformations import AvailableTransforms
from worley_helper.utils.logger import get_logger
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from urllib.parse import urlparse
import string
from botocore.exceptions import ClientError
import pandas as pd
import io
import re
import pyspark.sql.functions as F
import os 


# Init the logger for rest of the utility functions to log information
logger = get_logger(__name__)


def create_comma_separated_string(id_list: list[int]) -> str:
    """
    Function to create a comma-separated string from a list of numbers.

    Args:
        id_list (list[int]): A list of integers.

    Returns:
        str: A comma-separated string of the integers in the input list.
    """
    return ",".join(str(id) for id in id_list)

def add_masked_columns(df, col_names):
    """
    Function to create new columns as masked using the input col_names

    Args:
        df : dataframe to work on.
        col_names: list of col names to mask and create new columns out of it.
        

    Returns:
        df: returns new dataframe having extra masked columns
    """
    # Convert dataframe column names to lowercase for case-insensitive matching
    df_columns_lower = {col.lower(): col for col in df.columns}
    
    for col in col_names:
        col_lower = col.lower()  # Make input column name lowercase
        
        if col_lower not in df_columns_lower:
            print(f"Notification: Column '{col}' does not exist in the DataFrame.")
        else:
            # Retrieve the original case-sensitive column name
            original_col_name = df_columns_lower[col_lower]
            
            # Cast numeric columns to string before applying SHA-256
            df = df.withColumn(f"{col_lower}_masked", F.sha2(F.col(original_col_name).cast("string"), 256))
    
    return df

# Function to extract column names based on table_name
def extract_column_names(data, table_name):
    for table in data['table_definition']:
        if table['table_name'] == table_name:
            return [col['column_name'] for col in table['column_definitions']]
    return []


def flatten_nested_attributes(
    data_df: pyspark.sql.DataFrame, nested_attribute: str
) -> pyspark.sql.DataFrame:
    """
    Function to flatten nested attributes.

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        nested_attribute (str): The name of the nested attribute to be flattened.

    Returns:
        pyspark.sql.DataFrame: The input DataFrame with the nested attribute flattened.
    """
    data_df = data_df.withColumn(nested_attribute, explode(col(nested_attribute)))

    # Flatten struct attributes within the Period column
    period_cols = [
        field.name for field in data_df.schema[nested_attribute].dataType.fields
    ]
    for col_name in period_cols:
        data_df = data_df.withColumn(
            nested_attribute + "_" + col_name, col(nested_attribute).getField(col_name)
        )

    # Drop the original Period column
    data_df = data_df.drop(nested_attribute)

    return data_df


def get_partition_str_mi(
    input_date: str,
    timezone: str = TIMEZONE_SYDNEY,
    input_date_format: str = DATETIME_FORMAT,
) -> str:
    """
    Get partition string for the given input date, if the input date is none, it will consider today.

    Args:
        input_date (str): Input date in %Y/%m/%d format.
        timezone (str): Timezone for partition. Defaults to TIMEZONE_SYDNEY.
        input_date_format (str): Input date format. Defaults to DATETIME_FORMAT.

    Returns:
        str: Partition string with year=20xx/month=xx/day=xx/hour=xx/minute=xx.
    """
    input_date = datetime.strptime(input_date, input_date_format)

    year = input_date.strftime("%Y")
    month = input_date.strftime("%m")
    day = input_date.strftime("%d")
    hr = input_date.strftime("%H")
    mi = input_date.strftime("%M")
    partition_str = f"year={year}/month={month}/day={day}/hour={hr}/minute={mi}"

    return partition_str


def get_iceberg_table_properties(
    config_table_properties: Optional[Dict[str, str]] = {}
) -> str:
    """Creates the `TBLPROPERTIES` for an Iceberg Syntax"""
    value_list = []
    for k, v in config_table_properties.items():
        value_list.append(f"'{k}'" + "=" + f"'{v}'")

    if len(value_list) > 0:
        properties_string = f"TBLPROPERTIES ({', '.join(value_list)})"
    else:
        properties_string = ""
    return properties_string


def check_if_table_exists(
    database_name: str,
    table_name: str,
) -> bool:
    """Checks if the table exists in the Glue Catalog"""
    print(f"Looking for table `{table_name}` in database `{database_name}`")
    glue_client = boto3.client("glue")

    try:
        table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        print("Table found")
        return True

    except glue_client.exceptions.EntityNotFoundException:
        print("Table not found")
        return False
    except Exception as e:
        print("Error checking if table exists: ", e)
        raise Exception(
            f"Error checking if table `{database_name}`.`{table_name}` exists: {e}"
        )


def get_iceberg_partitions(config_partitions: Optional[List[str]] = []) -> str:
    """Creates the `PARTITIONED BY` statement for an Iceberg table"""

    if len(config_partitions) > 0:
        part_cols = ", ".join(config_partitions)
        partition_string = f"PARTITIONED BY ({part_cols})"
    else:
        partition_string = ""

    return partition_string


def rename_columns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    """Function that Renames Columns based on a Dictionary of {old:new}"""
    if isinstance(columns, dict):
        renamed_df = df.select(
            *[
                f.col(col_name).alias(columns.get(col_name.lower(), col_name))
                for col_name in df.columns
            ]
        )
        logger.info(f'Dataframe Schema after column name rename')
        print(renamed_df.printSchema())
        return renamed_df
    
    else:
        raise ValueError(
            "'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}"
        )


def standardize_columns(col_name: str) -> str:
    """
    Define a function to replace "." and ":" with "_" in a column name.

    Args:
        col_name (str): The column name to be renamed.

    Returns:
        str: The renamed column name.
    """
    #remove double __ from column name
    col_name = re.sub(r"__+", "", col_name)

    # Replace "." and ":" with "_"
    col_name = col_name.replace(".", "_").replace(":", "_")

    # Remove starting __ 
    if col_name.startswith("__"):
      col_name = col_name[2:] 

    # Trim the column name if it ends with "__value"
    if col_name.lower().endswith("__value"):
        col_name = col_name[:-7]

    return col_name

def standardize_columns_csv_xlsx(column_name: str, special_chars=[], replacement_chars="_", replace_non_ascii=True, replace_non_alphanumeric_with_underscore=True) -> str:
    """
    Normalize a column name by replacing non-printable ASCII characters with underscores,
    removing any trailing "#" characters, and replacing specified special characters.

    Args:
        column_name (str): The original column name.
        special_chars (list): List of special characters to be replaced.
        replacement_chars (str): Character(s) to replace special characters with.
        replace_non_ascii (bool): Flag to indicate whether to replace non-printable ASCII characters.

    Returns:
        str: The normalized column name.
    """
    
    logger.info(f"Original column name was {column_name}")
    if replace_non_ascii:
        # Define a set of printable ASCII characters
        printable_ascii = set(string.printable)

        # Replace non-printable ASCII characters with underscores
        normalized_name = ''.join(
            char if char in printable_ascii else '_' for char in column_name
        )
        logger.info(f"Normalized column name with non-ASCII characters replaced: {normalized_name}")
    elif replace_non_alphanumeric_with_underscore:
        # Replace non-alphanumeric characters with underscores
        normalized_name = ''.join(
            char if char.isalnum() else '_' for char in column_name
        )
        logger.info(f"Normalized column name with non-alphanumeric characters replaced: {normalized_name}")

    else:
        normalized_name = column_name
    
    # Replace special characters with replacement_chars
    for char in special_chars:
        normalized_name = normalized_name.replace(char, replacement_chars)
        logger.info(f"Normalized column name with special characters replaced: {normalized_name}")

    # Remove any trailing "#" characters
    normalized_name = normalized_name.rstrip('#')
    # Remove any trailing "_" characters
    normalized_name = normalized_name.rstrip('_')  
    # Remove any leading "_" characters
    normalized_name = normalized_name.lstrip('_')  
    # Replace any  "__" characters with "_"
    normalized_name = normalized_name.replace("__", "_")
    
    logger.info(f"Standardized column name was {normalized_name}")

    return normalized_name


def create_column_name_mapping_dict(
    config_columns: List[Dict[str, Any]]
) -> Dict[str, str]:
    """Helper function to create a dictionary mapping of columns names for the configuration file"""
    return_dict = {}
    for col in config_columns:
        raw_col_name = col.get("raw_column_name")
        curated_col_name = col["column_name"]
        if raw_col_name is not None:
            return_dict[raw_col_name] = curated_col_name
        else:
            return_dict[curated_col_name] = curated_col_name
    return return_dict


# Function to read Excel file from S3
def read_excel_from_s3(bucket, key, sheetName, header_row, xls_engine_name, region, skiprows=None):
    try:
        s3 = boto3.client('s3', region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=key)

        binary_xlsx_content = io.BytesIO(obj['Body'].read())
        # Create an ExcelFile object
        excel_file = pd.ExcelFile(binary_xlsx_content)

        # Get a list of sheet names
        sheet_names = excel_file.sheet_names

        # Print the sheet names
        logger.info(f"{sheet_names} found in xlsx {key}")

        # Trim the provided sheetName and search in sheet_names
        sheetName = sheetName.strip()
        for name in sheet_names:
            if sheetName == name.strip():
                logger.info(f"Sheet '{sheetName}' found in the xlsx file (original name: '{name}').")
                #Save the original sheet name to be used in read_excel
                sheetName = name
                logger.info(f"{sheetName} before trimming")
                break
        else:
            logger.error(f"Sheet '{sheetName}' not found in the xlsx file {key}.")
            return None

        return pd.read_excel(binary_xlsx_content, sheet_name=sheetName, engine=xls_engine_name, header=header_row, skiprows=skiprows, na_values=[], keep_default_na=False)
    except ClientError as e:
        logger.error(f"Error accessing S3 object {key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading Excel file {key} : {e}")
        return None


def write_glue_df_to_s3_with_specific_file_name(
    glueContext: GlueContext,
    df: DynamicFrame,
    s3_bucket_name: str,
    s3_key: str,
    tf_ctx : str,
    **kwargs
) -> bool:
    """
    Write a Spark DataFrame to Amazon S3 with a specific file name.

    Args:
        glueContext (GlueContext): The Glue context object.
        df (pyspark.sql.DataFrame): The Spark DataFrame to be written to S3.
        s3_bucket_name (str): The name of the S3 bucket where the data will be written.
        s3_key (str): The key (file path) within the S3 bucket where the data will be written.

    Returns:
        bool: True if the data was successfully written to S3, False otherwise.
    """
    try:
        # Rename the columns
        # renamed_dynamic_frame = replace_dot2underscore_in_dynamicframe(df)
        # Rename fields (columns) by replacing '.' with '_'
        current_columns = [field.name for field in df.schema()]

        logger.info(f"Logging varialbe params {kwargs}")

        csv_or_xlsx_datasource = kwargs.get('csv_or_xlsx_datasource', False)
        if csv_or_xlsx_datasource:
            logger.info("Column name standardization being performed for csv or xlsx based datasource")
            special_chars = kwargs.get('special_chars', [])
            replacement_chars = kwargs.get('replacement_chars', "_")
            replace_non_ascii = kwargs.get('replace_non_ascii', True)
            replace_non_alphanumeric_with_underscore = kwargs.get('replace_non_alphanumeric_with_underscore', True)
            renamed_columns = [standardize_columns_csv_xlsx(col, special_chars, replacement_chars, replace_non_ascii, replace_non_alphanumeric_with_underscore)  for col in current_columns]
        else:
            # Rename the columns
            renamed_columns = [standardize_columns(col) for col in current_columns]
        
        logger.info(f"Renamed columns {renamed_columns}")

        # Create a new DataFrame with renamed columns
        renamed_df = df.toDF().select(
            [
                col("`{}`".format(c)).alias(renamed_columns[i])
                for i, c in enumerate(current_columns)
            ]
        )
        # Convert the renamed DataFrame back to a DynamicFrame
        renamed_dynamic_frame = DynamicFrame.fromDF(
            renamed_df, glueContext, "renamed_dynamic_frame"
        )

        ####
        typecast_cols_to_string = kwargs.get('typecast_cols_to_string', False)
        if typecast_cols_to_string:
            string_typecasted_df = DynamicFrame.fromDF(
                renamed_dynamic_frame.toDF().select(*[col(c).cast("string").alias(c) for c in renamed_dynamic_frame.toDF().columns]),
                glueContext,
                "dyf_with_string_columns"
            )

            renamed_dynamic_frame = string_typecasted_df

            logger.info(f"Typecasted schema {renamed_dynamic_frame.schema()}")
            logger.info(f"Typecasted count {renamed_dynamic_frame.toDF().limit(1).collect()}")
        ####

        handle_voids = kwargs.get('handle_voids', False)
        if handle_voids:
            logger.info(f"Before Voids schema: {renamed_dynamic_frame.schema}")

            void_columns = [col[0] for col in renamed_dynamic_frame.toDF().dtypes if col[1] == 'void']
            
            if void_columns:  # Check if there are any void columns
                renamed_dynamic_frame = renamed_dynamic_frame.resolveChoice(specs=[(col, "cast:string") for col in void_columns])

            logger.info(f"After Void Handling schema: {renamed_dynamic_frame.schema}")
            logger.info("Void Handling count: {}".format(renamed_dynamic_frame.toDF().limit(1).collect()))        
        # control the format of file and format options
        out_format = kwargs.get('s3_output_format', "parquet")
        out_format_options = kwargs.get('format_options', {})
        # Write the DynamicFrame to Amazon S3
        glueContext.write_dynamic_frame.from_options(
            frame=renamed_dynamic_frame,
            connection_type="s3",
            connection_options={"path": f"s3://{s3_bucket_name}/{s3_key}"},
            format=out_format,
            format_options=out_format_options,
            transformation_ctx=tf_ctx

        )

        logger.info(f"Successfully wrote DynamicFrame to S3 at s3://{s3_bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Error processing data or writing to S3: {e}")
        return False

def create_mappings(schema):
    mappings = []
    for field in schema.fields:
        if field.dataType.typeName() in ["struct", "array"]:
            # Keep the original type for struct and array fields
            mappings.append((field.name, field.dataType.typeName(), field.name, field.dataType.typeName()))
        else:
            # Convert all other types to string
            mappings.append((field.name, field.dataType.typeName(), field.name, "string"))
    return mappings

def write_spark_df_to_s3_with_specific_file_name(df, output_path):
    # Repartition and write spark dataframe to S3
    df.coalesce(1).write.mode("overwrite").format(
        output_path.split(".")[-1]
    ).save(
        "/".join(output_path.split("/")[0:-1])
    )

    # Extract bucket and key name given a S3 file path
    s3_path = urlparse(output_path, allow_fragments=False)
    bucket_name, key = s3_path.netloc, s3_path.path.lstrip("/")

    # Rename the part file
    try:
        s3 = boto3.resource('s3')
        prefix = "/".join(key.split("/")[0:-1]) + "/part"
        for obj in s3.Bucket(bucket_name).objects.filter(Prefix=prefix):
            s3.Bucket(bucket_name).copy({'Bucket': bucket_name, 'Key': obj.key}, key)
            s3.Object(bucket_name, obj.key).delete()
    except Exception as err:
        raise Exception("Error renaming the part file to {}: {}".format(output_path, err))


def get_run_date_column_name(transforms) -> str:

    """
    Function to get the run date Column Name

    Args:
        transforms: Avaialable Transformations within Configuration
    
    Returns:
        str: Column name for the run date column
    """
    for custom_transform in transforms:
        if  custom_transform.transform == AvailableTransforms.add_run_date.value:
            run_date_column_name = custom_transform.column_name  
            logger.info(f"Run Date Column Name Found : {run_date_column_name}")

    return run_date_column_name


def add_primary_key(data_df: pyspark.sql.DataFrame,
                    primary_key: Union[str, List[str], None],
                        run_date_column_name: str) -> pyspark.sql.DataFrame:
    """
    Function to add Primary Key

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        primary_key: Primary Key Configuration
        run_date_column_name : Run Date Column Name

    Returns:
        pyspark.sql.DataFrame: The input DataFrame with the primary_key column
    """
    if primary_key:
        logger.info('Checking whether its a single or composite primary key')

        # check if primary_key is list i.e. Composite Primary Key
        if isinstance(primary_key, list):
            logger.info('Composite Primary Key is specified in config')
            
            try:

                # data_df = data_df.withColumn('primary_key', sha2(concat_ws("-", 
                #             *[col(c).cast("binary") for c in primary_key]), 256))
                data_df = data_df.withColumn('primary_key', sha2(concat_ws("-", *primary_key), 256))
                logger.info('Successfully added Composite Primary Key')
            except Exception as ex:
                logger.error('Error adding Composite Primary Key')
                raise Exception(f'Error adding Composite Primary Key : {str(ex)}')
        
        else:
            logger.info('Single Primary Key is specified in config')
            try:
                
                # casting the column to string Type as sha2 function
                # throws an error for non string data types
                data_df = data_df.withColumn('primary_key', sha2(col(primary_key).cast('string'), 256))

                logger.info('Successfully added Single Primary Key')
            except Exception as ex:
                logger.error('Error adding Single Primary Key')
                raise Exception(f'Error adding Single Primary Key : {str(ex)}')
            
    else:
        # Calculating Row value hash and adding that as a primary key
        logger.info('Assigning Row Hash as the Primary Key')

        if("row_hash" in data_df.columns):
            data_df = data_df.withColumn('primary_key', col("row_hash"))
            
    return data_df
        

def add_control_columns(data_df:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Function to add Control Columns for SCD Type2  Load

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.

    Returns:
        pyspark.sql.DataFrame: The input DataFrame with the control columns
    """
    logger.info('Adding Control Columns for SCD Type2 Load')
    try:
        #flag indicating if the row is current
        data_df = data_df.withColumn('is_current', lit(1))
        # defining eff_start_date as current_date
        data_df = data_df.withColumn('eff_start_date', to_date(lit(datetime.now())))
        # defining eff_end_date with default 9999-12-31
        data_df = data_df.withColumn('eff_end_date', to_date(lit('9999-12-31')))
        logger.info('Successfully added Control Columns')
    
    except Exception as ex:
        logger.error('Error adding Control Columns for SCD Type2 Load')
        raise Exception(f'Error adding Control Columns : {str(ex)}')
    
    return data_df


def add_row_hash_column(data_df: pyspark.sql.DataFrame,
                        run_date_column_name: str) -> pyspark.sql.DataFrame:
    """
    Function to add Row Hash Column

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        run_date_column_name : Run Date Column Name


    Returns:
        pyspark.sql.DataFrame: The input DataFrame with the row hash column
    """
    logger.info('Adding Row Hash Column')

    try:
        # removing the run_date column added through transforms from the columns
        all_columns = data_df.columns
        all_columns.remove(run_date_column_name)

        data_df = data_df.withColumn(
                'row_hash', sha2(concat_ws("-", *all_columns), 256)
                )

        return data_df

    except Exception as ex:
        logger.error(f'Error while Calculating Row Hash : {str(ex)}')

def validate_timestamp_column(data_df: pyspark.sql.DataFrame,
                        timestamp_column_name: str) -> bool:
    """
    Function to validate a timestamp Column

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        timestamp_column_name : Run Date Column Name

    Returns:
        pyspark.sql.DataFrame: True or False to indicate if column is date or datetime or time
    """
    logger.info('Validating Timestamp Column Name')
    try:
        column_type = data_df.schema[timestamp_column_name].dataType
        logger.info(f"Data Type of timestamp column {timestamp_column_name} : {column_type}")
        return isinstance(column_type, (TimestampType, DateType, TimestampNTZType))
    except Exception as ex:
        logger.error(f"Column {timestamp_column_name} not Found in DataFrame")
        raise ex

def dedup_records(data_df: pyspark.sql.DataFrame, 
                  timestamp_column: str) -> pyspark.sql.DataFrame:
    """
    Function to deduplicate records based on primary_key Column

    Args:
        data_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        timestamp_column (str): The appropriate timestamp column to rank duplicate records

    Returns:
        pyspark.sql.DataFrame: The output Spark DataFrame within which records are ded-duplicated
    """

    original_record_count = data_df.count()
    logger.info(f"Data Frame count before enforcing primary key constraint : {original_record_count}")

    window = Window.partitionBy("primary_key").orderBy(col(timestamp_column).desc())
    dedup_df = data_df.withColumn("row_num", row_number().over(window)) \
       .where("row_num = 1") \
       .drop("row_num")
    
    deduped_record_count = dedup_df.count()
    logger.info(f"Data Frame count after enforcing primary key constraint : {deduped_record_count}")
    
    dropped_record_count = original_record_count - deduped_record_count
    logger.info(f"Dropped Records in the DataFrame : {dropped_record_count}")

    return dedup_df

def save_spark_df_to_s3_with_specific_file_name(df, output_path):
    # Repartition and write spark dataframe to S3
    df.coalesce(1).write.mode("overwrite").format("parquet").save(output_path)

    # Extract bucket and key name given a S3 file path
    s3_path = urlparse(output_path, allow_fragments=False)
    bucket_name, key = s3_path.netloc, s3_path.path.lstrip("/")

    # Rename the part file
    # try:
    #     s3 = boto3.resource('s3')
    #     prefix = "/".join(key.split("/")[0:-1]) + "/part"
    #     for obj in s3.Bucket(bucket_name).objects.filter(Prefix=prefix):
    #         s3.Bucket(bucket_name).copy({'Bucket': bucket_name, 'Key': obj.key}, key)
    #         s3.Object(bucket_name, obj.key).delete()
    # except Exception as err:
    #     raise Exception("Error renaming the part file to {}: {}".format(output_path, err))

class S3Helper:
    def __init__(self, bucket_name, bucket_key, kms_key_arn):
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.kms_key_arn = kms_key_arn
        self.s3_client = boto3.client("s3")

    def upload_files(self, files):
        # Upload files to S3
        print("start file upload activity: Uploading.... ")
        for file_name, file_data in files:
            print("here is the file name for upload", file_name)
            s3_key = os.path.join(self.bucket_key, file_name)
            print("desired S3 key name: ", s3_key)
            try:
                print("The upload is going to start now: xxxxxxxxxxxx")
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        "ServerSideEncryption": "aws:kms",
                        "SSEKMSKeyId": self.kms_key_arn,
                    },
                )
            except Exception as e:
                print(f"Failed to upload file {file_name} to S3: {e}")

    def cps_upload_files(self, files, archive_key):
        # Upload files to S3
        print("start file upload activity: Uploading.... ")
        for file_name, file_data in files:
            print("here is the file name for upload", file_name)
            
            # Ensure the correct key path for S3 upload
            s3_key = os.path.join(archive_key, file_name)  # Use archive_key as the full path
            print("desired S3 key name: ", s3_key)

            try:
                print("The upload is going to start now: xxxxxxxxxxxx")
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        "ServerSideEncryption": "aws:kms",
                        "SSEKMSKeyId": self.kms_key_arn,
                    },
                )
                print(f"Successfully uploaded {file_name} to {s3_key}")
            except Exception as e:
                print(f"Failed to upload file {file_name} to S3: {e}")
                
    def upload_file(self, file_path,file_name):
        # Upload file to S3
        print("start file upload activity: Uploading.... ")
        print("here is the file name for upload", file_name)
        s3_key = os.path.join(self.bucket_key, file_name)
        print("desired S3 key name: ", s3_key)
        try:
            print("The upload is going to start now: xxxxxxxxxxxx")
            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": self.kms_key_arn,
                },
            )
        except Exception as e:
            print(f"Failed to upload file {file_name} to S3: {e}")
    
    def delete_s3_file(self, filename):
        try:
            s3_key = os.path.join(self.bucket_key, filename)
            # Delete the object from S3
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)

            return f"'{s3_key}' deleted successfully from bucket '{self.bucket_name}'."

        except ClientError as e:
            return f"Error deleting '{s3_key}': {e}"
    
    def rename_s3_file(self, current_file_name, new_file_name=None):
        try:
            # Construct the S3 keys
            current_s3_key = os.path.join(self.bucket_key, current_file_name)
            if new_file_name:
                new_s3_key = os.path.join(self.bucket_key, new_file_name)
            else:
                base_name = os.path.basename(current_file_name)
                new_s3_key = os.path.join(self.bucket_key, f"processed_{base_name}")

            # Copy the object to the new key with KMS encryption
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': current_s3_key},
                Key=new_s3_key,
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=self.kms_key_arn
            )
            
            # Delete the original object
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=current_s3_key)

            return f"Renamed '{current_file_name}' to '{os.path.basename(new_s3_key)}' in S3 bucket '{self.bucket_name}'."
        except Exception as e:
            return f"Error renaming file: {e}"
        
    def list_files(self, folder_name):
        file_paths = []
        if not folder_name.endswith('/'):
            folder_name += '/'
    
        paginator = self.s3_client.get_paginator('list_objects_v2')
    
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=folder_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('/'):  # Skip directories
                        absolute_path = f"s3://{self.bucket_name}/{key}"
                        file_paths.append(absolute_path)
    
        return file_paths
    
    def download_files(self, file_paths, local_directory='/tmp'):
        os.makedirs(local_directory, exist_ok=True)
        downloaded_files = []

        for file_path in file_paths:
            try:
                parsed_url = urlparse(file_path)
                s3_key = parsed_url.path.lstrip('/')
                
                # Skip if it's a directory (ends with '/')
                if s3_key.endswith('/'):
                    print(f"Skipping directory: {file_path}")
                    continue

                file_name = os.path.basename(s3_key)
                local_file_path = os.path.join(local_directory, file_name)

                print(f"Attempting to download: {file_path} to {local_file_path}")
                
                self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
                downloaded_files.append(local_file_path)
                print(f"Successfully downloaded: {file_path} to {local_file_path}")

            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")

        print(f"Download operation completed. {len(downloaded_files)} files downloaded.")
        
        return downloaded_files

