import logging
from typing import Optional, List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, col, current_timestamp, date_format, lit
from worley_helper.configuration.config import Configuration
from worley_helper.configuration.transformations import AvailableTransforms
from worley_helper.utils.helpers import (
    rename_columns as rc,
    create_column_name_mapping_dict,
)


class Transforms:
    """Set of PySpark transformations"""

    def __init__(self, configuration: Optional[Configuration] = None):
        self.configuration = configuration
        self.logger = logging.getLogger(__name__)

    def run(self, df: Optional[DataFrame] = None) -> DataFrame:
        """Runs all desired transformations in the Order in which they are in the Config File"""

        if self.configuration.transforms is None:
            self.logger.info("There are no transformations to run, skipping...")
            return df

        # Loop through transforms
        for custom_transform in self.configuration.transforms:
            if custom_transform.transform == AvailableTransforms.custom_sql.value:
                df = self.run_custom_sql(
                    df, custom_transform.sql, custom_transform.temp_view_name
                )
            if custom_transform.transform == AvailableTransforms.hash_column.value:
                df = self.hash_column(
                    df, custom_transform.column_names, custom_transform.drop_original
                )
            if custom_transform.transform == AvailableTransforms.rename_columns.value:
                df = self.rename_columns(df)
                self._rename_column_status = True
            if (
                custom_transform.transform
                == AvailableTransforms.change_data_types.value
            ):
                df = self.change_data_types(df)
            if custom_transform.transform == AvailableTransforms.add_run_date.value:
                df = self.add_run_time(
                    df, custom_transform.column_name, custom_transform.date_format
                )

            if (
                custom_transform.transform
                == AvailableTransforms.select_columns_from_config_file.value
            ):
                df = self.select_columns_from_config_file(df)

        return df

    def hash_column(
        self, df: DataFrame, columns: List[str], drop_original: Optional[bool] = False
    ) -> DataFrame:
        """
        Hashes a DataFrame Column using Spark's `sha2` function

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
        Returns
        -------
        Spark DataFrame with the new hashed columns. The original columns remain.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.hash_column(df)
        """
        all_colummns = df.columns
        for col_to_hash in columns:
            if col_to_hash not in all_colummns:
                print(f"Column {col_to_hash} not in DataFrame, skipping")
                continue

            original_name = col_to_hash
            has_column_name = col_to_hash + "_hash"
            print(f"Hashing column {original_name} with target name {has_column_name}")
            df = df.withColumn(
                has_column_name, sha2(col(f"`{original_name}`").cast("string"), 256)
            )
        if drop_original:
            df = df.drop(*columns)

        return df

    def add_run_time(
        self, df: DataFrame, column_name: str, date_format_col: str
    ) -> DataFrame:
        """
        Adds a new column which is the date the pipeline was run

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
            column_name: str:
                The desired name of the resulting column
            date_format_col: str:
                The desired format for the datetime column
        Returns
        -------
        Spark DataFrame with the new column run_date.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.add_run_time(df = df, column_name="my_execution_date", date_format_col="yyyy-MM-dd")
        """
        df = df.withColumn(column_name, current_timestamp()).withColumn(
            column_name, date_format(col(column_name), date_format_col)
        )
        return df

    def run_custom_sql(
        self, df: DataFrame, custom_sql: str, temp_view_name: str
    ) -> DataFrame:
        """
        Runs a custom SQL Query

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
            custom_sql:str
                The SQL statement to run against the data
            temp_view_name:str
                The name to give the Temporary View of the df
        Returns
        -------
        Spark DataFrame with the new column run_date.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.run_custom_sql(df)
        """
        DDL_KEYWORDS = ["alter", "drop", "create", "truncate"]

        if any(substring in custom_sql for substring in DDL_KEYWORDS):
            self.logger.warning(
                "DDL Commands are not available, please use only `SELECT` statement"
            )
            return df

        df.createOrReplaceTempView(f"{temp_view_name}")
        spark = df.sparkSession
        df = spark.sql(custom_sql)

        return df

    def rename_columns(self, df: DataFrame) -> DataFrame:
        """
        Renames the Columns based on the Config File setup if they exist in dataframe

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
        Returns
        -------
        Spark DataFrame with the new column names.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.rename_columns(df)
        """
        # Prepares the required data from the Configuration file
        run_schema_transform = self.configuration.table_schema
        if run_schema_transform is False:
            print(
                "No Schema definition present in configuration file, skipping transform"
            )
            return df

        # Converts object to standard Python list
        columns = [x.model_dump() for x in self.configuration.table_schema.columns]
        new_col_dict = create_column_name_mapping_dict(columns)

        # Rename the columns
        return rc(df, new_col_dict)
        

    def change_data_types(
        self, df: DataFrame, desired_column_mapping: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Casts the Data Types based on the Config File setup

        WARNING:
        PySpark `cast` conversion fails silently. Use with caution. When it fails all rows are converted to null

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
            desired_column_mapping: Dict[str, str]:
                A dict with the format {col_name:data_type} to use to convert columns
        Returns
        -------
        Spark DataFrame with the new data types names.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.cast_data_types(df, desired_column_mapping = {"my_current_name":"my_desired_name"})
        """

        if desired_column_mapping is not None:
            mapping_dict = desired_column_mapping
        else:
            if self.configuration.table_schema is None:
                return df
            else:
                desired_columns = self.configuration.table_schema.columns
                mapping_dict = {
                    x.column_name: x.column_data_type for x in desired_columns
                }

        list_of_cols = []
        for df_col in df.columns:
            try:
                list_of_cols.append(col(df_col).cast(mapping_dict[df_col]))
            except:
                print(f"Failed to cast column {df_col} - not doing anything")
                list_of_cols.append(col(df_col))

        return df.select(list_of_cols)

    def select_columns_from_config_file(self, df: DataFrame) -> DataFrame:
        """
        Selects the columns from the Config File.
        Please note this function selects the columns using the `curated` name

        Parameters
        ----------
        Args:
            df: DataFrame:
                The Spark DataFrame
        Returns
        -------
        Spark DataFrame with the new column names.

        Examples
        -------
        >>> transforms = Transforms(configuration = config)
        >>> df = transforms.select_columns_from_config_file(df)
        """
        if self.configuration.table_schema is None:
            print(
                "No Schema definition present in configuration file, skipping transform"
            )
            return df

        desired_columns = self.configuration.table_schema.columns
        desired_columns_list = [x.column_name for x in desired_columns]

        if(self._rename_column_status):
            print('Column Rename is Complete, proceeding with selecting columns')

            # find missing columns which are prsent in config and not in transformed dataframe
            missing_cols_list = []
            for column in desired_columns_list:
                if column not in df.columns:
                    missing_cols_list.append(column)
            
            print(f'Hardcoding NULL for Missing Columns  : {missing_cols_list}')
            
            # hardcode NULL for all missing columns
            for column in missing_cols_list:
                df = df.withColumn(column, lit(None))
            
            return df.select(desired_columns_list)
            
        else:
            print('Column Rename is Incomplete, skipping column selection')
            return df
        
        
        # Psuedo
        # missing_cols_list = []
        # actual_col_list = list(df.columns)
        
        # for col in desired_columns_list:
        #     if col not in actual_col_list:
        #         missing_cols_list.append(col)

        # desired_columns_list = desired_columns_list + missing_cols_list
        # # Add missing columns to dataframe 
        # for col in missing_cols_list:
        #     df = df.withColumn(col, lit(None))
        
        

      


