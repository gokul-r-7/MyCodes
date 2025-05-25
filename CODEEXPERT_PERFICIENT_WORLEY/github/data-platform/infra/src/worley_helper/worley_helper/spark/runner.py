"""
Python Class that abstracts the different common actions for Spark
"""
import os
import logging
from typing import Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from typing import Optional

from worley_helper.configuration.config import Configuration
from worley_helper.configuration.io import SparkFormat, LoadType, EntityLoadProperty
from worley_helper.utils.helpers import (
    get_iceberg_table_properties,
    check_if_table_exists,
    get_iceberg_partitions,
    get_run_date_column_name,
    add_primary_key,
    add_control_columns,
    add_row_hash_column,
    validate_timestamp_column,
    dedup_records
)
from worley_helper.utils.aws import get_aws_account_id
from worley_helper.configuration.project_mapping import SourceTableModel, FilterConditionModel

logger = logging.getLogger(__name__)

try:
    from awsglue.context import GlueContext
except:
    print("Couldn't import GlueContext, Glue specifics will fail")


class SparkDataRunner:
    """A class to manage all Spark related activities"""

    def __init__(self, configuration: Configuration, **kwargs):
        # Adds the config file
        self.configuration = configuration
        self.spark: SparkSession = kwargs.get("spark",None)
        self.source_data_location: str = kwargs.get("source_data_location", None)
        self.glue_context: Any = kwargs.get("glue_context", None)
        self.logger = logging.getLogger(__name__)

        if (
            self.configuration.source.spark_options.options.get("path") is None
            and self.source_data_location is None
        ):
            print(
                "No Data Location Provided, please provide a source path location by adding the `source_data_location` parameter to the class initiation"
            )

    def _get_spark_iceberg(self) -> SparkSession:
        """Sets the Spark Session for Iceberg"""

        warehouse = (
            self.configuration.target.iceberg_properties.iceberg_configuration.iceberg_catalog_warehouse
        )
        print(
            f"Setting up the appropriate Spark session for Iceberg using the following warehouse: {warehouse}"
        )
        conf = (
            SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .set(
                "spark.sql.sources.partitionOverwriteMode",
                "dynamic",
            )
            .set(
                "spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .set(
                "spark.sql.catalog.glue_catalog.warehouse",
                f"s3://{warehouse}",
            )
            .set(
                "spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .set(
                "spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
        )
        self.spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

    def _read_parquet(self) -> DataFrame:
        """Method that reads Parquet Files into a Spark DataFrame"""
        location = (
            self.configuration.source.spark_options.options.get("path")
            or self.source_data_location
        )
        if location is None:
            raise ValueError("No Data Location Provided")

        self.logger.info(f"Reading Data from {location}")
        return self.spark.read.load(
            format="parquet",
            path=location,
            **self.configuration.source.spark_options.options,         
        )

    def _read_dynamic_dataframe(self) -> Any:
        """Method that reads Dynamic DataFrames into a Spark DataFrame"""
        print(
            "Reading Dynamic Dataframe, please use .toDF() to convert to Spark DataFrame"
        )

        # Get the connection options
        if self.source_data_location is not None:
            connection_options = {"paths": [self.source_data_location]}

        elif self.configuration.source.glue_options.connection_options is not None:

            if(self.configuration.source.glue_options.format == SparkFormat.parquet.value):
                connection_options = {
                    "paths": [self.configuration.source.glue_options.connection_options],
                    "mergeSchema": True
                }
            else:
                connection_options = {
                    "paths": [self.configuration.source.glue_options.connection_options]
                }

        else:
            raise Exception(
                "Please include either the connection_options value or source_data_location"
            )

        print(
            f"Creating DynamicDataframe with connection_options: {connection_options}"
        )
        print(
            f"Creating DynamicDataframe with transformation_ctx: {self.configuration.source.glue_options.transformation_ctx}"
        )

        if(self.configuration.target.entity_load):
            print(f"Entity Load is enabled, building Glue Connections as per Entity Values")

            entity_transformation_ctx = self.configuration.source.glue_options.transformation_ctx
            entity_connection_options = f"{self.configuration.source.glue_options.connection_options}"
            
            if(not entity_connection_options.endswith('/')):
                entity_connection_options = f"{self.configuration.source.glue_options.connection_options}/"
                
            else:
                entity_connection_options = f"{self.configuration.source.glue_options.connection_options}"

            for entity_load_property in self.configuration.target.entity_load_properties:
                entity_s3_path = entity_connection_options + \
                            f"{entity_load_property.entity_s3_raw_partition_prefix}={entity_load_property.entity_table_attribute_value}/"
        
                entity_transformation_ctx = entity_transformation_ctx + '-' + \
                        f"{entity_load_property.entity_job_attribute_name}={entity_load_property.entity_table_attribute_value}-"
            
            # if format is parquet then add mergeSchema = True
            if(self.configuration.source.glue_options.format == SparkFormat.parquet.value):
                entity_connection_options = {
                    "paths": [entity_s3_path],
                    "mergeSchema": True
                }
            else:
                entity_connection_options = {
                    "paths": [entity_s3_path]
                }

            print(f"Entity Connection Options: {entity_connection_options}")
            print(f"Entity Transformation Context: {entity_transformation_ctx}")

            glue_df = self.glue_context.create_dynamic_frame.from_options(
                connection_type=self.configuration.source.glue_options.connection_type,
                connection_options=entity_connection_options,
                format=self.configuration.source.glue_options.format,
                format_options=self.configuration.source.glue_options.format_options,
                transformation_ctx=entity_transformation_ctx,
            )

        else:

            print(f"Entity Load is not enabled")

            glue_df = self.glue_context.create_dynamic_frame.from_options(
                connection_type=self.configuration.source.glue_options.connection_type,
                connection_options=connection_options,
                format=self.configuration.source.glue_options.format,
                format_options=self.configuration.source.glue_options.format_options,
                transformation_ctx=self.configuration.source.glue_options.transformation_ctx,
            )
        # glue_df = self.glue_context.create_dynamic_frame.from_options(
        #     connection_type=self.configuration.source.glue_options.connection_type,
        #     connection_options=connection_options,
        #     format=self.configuration.source.glue_options.format
        #     #transformation_ctx=self.configuration.source.glue_options.transformation_ctx,
        # )
        # print("========Here==========")
        # print(glue_df.printSchema())
        # print(glue_df.count())

        return glue_df
    
    def _read_iceberg(self) -> DataFrame:
        """Method that reads Iceberg Tables into a Spark DataFrame"""

        # Defines Database, Tables & Load_Type
        table_name = self.configuration.target.iceberg_properties.table_name
        database = self.configuration.target.iceberg_properties.database_name

        # Check if table exists
        if not check_if_table_exists(database, table_name):
            raise ValueError(
                f"Table {table_name} does not exist in the {database} database"
            )
        curated_df =  self.spark.read.format("iceberg").load(f"glue_catalog.{database}.{table_name}")
        #curated_df = self.spark.sql(f"SELECT * FROM glue_catalog.{database}.{table_name}")
        return curated_df
    

    def _write_iceberg(self, df: DataFrame, **kwargs) -> None:
        """Writes the contents of a DataFrame as an Iceberg table"""
        # Get Iceberg Spark Session
        #self._get_spark_iceberg()

        # Defines Database, Tables & Load_Type
        table_name = self.configuration.target.iceberg_properties.table_name
        database = self.configuration.target.iceberg_properties.database_name
        load_type = self.configuration.target.load_type
        run_date_column_name = get_run_date_column_name(self.configuration.transforms)


        # primary_key_attribute = getattr(self.configuration, 'table_schema.schema_properties.primary_key', 
        #                                     None)
             
        if(self.configuration.table_schema.schema_properties.primary_key):
            primary_key_attribute = self.configuration.table_schema.schema_properties.primary_key
            print(f'Primary Key found in the Table Schema Configuration : {primary_key_attribute}')

            if(self.configuration.target.primary_constraint_properties):
                enforce_primary_constraint = self.configuration.target.primary_constraint_properties.enforce_primary_constraint
                
                if(enforce_primary_constraint):
                    timestamp_column = self.configuration.target.primary_constraint_properties.timestamp_column_name
                else:
                    print(f"""Enforcing Primary Constraint is False, Deduplication on Primary Key
                                is disabled """)
            
            else:
                print(f"""Primary Constraint Properties are missing, Deduplication on Primary Key
                                is disabled """)
                enforce_primary_constraint = None

        else:
            print(f'Primary Key not found in the Table Schema Configuration')
            primary_key_attribute = None
            print(f"""Since Primary key is missing, Deduplication on Primary Key
                                is disabled """)
            enforce_primary_constraint = None
        
        # get entity_load properties if present in the configuration file
        entity_load = self.configuration.target.entity_load
        if(entity_load):
            entity_load_properties = self.configuration.target.entity_load_properties
            print(f"Entity Load Properties found in the Configuration : {entity_load_properties}")
        else:
            entity_load_properties = None

        
        if(load_type == LoadType.cdc.value):
            if(self.configuration.target.cdc_properties):
                cdc_status_column = self.configuration.target.cdc_properties.cdc_operation_column_name
                print(f"CDC Status Column found in the Table Schema Configuration : {cdc_status_column}")

                cdc_key_operation_dict = self.configuration.target.cdc_properties.cdc_operation_value_map
                print(f"CDC Key Operation Dictionary found in the Table Schema Configuration : {cdc_key_operation_dict}")
        
            else:
                print(f'CDC Properties not found in the Table Schema Configuration')
                os._exit(1)


        full_table_name = f"{database}.{table_name}"
        print(f"Converting to Iceberg using name: {full_table_name}")

        # Create a temp view to use SQL rather than DF
        temp_table_name = f"tmp_iceberg_{table_name}"
        cdc_temp_table_name = f"cdc_tmp_iceberg_{table_name}"

        # Add Control Columns if load_type is incremental so that columns gets added for table creation
        if(load_type == LoadType.incremental.value):
            df = add_row_hash_column(df,run_date_column_name)
            df = add_primary_key(df, primary_key_attribute, run_date_column_name)
            df = add_control_columns(df)
        
        if(load_type == LoadType.cdc.value):
            df = add_primary_key(df, primary_key_attribute, run_date_column_name)
            df = add_control_columns(df)
        
        if(load_type == LoadType.incremental_no_delete.value):
            df = add_primary_key(df, primary_key_attribute, run_date_column_name)
            df = add_control_columns(df)

        
        if(load_type == LoadType.cdc.value or load_type == LoadType.incremental.value
           or load_type == LoadType.incremental_no_delete.value):
            
            if(enforce_primary_constraint):
                is_timestamp_valid = validate_timestamp_column(df, timestamp_column)
            
                if(is_timestamp_valid):
                    print(f"Column provided for enforcing Primary Constraint i.e {timestamp_column} is a valid data type")
                    print(f"Deduplicating Records based on Primary Key Column")
                    df = dedup_records(df, timestamp_column)
                else:
                    print(f"Column provided for enforcing Primary Constraint i.e {timestamp_column} is not valid data type")
                    os._exit(1)

        df.createOrReplaceTempView(temp_table_name)

        # Define the Iceberg Table Properties
        table_properties = get_iceberg_table_properties(
            self.configuration.target.iceberg_properties.iceberg_configuration.table_properties
        )
        print("Defining Iceberg table properties as: ", table_properties)

        iceberg_table_name = f"glue_catalog.{full_table_name}"

        # Checks for Partitions
        if self.configuration.target.spark_options.partition is not None:
            partition_cols = get_iceberg_partitions(
                self.configuration.target.spark_options.partition.columns
            )
            print("Defining the table partition as: ", partition_cols)
        else:
            partition_cols = ""
            print("No partition columns found")

        if (
            self.configuration.target.iceberg_properties.iceberg_configuration.create_table
        ):
            print(
                f"Creating Iceberg Table, checking if the table exists. Using {database} and {table_name} as parameters"
            )
            # Check if table exists
            if (
                check_if_table_exists(database_name=database, table_name=table_name)
                is False
            ):
                print("Table does not exist, creating Iceberg table")
                sql = f"""
                    CREATE TABLE {iceberg_table_name}
                    USING iceberg
                    {partition_cols}
                    {table_properties}
                    LOCATION '{self.configuration.target.spark_options.options['path']}'
                    AS SELECT * FROM {temp_table_name}
                """
                print(sql)
                res = self.spark.sql(sql)
                print("Table successfully created...")
                return True
            else:
                print(f"{database} and {table_name} found")

                
                if load_type == LoadType.full_load.value:
                    print(f"Load Type is Full Load")
                    
                    if(entity_load):
                        print(f"Entity Load is enabled")
                        
                        if(len(entity_load_properties) > 1):
                            delete_filter = ''
                            #print(f"More than one Entity Load Property found"))
                            for entity_load_property in entity_load_properties:
                                entity_table_attribute_name = entity_load_property.entity_table_attribute_name
                                entity_table_attribute_value = entity_load_property.entity_table_attribute_value

                                if(delete_filter == ''):
                                    delete_filter = f"{entity_table_attribute_value} = '{entity_table_attribute_value}' "
                                
                                else:
                                     delete_filter = delete_filter + f"AND {entity_table_attribute_value} = '{entity_table_attribute_value}' "
                        
                        else:
                            entity_load_property = entity_load_properties[0]
                            delete_filter = f"{entity_load_property.entity_table_attribute_name} = '{entity_load_property.entity_table_attribute_value}' "

                        delete_sql = f"""
                            DELETE FROM {iceberg_table_name}
                            WHERE {delete_filter}
                            """
                        
                        print(f"Executing Delete Statement : {delete_sql}")
                        self.spark.sql(delete_sql)

                        insert_sql = f"""
                            INSERT INTO {iceberg_table_name}
                            SELECT * FROM {temp_table_name}
                            """
                        
                        print(f"Executing Insert Statement : {insert_sql}")
                        self.spark.sql(insert_sql)
                        
                        return True
                    
                    else:
                        logger.info("Performing a Full Load by Overwriting existing data")

                    # Create a new Iceberg table by overwriting the existing one
                        sql = f"""
                        INSERT OVERWRITE {iceberg_table_name}
                        SELECT * FROM {temp_table_name}
                        """
                    
                        print(f"Executing Statement : {sql}")
                        self.spark.sql(sql)
                        return True
                
                elif load_type == LoadType.incremental:
                    logger.info("Performing Incremental Load in SCD Type 2")

                    if(entity_load):
                        print(f"Entity Load is enabled")

                        if(len(entity_load_properties) > 1):
                            filter_clause = ''

                            for entity_load_property in entity_load_properties:
                                entity_table_attribute_name = entity_load_property.entity_table_attribute_name
                                entity_table_attribute_value = entity_load_property.entity_table_attribute_value

                                if(filter_clause == ''):
                                    filter_clause = f"hist.{entity_table_attribute_name} = '{entity_table_attribute_value}' "
                                
                                else:
                                     filter_clause = filter_clause + f"AND hist.{entity_table_attribute_name} = '{entity_table_attribute_value}' "
                        
                        else:
                            entity_load_property = entity_load_properties[0]
                            filter_clause = f"hist.{entity_load_property.entity_table_attribute_name} = '{entity_load_property.entity_table_attribute_value}' "

                        print(f'Filter Clause : {filter_clause}')

                        df.createOrReplaceTempView(temp_table_name)
                        
                        # filter out records in incoming dataset which have same row_hash in target after applying filter
                        row_hash_stmt = f'''SELECT * FROM 
                            (select * from {temp_table_name} src left join 
                            (select row_hash as tgt_row_hash from  
                            {iceberg_table_name} hist where {filter_clause} AND hist.is_current = 1) tgt 
                            on src.row_hash = tgt.tgt_row_hash ) src_cdc
                            WHERE src_cdc.tgt_row_hash is NULL '''
                        
                        print(f"Identifying Changed Records using query : {row_hash_stmt}")
                        src_cdc_df = self.spark.sql(row_hash_stmt)
                        src_cdc_df = src_cdc_df.drop('tgt_row_hash')

                        print(f"Count of Rows Added/Updated in Incoming Data : {src_cdc_df.count()}")
                        src_cdc_df.createOrReplaceTempView(cdc_temp_table_name)

                        # MERGE statement for Incremental SCD Type2 Load Insert and Updates
                        merge_stmt = f"""
                            MERGE INTO {iceberg_table_name} as tgt 
                            using (
                            SELECT *, 'curr' as record_source FROM {cdc_temp_table_name}
                            UNION ALL
                            select src.*, 'hist'  as record_source from {cdc_temp_table_name} src 
                            inner join {iceberg_table_name} as hist
                            ON src.primary_key = hist.primary_key
                            AND hist.is_current = 1
                            AND {filter_clause}
                            ) as src
                            ON src.primary_key = tgt.primary_key
                            AND tgt.is_current = 1 and src.record_source = 'hist'
                            WHEN MATCHED and record_source = 'hist'
                                THEN UPDATE SET tgt.{run_date_column_name} = current_timestamp, 
                                tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                            WHEN NOT MATCHED and record_source = 'curr' then INSERT *
                            """

                        print(f"Executing MERGE Statement for Inserts & Updates : {merge_stmt}")
                        self.spark.sql(merge_stmt)

                        # Updating is_current=0 for the records that were deleted and do no exist
                        # in incoming records to make them inactive in SCD Type2 Table
                        update_stmt = f"""
                            UPDATE {iceberg_table_name} as tgt
                            SET tgt.{run_date_column_name} = current_timestamp, tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                            WHERE tgt.primary_key in 
                            (SELECT hist.primary_key FROM {temp_table_name} src
                            RIGHT JOIN {iceberg_table_name} as hist
                            ON src.primary_key = hist.primary_key
                            WHERE hist.is_current = 1 AND {filter_clause}
                            AND src.primary_key is NULL)
                            """

                        print(f"Executing UPDATE Statement for Deletes : {update_stmt}")
                        self.spark.sql(update_stmt)
                        return True


                    else:
                        print(f"Entity Load is disabled")
    
                        df.createOrReplaceTempView(temp_table_name)

                        #filter out records in incoming dataset which have same row_hash in target
                        row_hash_stmt = f'''SELECT * FROM 
                            (select * from {temp_table_name} src left join 
                            (select row_hash as tgt_row_hash from  
                            {iceberg_table_name} where is_current = 1) tgt 
                            on src.row_hash = tgt.tgt_row_hash ) src_cdc
                            WHERE src_cdc.tgt_row_hash is NULL '''
                        
                        print(f"Identifying Changed Records using query : {row_hash_stmt}")
                        src_cdc_df = self.spark.sql(row_hash_stmt)
                        src_cdc_df = src_cdc_df.drop('tgt_row_hash')

                        print(f"Count of Rows Added/Updated in Incoming Data : {src_cdc_df.count()}")
                        src_cdc_df.createOrReplaceTempView(cdc_temp_table_name)
                    

                        # MERGE statement for Incremental SCD Type2 Load Insert and Updates
                        merge_stmt = f"""
                            MERGE INTO {iceberg_table_name} as tgt 
                            using (
                            SELECT *, 'curr' as record_source FROM {cdc_temp_table_name}
                            UNION ALL
                            select src.*, 'hist'  as record_source from {cdc_temp_table_name} src 
                            inner join {iceberg_table_name} as hist
                            ON src.primary_key = hist.primary_key
                            AND hist.is_current = 1
                            ) as src
                            ON src.primary_key = tgt.primary_key
                            AND tgt.is_current = 1 and src.record_source = 'hist'
                            WHEN MATCHED and record_source = 'hist'
                                THEN UPDATE SET tgt.{run_date_column_name} = current_timestamp, 
                                tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                            WHEN NOT MATCHED and record_source = 'curr' then INSERT *
                            """

                        print(f"Executing MERGE Statement for Inserts & Updates : {merge_stmt}")
                        self.spark.sql(merge_stmt)

                    # Updating is_current=0 for the records that were deleted and do no exist
                    # in incoming records to make them inactive in SCD Type2 Table
                        update_stmt = f"""
                            UPDATE {iceberg_table_name} as tgt
                            SET tgt.{run_date_column_name} = current_timestamp, tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                            WHERE tgt.primary_key in 
                            (SELECT hist.primary_key FROM {temp_table_name} src
                            RIGHT JOIN {iceberg_table_name} as hist
                            ON src.primary_key = hist.primary_key
                            WHERE hist.is_current = 1 AND src.primary_key is NULL)
                            """
                    
                        print(f"Executing UPDATE Statement for Deletes : {update_stmt}")
                        self.spark.sql(update_stmt)
                        return True
                
                elif load_type == LoadType.append:
                    
                    logger.info(f"Appending incoming data")
                    
                    append_stmt = f"""
                    INSERT INTO {iceberg_table_name}
                    SELECT * FROM {temp_table_name}
                    """

                    print(f"Executing INSERT Statement for Appending Data  : {append_stmt}")
                    self.spark.sql(append_stmt)
                    return True
                
                elif load_type == LoadType.cdc:
                    logger.info("Performing CDC Load in SCD Type 2")

                    
                    insert_identifier = cdc_key_operation_dict.insert
                    update_identifier = cdc_key_operation_dict.update
                    delete_identifier = cdc_key_operation_dict.delete

                    if(insert_identifier == update_identifier):
                        print(f'Same identifier for Insert & Update operations : {insert_identifier}')
                    else:
                        print(f'Different identifier for Insert & Update operations')

                    # MERGE to target iceberg table
                    insert_update_df = df.filter(df[cdc_status_column] != delete_identifier)
                    insert_update_df.createOrReplaceTempView(f"{temp_table_name}_modified")

                    logger.info(f"Qualified Insert Update Records for MERGE : {insert_update_df.count()}")

                    merge_stmt = f"""
                        MERGE INTO {iceberg_table_name} as tgt 
                        using (
                        SELECT *, 'curr' as record_source FROM {temp_table_name}_modified
                        UNION ALL
                        select src.*, 'hist'  as record_source from {temp_table_name}_modified src 
                        inner join {iceberg_table_name} as hist
                        ON src.primary_key = hist.primary_key
                        AND hist.is_current = 1
                        ) as src
                        ON src.primary_key = tgt.primary_key
                        AND tgt.is_current = 1 AND src.record_source = 'hist'
                        WHEN MATCHED and src.record_source = 'hist'
                            THEN UPDATE SET tgt.{run_date_column_name} = current_timestamp, 
                            tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                        WHEN NOT MATCHED THEN INSERT *
                        """
                    print(f"Executing MERGE Statement for Inserts & Updates : {merge_stmt}")
                    self.spark.sql(merge_stmt)

                    delete_df = df.filter(df[cdc_status_column] == delete_identifier)
                    delete_df.createOrReplaceTempView(f"{temp_table_name}_deleted")

                    logger.info(f"Qualified Delete Records for Update Satement : {delete_df.count()}")

                    # UPDATE is_current=0 for records deleted in incoming dataset
                    update_stmt = f"""
                        UPDATE {iceberg_table_name} as tgt
                        SET tgt.{run_date_column_name} = current_timestamp, 
                            tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                        WHERE tgt.is_current = 1 AND 
                        tgt.primary_key IN
                        (SELECT src.primary_key FROM {temp_table_name}_deleted src)
                        """
                    
                    print(f"Executing Update Statement for Deletes : {update_stmt}")
                    self.spark.sql(update_stmt)
                    return True
                
                elif load_type == LoadType.incremental_no_delete:
                    logger.info("Performing Incremental No delete Load in SCD Type 2")

                    df.createOrReplaceTempView(temp_table_name)
                    logger.info(f"Qualified Records for MERGE : {df.count()}")

                    merge_stmt = f"""
                        MERGE INTO {iceberg_table_name} as tgt 
                        using (
                        SELECT *, 'curr' as record_source FROM {temp_table_name}
                        UNION ALL
                        select src.*, 'hist'  as record_source from {temp_table_name} src 
                        inner join {iceberg_table_name} as hist
                        ON src.primary_key = hist.primary_key
                        AND hist.is_current = 1
                        ) as src
                        ON src.primary_key = tgt.primary_key
                        AND tgt.is_current = 1 AND src.record_source = 'hist'
                        WHEN MATCHED and src.record_source = 'hist'
                            THEN UPDATE SET tgt.{run_date_column_name} = current_timestamp, 
                            tgt.eff_end_date = current_timestamp, tgt.is_current = 0
                        WHEN NOT MATCHED THEN INSERT *
                        """
                    print(f"Executing MERGE Statement for Inserts & Updates : {merge_stmt}")
                    self.spark.sql(merge_stmt)
                    return True

                else:
                    logger.info("Load Type Configuration is still not supported")
                    os._exit(1)


    def _generate_spark_ddl(self) -> str:
        """Creates the DDL Statement based on the config file"""
        table_schema = self.configuration.table_schema
        if table_schema is None:
            return None

        if isinstance(table_schema, str):
            return table_schema

        all_list = [
            value.column_name + " " + value.column_data_type for value in table_schema
        ]
        return ", ".join(all_list)

    def _generate_iceberg_ddl(self) -> str:
        """Creates the DDL Statement based on the config file"""
        table_schema = self.configuration.table_schema
        if table_schema is None:
            return None

        if isinstance(table_schema, str):
            return table_schema

        all_list = [
            value.column_name + " " + value.column_data_type for value in table_schema
        ]
        return ", ".join(all_list)
    
    @classmethod
    def get_entity_load_attributes(self, configuration: Configuration, 
                                job_args:List, **kwargs) -> Configuration:
        """Sets the Entity Load Attributes in the Configuration"""

        #runner = SparkDataRunner(configuration=configuration, **kwargs)

        if(configuration.target.entity_load):
            print(f"Entity Load is enabled, fetching the entity load job attributes")
            
            if(not isinstance(configuration.target.entity_load_properties, list)):
                configuration.target.entity_load_properties = [configuration.target.entity_load_properties]

            #entity_job_attribute_names = []
            for entity_load_property in configuration.target.entity_load_properties:
                job_attribute_name = entity_load_property.entity_job_attribute_name
                attribute_value = job_args[job_args.index(f"--{job_attribute_name}") + 1]

                entity_load_property.entity_table_attribute_value = attribute_value
        
        else:
            print(f"Entity Load is not enabled")
        
        return configuration
    
    @classmethod
    def read_iceberg_table(self, configuration: Configuration, **kwargs) -> DataFrame:
        """Method that reads the data defined in the configuration file"""
        # Create a new Object
        runner = SparkDataRunner(configuration=configuration, **kwargs)
        
        if configuration.target.iceberg_properties.table_name and configuration.target.iceberg_properties.table_name != "":
            return runner._read_iceberg()
        else:
            raise ValueError("Proper Iceberg Table Name & Database name not provided in configuration")



    @classmethod
    def read_data(self, configuration: Configuration, **kwargs) -> DataFrame:
        """Method that reads the data defined in the configuration file"""
        # Create a new Object
        runner = SparkDataRunner(configuration=configuration, **kwargs)
        source_compute_engine = configuration.source.compute_engine
        if source_compute_engine == "spark":

            if configuration.source.glue_options is not None:
                return runner._read_dynamic_dataframe()

            source_type = configuration.source.spark_options.format.value
            # Reads Parquet Files
            if source_type == SparkFormat.parquet.value:
                return runner._read_parquet()
            else:
                raise ValueError("Format not supported yet by the helper class")
        else:
            raise ValueError("Compute Engine not supported yet by the helper class")

    @classmethod
    def write_data(self, df: DataFrame, configuration: Configuration, **kwargs) -> None:
        """Method that reads the data defined in the configuration file"""
        # Create a new Object
        runner = SparkDataRunner(configuration=configuration, **kwargs)
        target_type_compute_engine = configuration.target.compute_engine

        if target_type_compute_engine == "spark":
            target_type = configuration.target.spark_options.format.value
            drop_duplicates = configuration.target.drop_duplicates

            if(drop_duplicates):
                original_count = df.count()
                
                print(f"Dropping Exact Duplicate Records from the Incoming Data")
                print(f"Record Count before Dropping Duplicates : {original_count}")

                df = df.dropDuplicates()
                distinct_count = df.count()

                print(f"Record Count after Dropping Duplicates : {distinct_count}")
                
                record_diff = original_count - distinct_count
                print(f"Number of Records Dropped : {record_diff}")

            # Writes Iceberg Table
            if target_type == SparkFormat.iceberg.value:
                print("Entering Iceberg")
                return runner._write_iceberg(df)
            else:
                raise ValueError("Format not supported yet by the helper class")
        else:
            raise ValueError("Compute Engine not supported yet by the helper class")
    
    @classmethod
    def create_spark_session(self, configuration:Configuration, **kwargs) -> SparkSession:
        """
        Creates a Spark Session
        
        Returns:
            pyspark.sql.SparkSession: Spark Session Object with required configurations
        """

        runner = SparkDataRunner(configuration=configuration, **kwargs)
        warehouse = (
            configuration.target.iceberg_properties.iceberg_configuration.iceberg_catalog_warehouse
        )
        print(
            f"Setting up the appropriate Spark session for Iceberg using the following warehouse: {warehouse}"
        )

        account_id = get_aws_account_id()
        print(f"Using AWS Account ID: {account_id}")

        spark = SparkSession.builder \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{warehouse}") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold","-1")\
            .config("spark.sql.autoBroadcastJoinThreshold","-1")\
            .config("spark.sql.adaptive.enabled","false")\
            .getOrCreate()

            # .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled","true") \
            # .config("spark.sql.catalog.glue_catalog.glue.id",account_id) \
                
        return spark

# make filter optional
def read_iceberg_sql(spark: SparkSession, database: str, table:str, filter: Optional[str] = None) -> DataFrame:
    """Method that reads Glue Iceberg Table using SQL
        spark: Spark Session
        database: Glue Database Name
        table: Glue Table Name
        filter: Filter Condition for the Glue Table
    """
    if(filter):
        read_sql = f"SELECT * FROM glue_catalog.{database}.{table} WHERE {filter}"
    else:
        read_sql = f"SELECT * FROM glue_catalog.{database}.{table}"

    logger.info(f"Executing Table Read SQL: {read_sql}")
    iceberg_table_df = spark.sql(read_sql)
    return iceberg_table_df





