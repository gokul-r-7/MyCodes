from enum import Enum
from typing import Any, Optional, List, Union, Dict
from pydantic import BaseModel, Field, ValidationError, model_validator


class RunnerDefinition(str, Enum):
    source = "source"
    target = "target"


class SparkFormat(str, Enum):
    csv = "csv"
    json = "json"
    text = "text"
    xml = "xml"
    parquet = "parquet"
    jdbc = "jdbc"
    iceberg = "iceberg"
    spark_table = "spark_table"

class LoadType(str, Enum):
    full_load = "full_load"
    incremental = "incremental"
    append = "append"
    cdc = "cdc"
    incremental_no_delete = "incremental_no_delete"


class SparkPartitions(BaseModel):
    columns: Optional[List[str]] = Field(default=None, description="")


class IcebergConfiguration(BaseModel):
    iceberg_catalog_warehouse: Optional[str] = Field(
        default=None, description="The directory of the Warehouse Directory"
    )
    create_table: Optional[bool] = Field(
        default=False,
        description="Determines if the process should attempt at creating the Iceberg table",
    )
    table_properties: Optional[Dict[str, str]] = Field(
        default=None,
        description="The table properties that need to be applied to Iceberg",
    )


class IcebergProperties(BaseModel):
    database_name: str = Field(description="")
    table_name: str = Field(description="")
    iceberg_configuration: Optional[IcebergConfiguration] = Field(
        default=None, description=""
    )
    partition: Optional[Any] = Field(default=None, description="")


class SparkOptions(BaseModel):
    format: Union[SparkFormat] = Field(
        default=None, description="Specifies the data source format."
    )
    options: Optional[Dict[str, Any]] = Field(
        default={},
        description="The data source format options as per Spark documentation",
    )

class SourceSparkOptions(SparkOptions):
    pass


class TargetSparkOptions(SparkOptions):
    outputMode: Optional[str] = Field(
        default=None, description="Spark Output mode applies only to `target`"
    )
    partition: Optional[SparkPartitions] = Field(default=None, description="")


class GlueDynamicDataFrame(BaseModel):
    connection_type: str
    connection_options: Optional[str] = None
    format: str
    format_options: Optional[Dict[Any, Any]] = None
    transformation_ctx: str


class Sources(BaseModel):
    name: Optional[str] = Field(default=None, description="")
    compute_engine: Optional[str] = Field(default="spark", description="")
    spark_options: Optional[SourceSparkOptions] = Field(
        default=None, description="The Spark IO Source Options"
    )
    glue_options: Optional[GlueDynamicDataFrame] = Field(
        default=None, description="The Glue Job Options"
    )

class CdCOperation(BaseModel):
    insert: str
    update: str
    delete: str

class CDCProperties(BaseModel):
    cdc_operation_column_name: str = Field(description="The name of the cdc status column")
    # {"insert":"i", "update":"u", "delete":"d"}
    cdc_operation_value_map: CdCOperation = Field(description="The value map for the cdc operation & value")

class PrimaryConstraintProperties(BaseModel):
    enforce_primary_constraint: bool = Field(default = None, 
                description="""Boolean Value to indicate if uniqueness of the 
                    Primary Key Attribute should be enforced or not""")
    timestamp_column_name: Optional[str] = Field(default=None, 
                description="""Timestamp Column name to pick the latest record 
                of the Primary Key Attribute""")

class EntityLoadProperty(BaseModel):
    entity_table_attribute_name: str = Field(description="The name of the table attribute based on which data should be loaded")
    entity_job_attribute_name: str = Field(description="The name of the job parameter which has the value of the above mentioned attribute")
    entity_s3_raw_partition_prefix: str = Field(description="The S3 Partition Prefix within Raw S3 Bucket within the dataset which indicates the partition")
    entity_table_attribute_value: str = Field(default=None, description="The value of the table attribute based on which data should be loaded")

class Targets(BaseModel):
    name: Optional[str] = Field(default=None, description="")
    drop_duplicates : Optional[bool] = Field(default=False, description="Dropping exact duplicate records in incoming data")
    compute_engine: Optional[str] = Field(default="spark", description="")
    load_type: LoadType = Field(default="full_load", description="The Load Type for Curated Layer")
    entity_load: Optional[bool] = Field(default=False, description="Indicates if the entity load is enabled or not")
    entity_load_properties: Union[List[EntityLoadProperty],EntityLoadProperty, None] = Field(default=None, description="The Entity Load Properties")
    primary_constraint_properties: Optional[PrimaryConstraintProperties] =  Field(default=None, description="The Primary Constraint Properties")
    cdc_properties: CDCProperties = Field(default=None, description="The CDC Properties")
    spark_options: TargetSparkOptions = Field(description="The Spark IO Target Options")
    iceberg_properties: Optional[IcebergProperties] = Field(
        default=None, description="The Iceberg Properties"
    )

    @model_validator(mode='before')
    @classmethod
    def check_entity_load_properties(cls, values:Any):
        if values.get("entity_load") and not values.get("entity_load_properties"):
            raise ValueError("entity_load_properties must be provided when entity_load is True")
        return values
    
    @model_validator(mode='before')
    @classmethod
    def check_cdc_load_properties(cls, values:Any):
        if values.get("load_type") == LoadType.cdc.value and not values.get("cdc_properties"):
            raise ValueError("cdc_properties must be provided when load_type is cdc")
        return values


class TableSchemaProperties(BaseModel):
    enforce: Optional[bool] = True
    rename_columns: Optional[bool] = True
    primary_key: Union[str, List[str], None] = None



class TableSchemaColumns(BaseModel):
    column_name: str = Field(description="The desired curated column name")
    column_data_type: Optional[str] = Field(
        default=None, description="The desired curated columns type"
    )
    nullable: Optional[bool] = Field(
        default=True, description="Indicates if the column is nullable or not"
    )
    comment: Optional[str] = Field(default=None, description="Desired Column comments")
    data_classification: Optional[str] = Field(
        default=None, description="Desired Data classification"
    )
    # primary_key: Optional[bool] = Field(
    #     default=False, description="Indicates if the fields is a primary key"
    # )
    raw_column_name: Optional[str] = Field(
        default=None, description="The raw column name"
    )


class TableSchema(BaseModel):
    schema_properties: TableSchemaProperties
    columns: Optional[List[TableSchemaColumns]] = None


