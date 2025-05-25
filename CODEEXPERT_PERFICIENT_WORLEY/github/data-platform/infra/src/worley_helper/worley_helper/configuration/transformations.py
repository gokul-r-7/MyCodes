from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List


class AvailableTransforms(str, Enum):
    custom_sql = "custom_sql"
    add_run_date = "add_run_date"
    hash_column = "hash_column"
    rename_columns = "rename_columns"
    change_data_types = "change_data_types"
    select_columns_from_config_file = "select_columns_from_config_file"


class SQLTransform(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.custom_sql.value}"
    )
    sql: str = Field(description=" The custom SQL to run")
    temp_view_name: str = Field(
        description="The name to give the temp view. Use this if you want to concatenate SQL queries. Note this only applies to one Spark Session"
    )


class AddRunDate(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.add_run_date.value}"
    )
    column_name: Optional[str] = Field(
        description="The name to give the resulting column"
    )
    date_format: Optional[str] = Field(
        description="The desired date format. It's always set to UTC time"
    )


class HashColumn(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.hash_column.value}"
    )
    drop_original: Optional[bool] = Field(
        default=False,
        description="Flag to indicate if the original column needs to be dropped",
    )
    column_names: Optional[List[str]] = Field(
        description="The names of the columns to hash. Please note that this column needs to be present in the dataframe"
    )


class RenameColumn(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.rename_columns.value}"
    )
    rename_column: Optional[bool] = Field(
        description="Flag to run the process. True runs the column transformation"
    )


class ChangeDataTypes(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.change_data_types.value}"
    )
    change_types: Optional[bool] = Field(
        description="Flag to run the process. True runs the column transformation"
    )


class SelectColsConfigFile(BaseModel):
    transform: str = Field(
        description=f"The name of the transform to run, use: {AvailableTransforms.select_columns_from_config_file.value}"
    )
    select_columns: Optional[bool] = Field(
        default=True,
        description="Flag to run the process. True runs the column transformation",
    )
