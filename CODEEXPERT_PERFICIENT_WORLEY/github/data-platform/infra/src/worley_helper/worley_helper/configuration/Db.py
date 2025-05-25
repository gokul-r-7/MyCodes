from pydantic import BaseModel,Field
from typing import Any, Optional, List, Union, Dict


class DBParameter(BaseModel):
    connection_parms : str= Field(description="type of API request")
    partition_column : str= Field(default=None,description="type of API response")
    date_delta_column : str= Field(default=None,description="type of API response")
    ssl_flag : bool= Field(description="SSL Verification for the DB function")
    secret_key: str= Field(description="Secret Key to call the DB which contains the USER and Password")
    db_name_nonprod: str= Field(description="DB Name to be called")
    db_name_prod: str= Field(description="DB Name to be called")
    db_type: str= Field(description="DB Type to be called")


class DBJobParameter(BaseModel):
    bucket_name: str= Field(description="Bucket Name where to persist the api data")
    full_incr: str= Field(description="Flag to identify Incremental vs Full Load")
    input_path: str= Field(default="",description="Input S3 path to understand the data to be sourced from")
    table_prefix: str= Field(description="Table name to be named")
