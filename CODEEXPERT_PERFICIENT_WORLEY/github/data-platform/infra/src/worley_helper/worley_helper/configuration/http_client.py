from pydantic import BaseModel,Field
from typing import Any, Optional, List, Union, Dict



class ApiHeader(BaseModel):
    content_type: str= Field(description="Content Type to be defined for the API Function")

class DynamicApiHeader(BaseModel):
    Authorization :  str= Field(default=None,description="Authorization to be defined for the API Function")


class ApiParameter(BaseModel):
    api_method : str= Field(description="type of API request")
    api_response_type : str= Field(description="type of API response")
    endpoint: str= Field(description="URL to call the API function")
    api_retry: int= Field(description="Retry count for the API function")
    api_ssl_verify : bool= Field(description="SSL Verification for the API function")
    api_timeout: int= Field(description="TimeOut Parameter to be defined for the API Function")
    secret_key: str= Field(description="Secret Key to call the API which contains the USER and Password")
    api_auth_type: str= Field(description="Authentication Type to be defined for the API Function")
    api_headers : ApiHeader
    dynamic_api_headers : DynamicApiHeader
    api_query_params: str= Field(description="Initial Parameter to call the API")
    dynamic_api_query_param: Optional [str] = Field(default=None,description="Dynamic Parameter to call the API")
    api_body : str= Field(default=None,description="Body to call the API")



class JobParameter(BaseModel):
    bucket_name: str= Field(description="Bucket Name where to persist the api data")
    full_incr: str= Field(description="Flag to identify Incremental vs Full Load")
    input_path: str= Field(default="",description="Input S3 path to understand the data to be sourced from")
    table_prefix: str= Field(description="Table name to be named")
    secorg_id : str= Field(description="Security Organization ID")
