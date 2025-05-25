from pydantic import BaseModel, Field
from typing import Any, Optional, List, Union, Dict
class SMBConfiguration(BaseModel):
    host_url: str = Field(description="The Host URL to connect to the SMB server")
    host_port: int = Field(description="The SMB Port Number (usually 445)")
    secret_credentials: str = Field(description="Secret Key to call the API which contains the SMB username and password")
    destination_bucket: str = Field(description="The S3 bucket where the files will be uploaded")
    destination_folder: str = Field(description="The S3 bucket where file static path")
    kms_key_arn: str = Field(description="KMS key ARN to encrypt files in the S3 bucket")
    file_archive_flag: bool = Field(description="Flag to archive the files after processing")
    file_delete_flag: bool = Field(description="Flag to delete the files after processing")
    archive_folder: Optional[str] = "" 
    batch_run_start_time_str: Optional[str] = "" 
    source: Optional[List[Dict[str, str]]] = [] 

    