from typing import Union, List, Optional
from pydantic import BaseModel
from worley_helper.configuration.io import Sources, Targets, TableSchema
from worley_helper.configuration.http_client import ApiParameter,JobParameter
from worley_helper.configuration.Db import DBParameter,DBJobParameter
from worley_helper.configuration.data_quality import Quality
from worley_helper.configuration.ingest import SMBConfiguration
from worley_helper.configuration.transformations import (
    AddRunDate,
    SQLTransform,
    HashColumn,
    RenameColumn,
    ChangeDataTypes,
    SelectColsConfigFile,
)

class Configuration(BaseModel):
    SourceSystemId: str
    MetadataType: str
    source: Sources
    target: Targets
    transforms: Optional[
        List[
            Union[
                AddRunDate,
                SQLTransform,
                HashColumn,
                RenameColumn,
                ChangeDataTypes,
                SelectColsConfigFile,
            ]
        ]
    ] = None
    table_schema: Optional[TableSchema] = None
    quality: Optional[Quality] = None

class EcosysConfiguration(BaseModel):
    SourceSystemId: str
    MetadataType: str
    name : str
    api_parameter: ApiParameter
    job_parameter: JobParameter
    is_active : str

class DbConfiguration(BaseModel):
    SourceSystemId: str
    MetadataType: str
    name : str
    db_parameter: DBParameter
    job_parameter: DBJobParameter
    is_active : str

class SMBIngestConfiguration(BaseModel):
    SourceSystemId: str
    MetadataType: str
    is_active: bool
    aws_region: Optional[str] = None
    Pattern: str
    smb_configuration: SMBConfiguration