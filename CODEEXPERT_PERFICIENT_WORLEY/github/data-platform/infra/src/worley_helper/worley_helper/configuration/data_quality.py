from enum import Enum
from typing import Any, Optional, List, Union
from pydantic import BaseModel, Field


class DataQualityFrameworks(str, Enum):
    glue_data_quality = "glue_data_quality"


class GlueQualityConfiguration(BaseModel):
    rules: List[Any] = Field(description="The list of Data Quality checks to apply")
    extras: Optional[Any] = Field(
        default=None, description="Extra parameters required for the selected framework"
    )
    publishing_options: Optional[Any] =  Field(
        default=None, description="Extra parameters required for the selected framework"
    )


class Quality(BaseModel):
    framework: DataQualityFrameworks
    configuration: Union[GlueQualityConfiguration]
