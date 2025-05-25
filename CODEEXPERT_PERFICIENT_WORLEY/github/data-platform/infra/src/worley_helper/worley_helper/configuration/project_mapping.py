from typing import List, Dict, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator, RootModel
from enum import Enum
import re

class LogicalOperator(str, Enum):
    AND = "AND"
    OR = "OR"

class SourceTableModel(BaseModel):
    database: str = Field(..., description="Database name in Glue Catalog")
    table_name: str = Field(..., description="Table name in Glue Catalog")
    alias: str = Field(..., description="Alias used in SQL queries")
    only_current: bool = Field(default=True, description="Flag to indicate if only current records are needed")

    @field_validator('database')
    @classmethod
    def validate_database(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('Database name must be a non-empty string')
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
            raise ValueError('Database name can only contain alphanumeric characters and underscore')
        return v

    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('Table name must be a non-empty string')
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
            raise ValueError('Table name can only contain alphanumeric characters and underscore')
        return v

    @field_validator('alias')
    @classmethod
    def validate_alias(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('Alias must be a non-empty string')
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError('Alias must start with a letter and contain only alphanumeric characters and underscore')
        if len(v) > 30:
            raise ValueError('Alias length should not exceed 30 characters')
        return v.lower()

class FilterConditionModel(BaseModel):
    condition: str = Field(..., description="SQL filter condition")
    logical_operator: Optional[str] = Field(None, description="Logical operator (AND/OR) for combining conditions")

    @field_validator('condition')
    @classmethod
    def validate_condition(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('Condition must be a non-empty string')
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT']
        for keyword in dangerous_keywords:
            if keyword.upper() in v.upper():
                raise ValueError(f'Condition contains forbidden keyword: {keyword}')
        return v

    # @field_validator('logical_operator')
    # @classmethod
    # def validate_logical_operator(cls, v: Optional[str]) -> Optional[str]:
    #     if v is not None and v not in [op.value for op in LogicalOperator]:
    #         raise ValueError(f'Logical operator must be one of: {", ".join([op.value for op in LogicalOperator])}')
    #     return v

class TableFiltersModel(RootModel):
    """Model for table-specific filters with dynamic table aliases"""
    # Use arbitrary_types_allowed to allow dynamic keys
    root: Dict[str, List[FilterConditionModel]]

    @model_validator(mode='after')
    def validate_filters(self) -> 'TableFiltersModel':
        """
        Validates that:
        1. Only valid table aliases are used
        2. Filter conditions are properly structured
        3. Logical operators follow the rules:
           - For single condition: logical_operator should be "null" or None
           - For multiple conditions: 
             - First condition must have "AND" or "OR" (not "null" or None)
             - Middle conditions must have "AND" or "OR"
             - Last condition can have "null" or None
        """
        try:
            if not hasattr(self, '_table_aliases'):
                return self

            valid_aliases = self._table_aliases
            filter_aliases = set(self.root.keys())

            # Check for invalid aliases
            invalid_aliases = filter_aliases - valid_aliases
            if invalid_aliases:
                raise ValueError(f"Filter contains invalid table aliases: {invalid_aliases}")

            # Validate each filter condition
            for alias, conditions in self.root.items():
                if not conditions:
                    raise ValueError(f"No filter conditions provided for alias '{alias}'")
                
                # Validate logical operators based on number of conditions
                num_conditions = len(conditions)
                
                if num_conditions == 1:
                    # For single condition, logical operator should be "null" or None
                    if conditions[0].logical_operator not in [None, "null"]:
                        raise ValueError(
                            f"For single condition in alias '{alias}', logical_operator "
                            f"should be 'null' or None, got '{conditions[0].logical_operator}'"
                        )
                else:
                    # For multiple conditions
                    for i, condition in enumerate(conditions):
                        # First condition must have AND or OR (not null)
                        if i == 0:
                            if condition.logical_operator in [None, "null"]:
                                raise ValueError(
                                    f"For multiple conditions in alias '{alias}', first condition "
                                    f"cannot have 'null' logical operator"
                                )
                            if condition.logical_operator not in ["AND", "OR"]:
                                raise ValueError(
                                    f"First condition for alias '{alias}' must have "
                                    f"logical_operator as 'AND' or 'OR', got '{condition.logical_operator}'"
                                )
                        # Middle conditions must have AND or OR
                        elif i < num_conditions - 1:
                            if condition.logical_operator not in ["AND", "OR"]:
                                raise ValueError(
                                    f"Middle condition {i+1} for alias '{alias}' must have "
                                    f"logical_operator as 'AND' or 'OR', got '{condition.logical_operator}'"
                                )
                        # Last condition can have null/None or AND/OR
                        else:
                            if condition.logical_operator not in ["AND", "OR", None, "null"]:
                                raise ValueError(
                                    f"Last condition for alias '{alias}' must have "
                                    f"logical_operator as 'AND', 'OR', or 'null', got '{condition.logical_operator}'"
                                )

            return self

        except Exception as e:
            raise ValueError(f"Filter validation error: {str(e)}")

    def dict(self, *args, **kwargs):
        """Custom dict method to handle the root structure"""
        return self.root

class ProjectMappingMetadataModel(BaseModel):
    project_id: Union[str, List[str]] = Field(..., description="Project ID or List of project IDs")
    target_table: str = Field(..., description="Target Transformed Table Name")
    source_table_info: List[SourceTableModel] = Field(..., description="List of source curated tables")
    source_business_filter: TableFiltersModel = Field(..., description="Business filters for source tables")
    data_load_sql: str = Field(..., description="SQL transformation logic")
    is_active: bool = Field(default=True, description="Flag to enable/disable the load")

    @field_validator('project_id')
    @classmethod
    def validate_project_id(cls, v: Union[str, List[str]]) -> Union[str, List[str]]:
        if isinstance(v, str):
            if not v:
                raise ValueError('Project ID must not be empty')
        elif isinstance(v, list):
            if not v:
                raise ValueError('Project ID list must not be empty')
            for pid in v:
                if not isinstance(pid, str) or not pid:
                    raise ValueError('All project IDs must be non-empty strings')
        else:
            raise ValueError('Project ID must be either a string or list of strings')
        return v

    @field_validator('target_table')
    @classmethod
    def validate_target_table(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('Target table must be a non-empty string')
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
            raise ValueError('Target table name can only contain alphanumeric characters and underscore')
        return v

    @field_validator('data_load_sql')
    @classmethod
    def validate_sql(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError('SQL must be a non-empty string')
        
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT']
        for keyword in dangerous_keywords:
            if keyword.upper() in v.upper():
                raise ValueError(f'SQL contains forbidden keyword: {keyword}')
        
        required_keywords = ['SELECT', 'FROM']
        for keyword in required_keywords:
            if keyword.upper() not in v.upper():
                raise ValueError(f'SQL must contain {keyword} statement')
        
        return v

    @model_validator(mode='after')
    def validate_model(self) -> 'ProjectMappingMetadataModel':
        # Validate source tables
        aliases = [table.alias for table in self.source_table_info]
        if len(aliases) != len(set(aliases)):
            raise ValueError('Duplicate table aliases found')

        # Validate filter aliases against source tables
        valid_aliases = {table.alias for table in self.source_table_info}
        filter_aliases = set(self.source_business_filter.root.keys())
        
        invalid_aliases = filter_aliases - valid_aliases
        if invalid_aliases:
            raise ValueError(f'Filter contains invalid table aliases: {invalid_aliases}')

        return self


# New TableReadConfig model
class TableReadConfigModel(BaseModel):
    database: str = Field(..., description="Database name in Glue Catalog")
    table_name: str = Field(..., description="Table name in Glue Catalog")
    alias: str = Field(..., description="Alias used in SQL queries")
    only_current: bool = Field(default=True, description="Flag to indicate if only current records are needed")
    pushdown_filter: Optional[str] = Field(None, description="Preprocessed pushdown filter")
