from worley_helper.configuration.project_mapping import (
    SourceTableModel,
    TableFiltersModel,
    TableReadConfigModel,
    FilterConditionModel
)
from typing import List, Optional

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FilterPreprocessor:
    @staticmethod
    def preprocess(source_tables: List[SourceTableModel], 
                  table_filters: TableFiltersModel) -> List[TableReadConfigModel]:
        configs = []
        for table in source_tables:
            try:
                pushdown_filter = FilterPreprocessor._build_pushdown_filter(table, table_filters)
                logger.info(f"Generated pushdown filter for {table.alias}: {pushdown_filter}")
                
                config = TableReadConfigModel(
                    database=table.database,
                    table_name=table.table_name,
                    alias=table.alias,
                    only_current=table.only_current,
                    pushdown_filter=pushdown_filter
                )
                configs.append(config)
            except Exception as e:
                logger.error(f"Error preprocessing filters for {table.database}.{table.table_name}: {str(e)}")
                raise
        return configs
    
    @staticmethod
    def _build_pushdown_filter(table: SourceTableModel, 
                             table_filters: TableFiltersModel) -> Optional[str]:
        """
        Build pushdown filter combining is_current flag and business filters
        
        Args:
            table: Source table information
            table_filters: Business filters for all tables
            
        Returns:
            Combined pushdown filter string or None
        """
        try:
            filter_conditions = []

            # 1. Add is_current filter if required
            if table.only_current:
                filter_conditions.append("is_current = 1")

            # 2. Get and validate business filters for this table
            alias_filters = FilterPreprocessor._get_alias_filters(table.alias, table_filters)
            
            # 3. Build business filter if exists
            if alias_filters:
                business_filter = FilterPreprocessor._build_business_filter(alias_filters)
                if business_filter:
                    # Remove table alias prefix from conditions as it's not needed for pushdown
                    cleaned_filter = FilterPreprocessor._clean_alias_from_filter(
                        business_filter, 
                        table.alias
                    )
                    filter_conditions.append(cleaned_filter)

            # 4. Combine all filters
            if filter_conditions:
                final_filter = " AND ".join(f"({condition})" for condition in filter_conditions)
                logger.debug(f"Built pushdown filter for {table.alias}: {final_filter}")
                return final_filter

            return None

        except Exception as e:
            logger.error(f"Error building pushdown filter for {table.alias}: {str(e)}")
            raise

    @staticmethod
    def _get_alias_filters(alias: str, 
                          table_filters: TableFiltersModel) -> List[FilterConditionModel]:
        """
        Get and validate filters for specific table alias
        
        Args:
            alias: Table alias
            table_filters: All table filters
            
        Returns:
            List of filter conditions for the specified alias
        """
        try:
            return table_filters.filters.get(alias, [])
        except Exception as e:
            logger.error(f"Error getting filters for alias {alias}: {str(e)}")
            raise

    @staticmethod
    def _build_business_filter(filters: List[FilterConditionModel]) -> Optional[str]:
        """
        Build business filter string from filter conditions
        
        Args:
            filters: List of filter conditions
            
        Returns:
            Combined filter string or None
            
        Raises:
            ValueError: If logical operators are not properly specified:
                - First condition should not have logical operator
                - Single condition should not have logical operator
                - Multiple conditions require logical operators from second condition onwards
        """
        try:
            if not filters:
                return None

            # Validate single filter condition
            if len(filters) == 1:
                if filters[0].logical_operator is not None:
                    raise ValueError(
                        f"Single filter condition should not have a logical operator. "
                        f"Found: {filters[0].logical_operator} for condition: {filters[0].condition}"
                    )
                return filters[0].condition

            # Handle multiple filter conditions
            filter_parts = []
            for idx, filter_item in enumerate(filters):
                if idx == 0:
                    if filter_item.logical_operator is not None:
                        raise ValueError(
                            f"First filter condition should not have a logical operator. "
                            f"Found: {filter_item.logical_operator} for condition: {filter_item.condition}"
                        )
                    filter_parts.append(filter_item.condition)
                else:
                    if filter_item.logical_operator is None:
                        raise ValueError(
                            f"Missing logical operator for subsequent condition: {filter_item.condition}"
                        )
                    filter_parts.append(f"{filter_item.logical_operator} {filter_item.condition}")

            return " ".join(filter_parts)

        except Exception as e:
            logger.error(f"Error building business filter: {str(e)}")
            raise

    @staticmethod
    def _clean_alias_from_filter(filter_str: str, alias: str) -> str:
        """
        Remove table alias prefix from filter conditions as it's not needed for pushdown
        
        Args:
            filter_str: Original filter string with aliases
            alias: Table alias to remove
            
        Returns:
            Cleaned filter string without table aliases
        """
        try:
            # Replace patterns like "d.column" with just "column"
            alias_pattern = f"{alias}\\."
            cleaned_filter = re.sub(alias_pattern, "", filter_str)
            logger.debug(f"Cleaned filter from '{filter_str}' to '{cleaned_filter}'")
            return cleaned_filter
            
        except Exception as e:
            logger.error(f"Error cleaning alias from filter: {str(e)}")
            raise