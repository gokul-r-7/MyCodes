{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_costtypepcsmapping_base/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

SELECT 
    replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') AS display_name,
    replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') || '|' AS path,
    cost_object_category_value_internal_id,
    cost_object_category_value_id,
    parent_category_id,
    LEGACY_WBS,
    1 AS level
FROM {{ source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') }}
WHERE (parent_category_id = '' OR parent_category_id is null )  AND is_current = 1