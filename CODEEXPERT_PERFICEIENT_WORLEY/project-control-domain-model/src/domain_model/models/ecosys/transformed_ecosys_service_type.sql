{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_service_type/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

SELECT 
    `header_servicetypeidname` AS header_servicetypeidname,
    `header_servicetypeidname` AS service_type,
    TRIM(SUBSTRING_INDEX(`header_servicetypeidname`, '-', -1)) AS service_type_description,
    {{ run_date }} as created_date,
    {{ run_date }} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM {{ source('curated_ecosys', 'curated_header') }}
WHERE 
    header_servicetypeidname IS NOT NULL
    AND is_current = 1
ORDER BY 
    `header_servicetypeidname`


