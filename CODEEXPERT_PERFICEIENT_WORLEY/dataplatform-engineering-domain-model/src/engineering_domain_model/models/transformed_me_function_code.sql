{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_me_function_code/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    CAST(id AS decimal(38, 0)) AS id,
    designator,
    description,
    project_code,
    source_system_name,
    cast(execution_date as date) as extracted_date,
	{{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
{{ source('curated_e3d', 'curated_me_function_code') }}
