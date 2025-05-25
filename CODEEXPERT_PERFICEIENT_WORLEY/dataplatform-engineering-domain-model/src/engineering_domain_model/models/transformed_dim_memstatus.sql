{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_dim_memstatus/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select  
    disc,
    cast(status as decimal(38,0)) as status, 
    cast(percent  as decimal(38,0)) as percent_value, 
    cast(cumpercent as decimal(38,0)) as cumpercent, 
    description,
    project_code,
    source_system_name,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from 
    {{ source('curated_e3d', 'curated_dim_memstatus') }}

