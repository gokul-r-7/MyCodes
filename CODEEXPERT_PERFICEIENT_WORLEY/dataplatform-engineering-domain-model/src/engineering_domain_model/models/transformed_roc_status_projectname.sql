{%- set run_date = "CURRENT_TIMESTAMP()" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_roc_status_projectname/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select  
    project_id,
    maturity_step,
    activity,
    cast(step_percent as decimal(38,2)) *100 as step_percent, 
    cast(cum_percent as decimal(38,2)) *100 as cum_percent, 
    phase,
    source_system_name,
    'VGCP2' project_code,
    discipline,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from 
    {{ source('curated_e3d', 'curated_roc_status_projectname') }}

