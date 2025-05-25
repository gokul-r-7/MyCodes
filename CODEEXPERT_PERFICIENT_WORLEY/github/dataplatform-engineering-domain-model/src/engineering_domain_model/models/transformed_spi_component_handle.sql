{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spi_component_handle/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cmpnt_handle_id,
    proj_id,
    site_id,
    chg_num,
    plant_id,
    unit_id,
    chg_status,
    user_name,
    cast(chg_date as date) as chg_date,
    cmpnt_handle_name,
    cmpnt_handle_desc,
    area_id,
    source_system_name,
    'VGCP2' as project_code,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast(execution_date as date) as extracted_date
from
    {{ source('curated_spi', 'curated_component_handle') }}