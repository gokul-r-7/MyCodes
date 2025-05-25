{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spi_tag_category/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cast(tag_category_id as numeric(38, 0)),
    tag_category_name,
    tag_category_desc,
    cast(proj_id as numeric(38, 0)),
    cast(site_id as numeric(38, 0)),
    cast(chg_num as numeric(38, 0)),
    chg_status,
    user_name,
    chg_date,
    'VGCP2' as project_code,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast(execution_date as date) as extracted_date,
    source_system_name
from
    {{ source('curated_spi', 'curated_tag_category') }}