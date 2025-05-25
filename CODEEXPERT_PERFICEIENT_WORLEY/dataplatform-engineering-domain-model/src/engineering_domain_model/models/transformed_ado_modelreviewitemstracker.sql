{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ado_modelreviewitemstracker/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


Select
    id,
    `work item type` as work_item_type,
    `area path` as area_path,
    `aasigned to` as assigned_to,
    `model review area` as model_review_area,
    `model review session` as model_review_session,
    tagid,
    title,
    tags,
    `date raised` as date_raised,
    `cwa`,
    `owner`,
    `responsibility`,
    `primary discipline` as primary_discipline,
    `secondary discipline` as secondary_discipline,
    `target date` as target_date,
    `state`,
    `worley remarks` as worley_remarks,
    `parent`,
    source_system_name,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    'VGCP2' as project_code

	
from
    {{ source('curated_e3d', 'curated_modelreviewitemstracker') }}
