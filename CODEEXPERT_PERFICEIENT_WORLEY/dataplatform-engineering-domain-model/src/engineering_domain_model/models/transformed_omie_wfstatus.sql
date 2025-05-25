{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_omie_wfstatus/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    
    id,
    cast(name as varchar(500)) as name,
    cast(description as varchar(4000)) as description,
    closed,
    onhold,
    cast(fill as varchar(500)) as fill,
    cast(stroke as varchar(500)) as stroke,
    primary,
    order,
    updated_by_id,
    mdate,
    created_by_id,
    cdate,
    disabled,
    deleted,
    include_req_team,
    include_res_team,
    include_req_contact,
    include_res_contact,
    cast(execution_date as date) as extracted_date,
	source_system_name,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(100)) as project_code
from
     {{ source('curated_omie', 'curated_wfstatus') }}
