{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_project_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


with transformed_erm_project_dim as (
    select 
        proj_no as project_no,
        proj_id as project_code,
        projdescr as project_name,
        country, 
        region,
        worley_project_office,
        sub_region,
        row_number() over (partition by proj_no order by proj_no) as rn,
        {{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id  
    from {{ ref('transformed_erm_user_project_access') }}
)




select
    project_no,
    project_code,
    project_name,
    country, 
    region,
    worley_project_office,
    sub_region,
    model_created_date,
    model_updated_date,
    model_load_id
from transformed_erm_project_dim
where rn=1