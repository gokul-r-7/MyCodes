{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_user_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


with transformed_erm_user_dim as (
    select 
        usr_id as user_id,
        username as user_name,
        email as user_email,
        {{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id,
        row_number() over (partition by usr_id, username, email order by usr_id) as rn
    from {{ ref('transformed_erm_user_project_access') }}

)
select 
    user_id,
    user_name,
    user_email,
    model_created_date,
    model_updated_date,
    model_load_id
from transformed_erm_user_dim
where rn = 1