{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_project_access/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

select
    'NA' as id,
    'NA' as access_id,
    split_part(prj.userproject_userusername, '@', 1) as user_id,
    prj.userproject_projectidlist as project_id,
    hdr.header_ownerorganizationid as org_id,
    'NA' as active_indicator,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    {{ source('curated_ecosys','curated_ecosys_userproject')}} prj
    inner join {{ source('curated_ecosys', 'curated_ecosys_users') }} usr
        on prj.userproject_userusername = usr.users_loginname
        and prj.is_current = 1
        and usr.is_current = 1
    inner join {{ source('curated_ecosys','curated_header')}} hdr
        on prj.userproject_projectidlist = hdr.header_projectid
        and hdr.is_current = 1
