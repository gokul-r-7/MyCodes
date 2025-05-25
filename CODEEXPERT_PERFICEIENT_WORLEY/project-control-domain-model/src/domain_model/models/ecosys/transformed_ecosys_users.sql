{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_users/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}
SELECT
'' as ID,
split_part(users_loginname, '@', 1) as USER_ID,
users_fullname as USER_NAME,
users_loginname as USER_EMAIL,
users_mainmenu as USER_ROLE,
users_costobjectsuperuser as costobjectsuperuser,
'true' as ACTIVE_INDICATOR,
{{run_date}} as created_date,
{{run_date}} as updated_date,
{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_ecosys', 'curated_ecosys_users') }}
