{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_org_access/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

with cte as (
    SELECT
        distinct '' as id,
        '' as access_id,
        split_part(org.usersecorg_userusername, '@', 1) as user_id,
        org.usersecorg_secorgidlist as org_id,
        true as active_indicator,
        '' as created_date,
        '' as updated_date
    FROM
	{{ source('curated_ecosys', 'curated_ecosys_usersecorg') }} org,
	{{ source('curated_ecosys', 'curated_ecosys_users') }} usr
    WHERE
        org.usersecorg_userusername = usr.users_loginname
        and usr.users_costobjectsuperuser = 'false'
        and org.is_current = 1
    union
    SELECT
        distinct '-1' as id,
        '' as access_id,
        split_part(usr1.users_loginname, '@', 1) as user_id,
        '1' as org_id,
        TRUE as active_indicator,
        '' as created_date,
        '' as updated_date
    FROM
        {{ source('curated_ecosys', 'curated_ecosys_users') }} usr1
    where
        usr1.users_costobjectsuperuser = 'true'
)
select
    id,
    access_id,
    user_id,
    org_id,
    cast(active_indicator as BOOLEAN) as active_indicator,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    cte