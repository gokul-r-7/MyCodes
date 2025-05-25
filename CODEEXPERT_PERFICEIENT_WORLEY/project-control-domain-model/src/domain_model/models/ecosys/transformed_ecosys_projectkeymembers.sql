{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_projectkeymembers/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

SELECT
        projectkeymembers_costobjectid as costobjectid,
        cast(projectkeymembers_enddate as date) as  enddate,
        projectkeymembers_internalid as internalid,
        projectkeymembers_projectinternalid as projectinternalid,
        projectkeymembers_projectkeymembertypeid as projectkeymembertypeid,
        projectkeymembers_resourceid as resourceid,
        cast(projectkeymembers_startdate as date) as startdate,
        execution_date ,
        source_system_name  ,
        {{run_date}} as created_date,
		{{run_date}} as updated_date,
		{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_ecosys', 'curated_ecosys_projectkeymembers') }}


    