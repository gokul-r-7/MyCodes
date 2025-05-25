{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'oracle_p6/transformed_p6_project_relationship/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["p6", "v2"]
        )
}}

SELECT 
createdate as createdate,
lag as lag,
lastupdatedate as lastupdatedate,
objectid as objectid,
predecessoractivityobjectid as predecessoractivityobjectid,
predecessorprojectobjectid as predecessorprojectobjectid,
project_id as project_id,
source_system_name as source_system_name,
successoractivityobjectid as successoractivityobjectid,
successorprojectobjectid as successorprojectobjectid,
type as type,
cast(execution_date as date),
{{run_date}} as created_date,
{{run_date}} as updated_date,
{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_p6', 'curated_project_relationship') }}