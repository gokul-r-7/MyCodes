{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'oracle_p6/transformed_p6_activitycode/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["p6", "v2"]
        )
}}

select 
    '' as activitycode_pk,
    codeconcatname,
    codetypename,
    cast(codetypeobjectid as int) as codetypeobjectid,
    codetypescope,
    codevalue,
    color,
    cast(createdate as date) as createdate,
    createuser,
    description,
    cast(lastupdatedate as date) as lastupdatedate,
    lastupdateuser,
    cast(objectid as int) as objectid,
    cast(parentobjectid as int) as parentobjectid,
    cast(projectobjectid as int) as projectobjectid,
    sequencenumber,
    '' as etl_job_status_pk,
    '' as active_indicator,
    '' as created_by,
    '' as updated_by,
    execution_date as extracted_date,
    'VGCP2' as project_code,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM
    {{ source('curated_p6','curated_project_activitycode') }}
union all
select
    '' as activitycode_pk,
    codeconcatname,
    codetypename,
    cast(codetypeobjectid as int) as codetypeobjectid,
    codetypescope,
    codevalue,
    color,
    cast(createdate as date) as createdate,
    createuser,
    description,
    cast(lastupdatedate as date) as lastupdatedate,
    lastupdateuser,
    cast(objectid as int) as objectid,
    cast(parentobjectid as int) as parentobjectid,
    cast(projectobjectid as int) as projectobjectid,
    sequencenumber,
    '' as etl_job_status_pk,
    '' as active_indicator,
    '' as created_by,
    '' as updated_by,
    execution_date as extracted_date,
    'VGCP2' as project_code,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    {{ source('curated_p6','curated_activitycode') }}




