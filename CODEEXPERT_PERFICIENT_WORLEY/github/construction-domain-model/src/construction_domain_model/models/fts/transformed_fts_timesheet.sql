{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
  
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=  target.location ~ 'transformed_fts_timesheet/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["fts"]
        ) 
}}
   
select
    SITE_CODE as sitecode,
    cast(EMPLOYEE_NUMBER as integer) as employeenumber,
    CLASS as class,
    CREW_CODE as crewcode,
    TIMECARD_DATE  as timecarddate,
    WEEK_ENDING_DATE as weekendingdate,
    PROJECT_NUM as projectid,
    TASK_NUMBER as tasknumber,
    TASK_NAME as taskname,
    cast(ST_HOURS as float) as spenthours,
    cast(OT_HOURS as float) as overtimehours,
    cast(DT_HOURS as float) as dutyhours,
    IWP as iwpsid,
    SOURCE_SYSTEM_NAME as source_system_name,
    cast(execution_date as DATE) as execution_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_fts', 'curated_fts_timesheet') }}
{%- if execution_date_arg != "" or is_incremental() %}
    where
    {%- if execution_date_arg != "" %}
        cast(execution_date as DATE) >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            cast(execution_date as DATE) > (select max(execution_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
{%- endif %}
  