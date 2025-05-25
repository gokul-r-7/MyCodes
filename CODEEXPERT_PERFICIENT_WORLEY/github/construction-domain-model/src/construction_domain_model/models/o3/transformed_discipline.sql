{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_discipline/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
    ) 
}}
   
    
select 
    {{ dbt_utils.generate_surrogate_key(['projectid','disciplinecode','description']) }} as discipline_key,
    CAST(id as INT) as disciplineid,
    CAST(id as INT) as ID,
    CAST(datecreated as TIMESTAMP) as datecreated,
    CAST(datemodified as TIMESTAMP) as datemodified,
    projectid,
    description,
    CAST(modifiedbyuserid as INT) as modifiedbyuserid,
    modifiedbyuser,
    CAST(isdeleted as BOOLEAN) as isdeleted,
    drawingidsjson,
    CAST(createdbyuserid as INT) as createdbyuserid,
    createdbyuser,
    projectinfo,
    disciplinecode,
    description as disciplinedescription,
    CAST(NULL AS STRING) AS projectcode,
    source_system_name,
    is_current,
    cast(execution_date as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_o3', 'curated_disciplines') }}
where is_current = 1
{%- if execution_date_arg != "" %}
    and execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(execution_date as DATE) > (select max(execution_date)  from {{ this }})
    {%- endif %}
{%- endif %} 
 