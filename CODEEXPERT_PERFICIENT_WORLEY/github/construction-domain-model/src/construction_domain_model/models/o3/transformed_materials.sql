{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_materials/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
        ) 
}}
 
select
    {{ dbt_utils.generate_surrogate_key(['projectid','catalogcode']) }} as materials_key,
    id,
    projectid,
    projectinfo,
    materialcatalogid,
    catalogcode,
    description,
    materialtype,
    uom,
    size1,
    size2,
    statusid,
    status,
    manufacturerid,
    manufacturer,
    sourceid,
    source,
    installationworkpackageid,
    iwp,
    supplier,
    ponumber,
    receiptnumber,
    ownercompanyid,
    ownercompany,
    location,
    quantity,
    datecreated,
    createdbyuserid,
    createdbyuser,
    datemodified,
    modifiedbyuserid,
    modifiedbyuser,
    constructionworkpackageid,
    constructionworkpackage,
    trackingtag,
    bay,
    bank,
    level,
    datemodified_ts,
    source_system_name,
    primary_key,
    is_current,
    eff_start_date,
    eff_end_date,
    cast(execution_date as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_o3', 'curated_materials') }}
where is_current = 1
{%- if execution_date_arg != "" %}
    and execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(execution_date as DATE) > (select max(execution_date)  from {{ this }})
    {%- endif %}
{%- endif %}
   