{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_user/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}

SELECT
    id,
    name,
    firstname,
    lastname,
    username,
    email,
    alias,
    title,
    department,
    companyname,
    lastmodifieddate,
    lastmodifieddate_ts,
    source_system_name,
    eff_start_date,
    primary_key,
    CAST(eff_end_date as DATE) as eff_end_date ,
    is_current,
    CAST(execution_date as date) as execution_date,
    CAST({{run_date}} as timestamp) as model_created_date,
    CAST({{run_date}} as timestamp) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id	
FROM {{ source('curated_salesforce', 'curated_user') }} ur
where ur.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(ur.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}