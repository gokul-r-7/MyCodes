{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_b_p_request__c/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}

SELECT
    id,
    b_p_budget_corporatec,
    b_p_budget_contractc,
    statusc,
    sales_leadc,
    jacobs_sales_leadc,
    lastmodifieddate,
    opportunityc,
    work_hoursc,
    lastmodifieddate_ts,
    source_system_name,
    primary_key,
    is_current,
    eff_start_date,
    eff_end_date,
    CAST(execution_date as date) as execution_date,
    CAST({{run_date}} as timestamp) as model_created_date,
    CAST({{run_date}} as timestamp) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id	
FROM {{ source('curated_salesforce', 'curated_b_p_request__c') }} bp
where bp.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(bp.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}