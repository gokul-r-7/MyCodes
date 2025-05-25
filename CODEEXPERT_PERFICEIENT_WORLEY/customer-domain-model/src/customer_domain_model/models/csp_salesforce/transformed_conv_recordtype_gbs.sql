{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_conv_recordtype_gbs/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}

SELECT
    id_record_type_gbs,
    record_type_gbs,
    CAST(execution_date as date) as execution_date,
    CAST({{run_date}} as timestamp) as model_created_date,
    CAST({{run_date}} as timestamp) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id	
FROM {{ source('curated_salesforce', 'curated_conv_recordtype_gbs') }}
