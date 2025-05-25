{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_projects/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

SELECT
    project
    ,projectname
    ,projectcode
    ,projectdescription
    ,projectshortname
    ,source_system_name
    ,is_current AS meta_journal_current
    ,CAST(project AS VARCHAR(100)) AS dim_project_rls_key           
    ,CAST(execution_date AS TIMESTAMP) AS dbt_ingestion_date
    ,CAST(eff_end_date AS TIMESTAMP) AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    {{ source('curated_aconex', 'curated_project') }} 
WHERE is_current = 1
