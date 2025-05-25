{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_dcs_mapping/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}


SELECT
    project_id,
    deliverable_id,
    doc_no,
    projectinternalid
    ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
    ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dim_snapshot_date	
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id,
    source_system_name
FROM
    {{ source('curated_ecosys', 'curated_dcs_mapping') }}
