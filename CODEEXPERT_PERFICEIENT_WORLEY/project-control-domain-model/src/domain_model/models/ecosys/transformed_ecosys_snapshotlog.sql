{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_snapshotlog/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

SELECT 
    snapshotlog_costobjectid,
    snapshotlog_createdate,
	snapshotlog_description,
	snapshotlog_exclude,
	snapshotlog_ownerorganizationid,
	snapshotlog_snapshotid,
	snapshotlog_snapshotintid,
	snapshotlog_transactiontypeid,
	secorg_id,
    cast(execution_date as date),
    source_system_name,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id	
FROM {{ source('curated_ecosys', 'curated_ecosys_snapshotlog') }}
