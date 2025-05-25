{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_gate_data/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}



SELECT
  deliverablehierarchypathid,
  deliverablename,
  costobjecthierarchypathid,
  costobjectname,
  relationshiptypetocaid1,
  progressmethodname,
  sequence,
  codeid,
  codename,
  gate,
  actualquantity,
  quantity,
  alternateunits,
  max,
  complete,
  statusname,
  plannedstart,
  plannedfinish,
  actualstart,
  actualfinish,
  forecaststart,
  forecastfinish,
  target1startdate,
  target1finishdate,
  target2startdate,
  target2finishdate,
  progresslinktoid,
  planningactivitylink,
  documentstatus,
  globaldeliverablelibraryabcidname,
  workingforecasttransactioninternalid,
  projectinternalid,
  source_system_name
  ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
  ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dim_snapshot_date	
  ,{{run_date}} as dbt_created_date
  ,{{run_date}} as dbt_updated_date
  ,{{ generate_load_id(model) }} as dbt_load_id
FROM {{ source('curated_ecosys', 'curated_gate_data') }}
