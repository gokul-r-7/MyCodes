{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_change_deliverable/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}




SELECT
  change_deliverable_actualfinish,
  change_deliverable_actualstart,
  change_deliverable_appchangehours,
  change_deliverable_caid,
  change_deliverable_cainternalid,
  change_deliverable_changecurrent,
  change_deliverable_changehours,
  change_deliverable_changeid,
  change_deliverable_changeinternalid,
  change_deliverable_changestatusname,
  change_deliverable_classificationname,
  change_deliverable_clientdoctypeid,
  change_deliverable_delegate,
  change_deliverable_deliverableid,
  change_deliverable_deliverableinternalid,
  change_deliverable_deliverablename,
  change_deliverable_deliverableowner,
  change_deliverable_earlyfinish,
  change_deliverable_earlystart,
  change_deliverable_gatesetid,
  change_deliverable_gatesetname,
  change_deliverable_gateworkingforecast,
  change_deliverable_latefinish,
  change_deliverable_latestart,
  change_deliverable_plannedfinish,
  change_deliverable_plannedstart,
  change_deliverable_project,
  change_deliverable_projectinternalid,
  change_deliverable_publishedtopowerbi,
  change_deliverable_responsibility,
  change_deliverable_rev,
  change_deliverable_revdate,
  change_deliverable_unappchangehours,
  change_deliverable_wpdoctypeid,
  source_system_name
  ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
  ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dim_snapshot_date	
  ,{{run_date}} as dbt_created_date
  ,{{run_date}} as dbt_updated_date
  ,{{ generate_load_id(model) }} as dbt_load_id
FROM {{ source('curated_ecosys', 'curated_change_deliverable') }}
