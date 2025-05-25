{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_deliverable_gate/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select 
    deliverable_gate_actualfinish as actualfinish,
	deliverable_gate_actualstart as actualstart,
	deliverable_gate_cainternalid as cainternalid,
	deliverable_gate_codeid as codeid,
	deliverable_gate_codename as codename,
	deliverable_gate_complete as complete,
	deliverable_gate_costobjectid as costobjectid,
	deliverable_gate_countofworkingforecasttransactions as countofworkingforecasttransactions,
	deliverable_gate_currentbudgethours as currentbudgethours,
	deliverable_gate_dela60egateprogress as dela60egateprogress,
	deliverable_gate_dela60ewftdelgateincremental as dela60ewftdelgateincremental,
	deliverable_gate_deliverableid as deliverableid,
	deliverable_gate_deliverableinternalid as deliverableinternalid,
	deliverable_gate_forecastfinish as forecastfinish,
	deliverable_gate_forecaststart as forecaststart,
	deliverable_gate_gate as gate,
	deliverable_gate_max as max,
	deliverable_gate_plannedfinish as plannedfinish,
	deliverable_gate_plannedstart as plannedstart,
	deliverable_gate_planningactivitylink as planningactivitylink,
	deliverable_gate_progresslinktoid as progresslinktoid,
	deliverable_gate_publishedtopowerbi as publishedtopowerbi,
	deliverable_gate_sequence as sequence,
    execution_date,
    source_system_name,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM {{ source('curated_ecosys','curated_deliverable_gate') }}