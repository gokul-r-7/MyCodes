{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_deliverable_source/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select 
    deliverable_source_budgetid as budgetid,
	deliverable_source_caid as caid,
	deliverable_source_cainternalid as cainternalid,
	deliverable_source_changeid as changeid,
	deliverable_source_changetypeidname as changetypeidname,
	deliverable_source_delid as delid,
	deliverable_source_delinternalid as delinternalid,
	deliverable_source_description as description,
	deliverable_source_pbbudget as pbbudget,
	deliverable_source_pbchanges as pbchanges,
	deliverable_source_pbchangesapp as pbchangesapp,
	deliverable_source_pbchangesunapp as pbchangesunapp,
	deliverable_source_pbcurrent as pbcurrent,
	deliverable_source_pborigapproved as pborigapproved,
	deliverable_source_project as project,
	deliverable_source_projectinternalid as projectinternalid,
	deliverable_source_systemdate as systemdate,
	deliverable_source_transactiontypeid as transactiontypeid,
    execution_date,
    source_system_name,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM {{ source('curated_ecosys','curated_deliverable_source') }}