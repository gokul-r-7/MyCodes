{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spel_modelitem/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    sp_id,
    description,
	CAST(isunchecked AS decimal(38, 0)) AS isunchecked,
	CAST(modelitemtype AS decimal(38, 0)) AS modelitemtype,
	CAST(updatecount AS decimal(38, 0)) AS updatecount,
	CAST(itemstatus AS decimal(38, 0)) AS itemstatus,
    itemtypename,
	source_system_name,
	'VGCP2' project_code,
	cast(execution_date as date) as extracted_date,
	{{run_date}} as model_created_date,
	{{run_date}} as model_updated_date,
	{{ generate_load_id(model) }} as model_load_id
FROM 
{{ source('curated_spel', 'curated_t_modelitem') }}
