{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spel_codelists/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    CAST(codelist_number AS decimal(38, 0)) AS codelist_number,
    CAST(codelist_index AS decimal(38, 0)) AS codelist_index,
    codelist_text,
    codelist_short_text,
    CAST(codelist_constraint AS decimal(38, 0)) AS codelist_constraint,
    CAST(codelist_sort_value AS decimal(38, 0)) AS codelist_sort_value,
    CAST(codelist_entry_disabled AS decimal(38, 0)) AS codelist_entry_disabled,
	source_system_name,
	'VGCP2' project_code,
	cast(execution_date as date) as extracted_date,
	{{run_date}} as model_created_date,
	{{run_date}} as model_updated_date,
	{{ generate_load_id(model) }} as model_load_id
FROM 
{{ source('curated_spel', 'curated_codelists') }}
