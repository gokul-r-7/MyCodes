{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_elec_mto_equip/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    wbs,
    site,
    zone,
    equipment,
    type,
    refno,
    description,
    EFT,
    NFT,
    UFT,
    substring(split_part(site, '-',2),1,3) AS wbs_new,
    source_system_name,
    'VGCP2' project_code,
    cast(execution_date as date) as extracted_date,
	'VGCP2'|| '_' ||
    substring(split_part(site, '-',2),1,3) AS PROJECT_CODE_WBS,
    CASE WHEN REGEXP_INSTR(RIGHT(ZONE, 4), '[0-9]') > 0 THEN 'UNKNOWN'
    ELSE SUBSTRING(ZONE, LENGTH(ZONE) - 3, 4)
    END AS Equipment_Category,    
	{{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
{{ source('curated_e3d', 'curated_elecequiwbsrep') }}
