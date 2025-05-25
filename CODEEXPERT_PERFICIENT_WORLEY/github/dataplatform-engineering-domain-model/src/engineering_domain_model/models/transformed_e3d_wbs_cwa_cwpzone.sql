{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_wbs_cwa_cwpzone/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    Split_part(site, '-', 2)  AS WBS,
    Substring(Replace(zone, '/', ''), 2, 2) AS CWA,
    Substring(Replace(zone, '/', ''), 2, 4) AS CWPZone,
    source_system_name,
    'VGCP2' project_code,
	'VGCP2'|| '_' ||
	Split_part(site, '-', 2)|| '_' ||
    Substring(Replace(zone, '/', ''), 2, 2)|| '_' ||
    Substring(Replace(zone, '/', ''), 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE,    
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM  
    (SELECT DISTINCT site,
            zone,
            source_system_name,
            execution_date
            FROM  
            {{ source('curated_e3d', 'curated_vg_e3d_vglequip_sheet') }}
            WHERE zone NOT LIKE '%TEMP%'
            UNION
            SELECT DISTINCT site,
            zone,
            source_system_name,
            execution_date
    FROM  
        {{ source('curated_e3d', 'curated_vg_e3d_vglpipes_sheet') }}
    WHERE zone NOT LIKE '%TEMP%'
    UNION
    SELECT DISTINCT site,
            zone,
            source_system_name,
            execution_date
    FROM  
        {{ source('curated_e3d', 'curated_vg_e3d_vglstruc_sheet') }}
    WHERE zone NOT LIKE '%TEMP%')

