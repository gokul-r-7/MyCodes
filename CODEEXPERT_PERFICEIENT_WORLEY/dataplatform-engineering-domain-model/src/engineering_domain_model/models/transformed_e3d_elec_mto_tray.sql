{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_elec_mto_tray/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

SELECT  
    wbs,
    CASE
        WHEN Position('/' IN site) > 0 THEN Substring(site, 1, Position('/' IN site) - 1)
                    || Substring(site, Position('/' IN site) + 1)
        ELSE site
    END AS SITE,
    CASE
        WHEN Position('/' IN zone) > 0 THEN Substring(zone, 1, Position('/' IN zone) - 1)
                    || Substring(zone, Position('/' IN zone) + 1)
        ELSE zone
    END AS ZONE,
    CASE
        WHEN Position('/' IN cwaytagno) > 0 THEN Substring(cwaytagno, 1, Position('/' IN cwaytagno) - 1)
                    || Substring(cwaytagno, Position('/' IN cwaytagno) + 1)
        ELSE cwaytagno
    END AS CWAY,
    ctray,
    TYPE,
    refno,
    desc,
    length_ft,
    weight_lbs,
    Substring(Split_part(site, '-', 2), 1, 3) AS WBS_NEW,
    ewp_ewp,
    cwa_cwarea,
    cwp_workpackno,
    cwpzone_areacode,
    '' AS WIDTH, --Replace(width, 'in', '') AS WIDTH,      -- Missing Column
    Split_part(site, '-', 2) AS WBS1,
    Substring(zone, 3, 2) AS CWA1,
    Substring(zone, 3, 4) AS CWPZONE1,
    Split_part(zone, '/', 2) AS CWP1,
	'VGCP2'|| '_' ||
    Substring(Split_part(site, '-', 2), 1, 3)|| '_' ||
    cwa_cwarea || '_' ||
    cwpzone_areacode AS PROJECT_CODE_WBS_CWA_CWPZONE, 
    source_system_name,
    'VGCP2' project_code,
	cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM
{{ source('curated_e3d', 'curated_electraywbsrep') }}

