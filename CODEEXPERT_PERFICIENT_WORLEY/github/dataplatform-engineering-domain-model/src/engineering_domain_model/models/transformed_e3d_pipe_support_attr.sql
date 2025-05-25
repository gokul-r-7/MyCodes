{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_pipe_support_attr/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    NAME,
    type,
    lock,
    owner,
    project,
    workpack_no,
    isometric,
    suppo,
    support_type,
    bore_inch,
    tag as tag_name,
    profile,
    length_mm,
    quantity,
    client_doc_num,
    drawing_no,
    pid_line_no,
    zone,
    site,
    Split_part(coalesce(site, ''), '-', 2) AS WBS,
    Substring(Replace(zone, '/', ''), 2, 2) AS CWA,
    Substring(Replace(zone, '/', ''), 2, 4) AS CWPZONE,    
    'VGCP2'|| '_' ||
    Split_part(coalesce(site, ''), '-', 2) || '_' ||
    Substring(Replace(zone, '/', ''), 2, 2)|| '_' ||
    Substring(Replace(zone, '/', ''), 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE,
    cast(execution_date as date) as extracted_date,
    source_system_name,
    'VGCP2' project_code,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
    {{ source('curated_e3d', 'curated_vg_e3d_vglpipesupport_sheet') }}

