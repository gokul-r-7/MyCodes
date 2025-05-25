{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_psr_project_details/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

SELECT
    proj.proj_no,
    proj.proj_id,
    proj.descr,
    cast(proj.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id
FROM
    {{ source('curated_erm', 'proc_sch_hdr') }} hdr,
    {{ source('curated_erm', 'proc_sch_hdr_cpv') }} cpv,
    {{ source('curated_erm', 'proj') }} proj
WHERE
    hdr.proj_no = proj.proj_no
    AND hdr.proc_sch_hdr_no = cpv.proc_sch_hdr_no
  
     
union
SELECT
    proj.proj_no,
    proj.proj_id,
    proj.descr,
    cast(proj.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id
FROM
    {{ source('curated_erm', 'po_hdr') }} 
    join {{ source('curated_erm', 'proj') }} on po_hdr.proj_no = proj.proj_no
    join {{ source('curated_erm', 'expedite_hdr') }} eh on po_hdr.po_no = eh.po_no