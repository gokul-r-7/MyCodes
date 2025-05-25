{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_wp_po_tmr/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT DISTINCT 
    th.tmr_id,
    th.rev,            
    ph.po_id,     
    ph.ver AS po_ver,       
    ph.po_no,
    COALESCE(thc.rev_date, th.ver_raised_date) AS tmr_rev_date,
    CAST(th.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'tmr_item_po_item') }} tipi
JOIN {{ source('curated_erm', 'tmr_item') }} ti ON tipi.tmr_item_no = ti.tmr_item_no
JOIN {{ source('curated_erm', 'tmr_hdr') }} th ON ti.tmr_no = th.tmr_no
JOIN {{ source('curated_erm', 'po_item') }} pi ON tipi.po_item_no = pi.po_item_no
JOIN {{ source('curated_erm', 'po_hdr') }} ph ON pi.po_no = ph.po_no
JOIN {{ source('curated_erm', 'proj') }} proj ON ph.proj_no = proj.proj_no
LEFT JOIN {{ source('curated_erm', 'tmr_hdr_cpv') }} thc ON th.tmr_no = thc.tmr_no
LEFT JOIN {{ source('curated_erm', 'tmr_item_po_item_hist') }} tipih 
       ON tipih.tmr_item_po_item_no = tipi.tmr_item_po_item_no 
      AND tipih.po_ver = tipi.po_ver
      AND tipih.is_current = 1
WHERE tipi.is_current = 1
  AND ti.is_current = 1 
  AND th.is_current = 1
  AND thc.is_current = 1
  AND pi.is_current = 1
  AND ph.is_current = 1
  AND proj.is_current = 1
  AND tipih.tmr_item_po_item_no IS NULL 

UNION ALL


SELECT DISTINCT 
    th.tmr_id,
    th.rev,             
    ph.po_id,       
    ph.ver AS po_ver,
    ph.po_no,
    COALESCE(thc.rev_date, th.ver_raised_date) AS tmr_rev_date,
    CAST(th.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'tmr_item_po_item_hist') }} tipi
JOIN {{ source('curated_erm', 'tmr_item_hist') }} ti ON tipi.tmr_item_hist_no = ti.tmr_item_hist_no
JOIN {{ source('curated_erm', 'tmr_hdr_hist') }} th ON ti.tmr_hdr_hist_no = th.tmr_hdr_hist_no
JOIN {{ source('curated_erm', 'po_item_hist') }} pi ON tipi.po_item_hist_no = pi.po_item_hist_no
JOIN {{ source('curated_erm', 'po_hdr_hist') }} ph ON pi.po_hdr_hist_no = ph.po_hdr_hist_no
JOIN {{ source('curated_erm', 'proj') }} proj ON ph.proj_no = proj.proj_no
LEFT JOIN {{ source('curated_erm', 'tmr_hdr_hist_cpv') }} thc ON th.tmr_hdr_hist_no = thc.tmr_hdr_hist_no
WHERE tipi.is_current = 1
  AND ti.is_current = 1 
  AND th.is_current = 1
  AND thc.is_current = 1
  AND pi.is_current = 1
  AND ph.is_current = 1
  AND proj.is_current = 1
