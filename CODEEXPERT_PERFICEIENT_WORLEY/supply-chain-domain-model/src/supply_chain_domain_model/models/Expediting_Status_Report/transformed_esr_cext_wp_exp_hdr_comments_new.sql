{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_wp_exp_hdr_comments_new/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


  SELECT  expedite_hdr_no,
          proj_no,
          po_no,
          comnt,
	   cast(execution_date as date) as etl_load_date,
       {{run_date}} as model_created_date,
       {{run_date}} as model_updated_date,
       {{ generate_load_id(model) }} as model_load_id		
FROM 
(
SELECT 
    eh.expedite_hdr_no,
    CONCAT_WS(' ', ehl.upd_date, COALESCE(ehl.def_usr_id, ''), ':', ehl.txt) AS comnt, 
    ehl.upd_date,
    ph.proj_no,
    ph.po_no,
    eh.execution_date
FROM {{ source('curated_erm', 'expedite_hdr') }} eh
JOIN {{ source('curated_erm', 'po_hdr') }} ph 
    ON eh.po_no = ph.po_no
LEFT JOIN (
    SELECT 
        expedite_hdr_no,
        upd_date,
        def_usr_id,
        txt,
        ROW_NUMBER() OVER (PARTITION BY expedite_hdr_no ORDER BY upd_date DESC) AS rn
    FROM {{ source('curated_erm', 'expedite_hdr_log') }}
    WHERE txt NOT LIKE '**ARCH%'
      AND is_current = 1
) ehl 
    ON eh.expedite_hdr_no = ehl.expedite_hdr_no 
    AND ehl.rn = 1  -- Select only the most recent log entry per expedite_hdr_no

WHERE ph.po_no NOT IN (121378, 55979, 56004, 56045, 56047)
  AND eh.is_current = 1
  AND ph.is_current = 1

ORDER BY ehl.upd_date DESC
)