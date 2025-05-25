{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_psr_wp_proc_st_vert_tmrros/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT  
    pch.proc_sch_hdr_no,
    pch.proc_sch_hdr_id,
    th.tmr_no,
    th.tmr_id,
    th.ver,
    th.deadline,
    th.tech_hand_usr_id AS req_originator,
    CAST(th.execution_date AS DATE) AS etl_load_date,
    {{run_date}} AS model_created_date,
    {{run_date}} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'tmr_hdr') }} th
INNER JOIN {{ source('curated_erm', 'proc_sch_hdr') }} pch
    ON (pch.tmr_no = th.tmr_no OR pch.proj_planned_tmr_hdr_no = th.proj_planned_tmr_hdr_no)
WHERE th.proc_int_stat_no = 3
  AND th.is_current = 1
  AND pch.is_current = 1

UNION ALL

SELECT  
    pch.proc_sch_hdr_no,
    pch.proc_sch_hdr_id,
    th.tmr_no,
    th.tmr_id,
    th.ver,
    th.deadline,
    th.tech_hand_usr_id,
    CAST(th.execution_date AS DATE) AS etl_load_date,
    {{run_date}} AS model_created_date,
    {{run_date}} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'tmr_hdr_hist') }} th
INNER JOIN {{ source('curated_erm', 'proc_sch_hdr') }} pch
    ON (pch.tmr_no = th.tmr_no OR pch.proj_planned_tmr_hdr_no = th.proj_planned_tmr_hdr_no)
WHERE (th.tmr_no, th.ver) IN (
    SELECT thh.tmr_no, MAX(thh.ver) 
    FROM {{ source('curated_erm', 'tmr_hdr_hist') }} thh
    WHERE thh.is_current = 1
    GROUP BY thh.tmr_no
)
AND NOT EXISTS (
    SELECT 1
    FROM {{ source('curated_erm', 'tmr_hdr') }} thh
    WHERE thh.tmr_no = th.tmr_no
      AND thh.ver = th.ver
      AND thh.is_current = 1
);
