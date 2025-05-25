{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_psr_dext_w_asv_r50039_po/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT PSH.proc_sch_hdr_no, 
       PH.po_id,
       PH.po_no,
       PH.title,
       PES.po_ext_stat_id,
       PH.ver,
       COALESCE(EH.expedite_usr_id, PH.expedite_usr_id) AS expedite_usr_id, 
       CAST(PP.po_gross_value AS bigint) AS po_gross_value, -- Preserve precision
       PHH.def_date AS issued_date,
       PH.cur_id,
       PH.merc_hand_usr_id AS responsible_usr_id,
       PH.deadline,
       SUPPL.suppl_id,
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       CURRENT_DATE AS model_created_date,
       CURRENT_DATE AS model_updated_date,
       UUID() AS model_load_id
FROM {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN {{ source('curated_erm', 'po_item') }} PI 
    ON PI.mat_no = PSH.proj_comp_no
JOIN {{ source('curated_erm', 'po_hdr') }} PH 
    ON PH.po_no = PI.po_no
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} SUPPL 
    ON PH.suppl_no = SUPPL.suppl_no
JOIN {{ source('curated_erm', 'proc_ext_stat') }} PES 
    ON PH.proc_ext_stat_no = PES.proc_ext_stat_no
JOIN {{ source('curated_erm', 'expedite_hdr') }} EH 
    ON EH.po_no = PH.po_no
JOIN (
    SELECT po_no, def_date
    FROM (
        SELECT po_no, def_date, 
               ROW_NUMBER() OVER (PARTITION BY po_no ORDER BY def_date ASC) AS rn
        FROM {{ source('curated_erm', 'po_hdr_hist') }}
    ) sub
    WHERE rn = 1
) PHH ON PHH.po_no = PH.po_no
LEFT JOIN {{ ref('transformed_net_po_price') }} PP 
    ON PP.po_no = PH.po_no
WHERE PSH.is_current = 1 
  AND PI.is_current = 1 
  AND PH.is_current = 1  
  AND PES.is_current = 1 
  AND EH.is_current = 1

  Union

  SELECT PSH.PROC_SCH_HDR_NO, 
       PH.PO_ID,
       PH.PO_NO,
       PH.TITLE,
       PES.PO_EXT_STAT_ID,
       PH.VER,
       COALESCE(EH.EXPEDITE_USR_ID, PH.EXPEDITE_USR_ID) AS EXPEDITE_USR_ID,
       CAST(PP.po_gross_value AS bigint) AS po_gross_value, 
       PHH.DEF_DATE AS ISSUED_DATE,
       PH.CUR_ID,
       PH.MERC_HAND_USR_ID AS RESPONSIBLE_USR_ID,
       PH.DEADLINE,
       SUPPL.SUPPL_ID,
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       CURRENT_DATE AS model_created_date,
       CURRENT_DATE AS model_updated_date,
       UUID() AS model_load_id
FROM {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN {{ source('curated_erm', 'tmr_hdr') }} TH 
    ON COALESCE(PSH.TMR_NO, '') = COALESCE(TH.TMR_NO, '')
JOIN {{ source('curated_erm', 'tmr_item') }} TI 
    ON TH.TMR_NO = TI.TMR_NO
JOIN {{ source('curated_erm', 'tmr_item_po_item') }} TIPI 
    ON TI.TMR_ITEM_NO = TIPI.TMR_ITEM_NO
JOIN {{ source('curated_erm', 'po_item') }} PI 
    ON TIPI.PO_ITEM_NO = PI.PO_ITEM_NO
JOIN {{ source('curated_erm', 'po_hdr') }} PH 
    ON PI.PO_NO = PH.PO_NO
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} SUPPL 
    ON PH.SUPPL_NO = SUPPL.SUPPL_NO
JOIN {{ source('curated_erm', 'proc_ext_stat') }} PES 
    ON PH.PROC_EXT_STAT_NO = PES.PROC_EXT_STAT_NO
JOIN {{ source('curated_erm', 'expedite_hdr') }} EH 
    ON EH.PO_NO = PH.PO_NO
JOIN (
    SELECT po_no, def_date
    FROM (
        SELECT po_no, def_date, 
               ROW_NUMBER() OVER (PARTITION BY po_no ORDER BY def_date ASC) AS rn
        FROM {{ source('curated_erm', 'po_hdr_hist') }}
    ) sub
    WHERE rn = 1
) PHH ON PHH.po_no = PH.po_no
LEFT JOIN {{ ref('transformed_net_po_price') }} PP 
    ON PP.po_no = PH.po_no 
WHERE PSH.is_current = 1 
  AND TH.is_current = 1 
  AND TI.is_current = 1 
  AND TIPI.is_current = 1 
  AND PI.is_current = 1 
  AND PH.is_current = 1 
  AND PES.is_current = 1 
  AND EH.is_current = 1

Union
SELECT PSH.PROC_SCH_HDR_NO, 
       PH.PO_ID,
       PH.PO_NO,
       PH.TITLE,
       PES.PO_EXT_STAT_ID,
       PH.VER,
       COALESCE(EH.EXPEDITE_USR_ID, PH.EXPEDITE_USR_ID) AS EXPEDITE_USR_ID,
       CAST(PP.po_gross_value AS bigint) AS po_gross_value, 
       PHH.DEF_DATE AS ISSUED_DATE,
       PH.CUR_ID,
       PH.MERC_HAND_USR_ID AS RESPONSIBLE_USR_ID,
       PH.DEADLINE,
       SUPPL.SUPPL_ID,
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       CURRENT_DATE AS model_created_date,
       CURRENT_DATE AS model_updated_date,
       UUID() AS model_load_id
FROM {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN {{ source('curated_erm', 'tmr_hdr') }} TH 
    ON COALESCE(PSH.proj_planned_tmr_hdr_no, '') = COALESCE(TH.proj_planned_tmr_hdr_no, '')
JOIN {{ source('curated_erm', 'tmr_item') }} TI 
    ON TH.TMR_NO = TI.TMR_NO
JOIN {{ source('curated_erm', 'tmr_item_po_item') }} TIPI 
    ON TI.TMR_ITEM_NO = TIPI.TMR_ITEM_NO
JOIN {{ source('curated_erm', 'po_item') }} PI 
    ON TIPI.PO_ITEM_NO = PI.PO_ITEM_NO
JOIN {{ source('curated_erm', 'po_hdr') }} PH 
    ON PI.PO_NO = PH.PO_NO
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} SUPPL 
    ON PH.SUPPL_NO = SUPPL.SUPPL_NO
JOIN {{ source('curated_erm', 'proc_ext_stat') }} PES 
    ON PH.PROC_EXT_STAT_NO = PES.PROC_EXT_STAT_NO
JOIN {{ source('curated_erm', 'expedite_hdr') }} EH 
    ON EH.PO_NO = PH.PO_NO
JOIN (
    SELECT po_no, def_date
    FROM (
        SELECT po_no, def_date, 
               ROW_NUMBER() OVER (PARTITION BY po_no ORDER BY def_date ASC) AS rn
        FROM {{ source('curated_erm', 'po_hdr_hist') }}
    ) sub
    WHERE rn = 1
) PHH ON PHH.po_no = PH.po_no
LEFT JOIN {{ ref('transformed_net_po_price') }} PP 
    ON PP.po_no = PH.po_no 
WHERE PSH.is_current = 1 
  AND TH.is_current = 1 
  AND TI.is_current = 1 
  AND TIPI.is_current = 1 
  AND PI.is_current = 1 
  AND PH.is_current = 1 
  AND PES.is_current = 1 
  AND EH.is_current = 1
