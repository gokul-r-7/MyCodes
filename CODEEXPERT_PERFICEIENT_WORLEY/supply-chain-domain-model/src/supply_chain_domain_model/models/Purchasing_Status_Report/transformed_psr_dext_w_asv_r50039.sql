{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_psr_dext_w_asv_r50039/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT PSH.proc_sch_hdr_no, 
      NULLIF(PPTH.proj_planned_tmr_hdr_id, TH.tmr_id) AS tmr_mr_id,
      NULLIF(PPTH.title, TH.title)                    AS title,
      PES.tmr_ext_stat_id,
      TH.ver,
      TH.rev,
      CASE 
          WHEN TH.workflow_purpose_no = 1 THEN 'Engineering'  
          WHEN TH.workflow_purpose_no = 2 THEN 'Enquiry Only' 
          WHEN TH.workflow_purpose_no = 3 THEN 'Enquiry And Purchase' 
          WHEN TH.workflow_purpose_no = 4 THEN 'Purchase' 
          WHEN TH.workflow_purpose_no = 5 THEN 'Cancellation' 
          ELSE NULL 
      END AS issue_purpose,
      TH.issued_date,
      CAST(NULL AS STRING) AS value_quan,  -- Fix for NULLTYPE issue
      CAST(NULL AS STRING) AS unit_cur_id, -- Fix for NULLTYPE issue
      TI.deadline,
      TH.tech_hand_usr_id  AS responsible_usr_id,
      suppl.suppl_id,
      CAST(PSH.execution_date AS DATE) AS etl_load_date,
      {{run_date}} AS model_created_date,
      {{run_date}} AS model_updated_date,
      {{ generate_load_id(model) }} AS model_load_id
FROM    {{ source('curated_erm', 'proc_sch_hdr') }} PSH
LEFT OUTER JOIN {{ source('curated_erm', 'tmr_item') }} TI ON TI.mat_no = PSH.proj_comp_no
JOIN {{ source('curated_erm', 'tmr_hdr') }} TH ON TH.tmr_no = TI.tmr_no
JOIN {{ source('curated_erm', 'proc_ext_stat') }} PES ON PES.proc_ext_stat_no = TH.proc_ext_stat_no
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} suppl ON suppl.suppl_no = TH.suppl_no
LEFT OUTER JOIN {{ source('curated_erm', 'proj_planned_tmr_hdr') }} PPTH ON PPTH.proj_planned_tmr_hdr_no = TH.proj_planned_tmr_hdr_no
WHERE PSH.is_current = 1 
AND TI.is_current = 1 
AND TH.is_current = 1 
AND PES.is_current = 1 
AND PPTH.is_current = 1

UNION

SELECT PSH.PROC_SCH_HDR_NO, 
       TH.TMR_ID  AS TMR_MR_ID,
       TH.TITLE,
       PES.TMR_EXT_STAT_ID,
       TH.VER,
       TH.REV,
       CASE 
           WHEN TH.WORKFLOW_PURPOSE_NO = 1 THEN 'ENGINEERING'  
           WHEN TH.WORKFLOW_PURPOSE_NO = 2 THEN 'ENQUIRY ONLY' 
           WHEN TH.WORKFLOW_PURPOSE_NO = 3 THEN 'ENQUIRY AND PURCHASE' 
           WHEN TH.WORKFLOW_PURPOSE_NO = 4 THEN 'PURCHASE' 
           WHEN TH.WORKFLOW_PURPOSE_NO = 5 THEN 'CANCELLATION' 
           ELSE NULL 
       END AS ISSUE_PURPOSE,
       TH.ISSUED_DATE,
       CAST(NULL AS STRING) AS VALUE_QUAN,  -- Fix for NULLTYPE issue
       CAST(NULL AS STRING) AS UNIT_CUR_ID, -- Fix for NULLTYPE issue
       TH.DEADLINE,
       TH.TECH_HAND_USR_ID  AS RESPONSIBLE_USR_ID,
       SUPPL.SUPPL_ID,
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       {{run_date}} AS model_created_date,
       {{run_date}} AS model_updated_date,
       {{ generate_load_id(model) }} AS model_load_id
FROM   {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN   {{ source('curated_erm', 'tmr_hdr') }} TH ON TH.TMR_NO = PSH.TMR_NO
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} SUPPL ON SUPPL.SUPPL_NO = TH.SUPPL_NO
JOIN   {{ source('curated_erm', 'proc_ext_stat') }} PES ON PES.PROC_EXT_STAT_NO = TH.PROC_EXT_STAT_NO
WHERE  PSH.is_current = 1 
AND    TH.is_current = 1 
AND    PES.is_current = 1


UNION

SELECT PSH.proc_sch_hdr_no, 
       TH.tmr_id AS tmr_mr_id,
       TH.title,
       PES.tmr_ext_stat_id,
       TH.ver,
       TH.rev,
       CASE 
           WHEN TH.workflow_purpose_no = 1 THEN 'Engineering'  
           WHEN TH.workflow_purpose_no = 2 THEN 'Enquiry Only' 
           WHEN TH.workflow_purpose_no = 3 THEN 'Enquiry And Purchase' 
           WHEN TH.workflow_purpose_no = 4 THEN 'Purchase' 
           WHEN TH.workflow_purpose_no = 5 THEN 'Cancellation' 
           ELSE NULL 
       END AS issue_purpose,
       TH.issued_date,
       CAST(NULL AS STRING) AS value_quan,  -- Fixed NULLTYPE issue
       CAST(NULL AS STRING) AS unit_cur_id, -- Fixed NULLTYPE issue
       TH.deadline,
       TH.tech_hand_usr_id  AS responsible_usr_id,
       suppl.suppl_id,
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       {{run_date}} AS model_created_date,
       {{run_date}} AS model_updated_date,
       {{ generate_load_id(model) }} AS model_load_id
FROM   {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN   {{ source('curated_erm', 'proj_planned_tmr_hdr') }} PPTH 
       ON PPTH.proj_planned_tmr_hdr_no = PSH.proj_planned_tmr_hdr_no
LEFT OUTER JOIN {{ source('curated_erm', 'tmr_hdr') }} TH 
       ON TH.proj_planned_tmr_hdr_no = PPTH.proj_planned_tmr_hdr_no
LEFT OUTER JOIN {{ ref('transformed_erm_suppl') }} suppl 
       ON suppl.suppl_no = TH.suppl_no
JOIN   {{ source('curated_erm', 'proc_ext_stat') }} PES 
       ON PES.proc_ext_stat_no = TH.proc_ext_stat_no
JOIN   {{ source('curated_erm', 'tmr_hdr') }} TH_FILTER 
       ON TH_FILTER.proj_planned_tmr_hdr_no = PPTH.proj_planned_tmr_hdr_no
WHERE  PSH.is_current = 1 
AND    TH.is_current = 1 
AND    PES.is_current = 1 
AND    PPTH.is_current = 1


UNION


SELECT PSH.proc_sch_hdr_no, 
       PPTH.proj_planned_tmr_hdr_id AS tmr_mr_id,
       PPTH.title,
       CAST(NULL AS STRING) AS tmr_ext_stat_id,  
       CAST(NULL AS STRING) AS ver,             
       CAST(NULL AS STRING) AS rev,             
       CAST(NULL AS STRING) AS issue_purpose,   
       CAST(NULL AS DATE) AS issued_date,       
       CAST(NULL AS STRING) AS value_quan,      
       CAST(NULL AS STRING) AS unit_cur_id,     
       CAST(NULL AS TIMESTAMP) AS deadline,     
       PPTH.resp_usr_id AS responsible_usr_id,
       CAST(NULL AS STRING) AS suppl_id,        
       CAST(PSH.execution_date AS DATE) AS etl_load_date,
       {{run_date}} AS model_created_date,
       {{run_date}} AS model_updated_date,
       {{ generate_load_id(model) }} AS model_load_id
FROM   {{ source('curated_erm', 'proc_sch_hdr') }} PSH
JOIN   {{ source('curated_erm', 'proj_planned_tmr_hdr') }} PPTH 
       ON PPTH.proj_planned_tmr_hdr_no = PSH.proj_planned_tmr_hdr_no
JOIN   {{ source('curated_erm', 'proj') }} PRJ 
       ON PRJ.proj_no = PPTH.proj_no
LEFT JOIN {{ source('curated_erm', 'tmr_hdr') }} TH 
       ON TH.proj_planned_tmr_hdr_no = PPTH.proj_planned_tmr_hdr_no
WHERE  TH.proj_planned_tmr_hdr_no IS NULL  -- Optimized condition
AND    PSH.is_current = 1 
AND    PRJ.is_current = 1 
AND    PPTH.is_current = 1

	