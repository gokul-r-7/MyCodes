{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_requisition_doc_ctrl/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT
    psr.proj_no AS project_code,
    psr.proj_id,
    psr.ver AS current_revision,
    psr.title AS requisition_title,
    psr.req_originator AS originator,
    psr.buyer,
    psr.expediter AS expeditor,
    psr.criticality,
    psr.discipline,
    psr.supplier,
    psr.potential_suppliers AS potential_supplier,
    psr.budget,
    psr.proj_planned_tmr_hdr_id AS requisition_no,
    psr.po_number,
    psr.lead_time,
    psr.sub_project AS subproject,
    psr.cf_new_float AS float,
    psr.proc_mstone_id AS milestone_id,
    psr.proc_milestone_id AS milestone,
    psr.m_planned_date AS milestone_planned_date,
    psr.m_forecast_date AS milestone_forecast_date,
    psr.m_actual_date AS milestone_actual_date,
    psr.m_duration AS milestone_duration,
    psr.m_float AS milestone_float,
    psr.milestone_comment AS milestone_comment,
    psr.calc_seq AS milestone_seq,
    psr.remarks AS comment,
    psr.contract_owner,
    psr.contract_holder,
    psr.project_region,
    sch.upd_date AS cutoff_date,
    jip33.jip33_flg AS jip33,
    {{run_date}} AS model_created_date,
    {{run_date}} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ ref('transformed_psr_cext_w_proc_status_50037_api_dh') }} psr
LEFT JOIN (
    SELECT  
        proc_sch_hdr_no,
        upd_date,
        proj_no,
        proc_sch_hdr_id
    FROM {{ source('curated_erm', 'proc_sch_hdr') }}
) sch 
ON psr.proj_no = sch.proj_no
AND psr.proc_sch_hdr_no = sch.proc_sch_hdr_no
LEFT JOIN (
    SELECT
        th.tmr_id,
        th.tmr_no,
        th.proj_no,
        cvli.value AS jip33_flg
    FROM {{ source('curated_erm', 'tmr_hdr') }} th
    JOIN {{ source('curated_erm', 'tmr_hdr_cpv') }} thc 
        ON th.tmr_no = thc.tmr_no
    JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli 
        ON thc.jip33_doc = cvli.value_list_item_no
) jip33
ON sch.proj_no = jip33.proj_no
AND sch.proc_sch_hdr_id = jip33.tmr_id;