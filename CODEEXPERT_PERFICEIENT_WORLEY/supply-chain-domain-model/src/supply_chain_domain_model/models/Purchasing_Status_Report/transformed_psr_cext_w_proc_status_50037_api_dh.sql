{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_psr_cext_w_proc_status_50037_api_dh/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



select distinct  
    main.proj_no,
    main.proj_id,
    main.proj_id_descr,
    main.proc_sch_hdr_no,
    main.proc_sch_hdr_id,
    main.ver,          
    main.title,
    main.req_originator,
    case 
        when main.po_cnt < 2 then main.buyer 
        else main.assigned_buyer 
    end as buyer,
    case 
        when main.po_cnt < 2 then main.expediter 
        else main.proc_sch_expeditor 
    end as expediter,
    main.criticality,
    main.discipline,
    case 
        when main.po_cnt < 2 then main.supplier 
        else main.potential_suppliers 
    end as supplier,         
    main.potential_suppliers,
    main.budget,
    main.rem,
    main.proj_planned_tmr_hdr_id,
    main.mr_tmr,
    case 
        when main.po_cnt >= 2 then 'multiple' 
        else main.po_number 
    end as po_number,
    case 
        when main.po_cnt >= 2 then null 
        else main.po_gross_value 
    end as po_gross_value,
    main.deliv_deadline,
    coalesce(lt.lead_time, main.lead_time) as lead_time,         
    main.proc_sch_stat_no,
    main.sub_project,
   datediff(main.deliv_deadline, main.fct_date) as cf_new_float,
    main.proc_mstone_id,
    main.proc_milestone_id,
    main.m_planned_date,
    main.m_forecast_date,
    main.m_actual_date,
    main.m_duration,
    main.m_float,
    main.milestone_comment,
    main.schedule_item_type_no,
    main.calc_seq,
    main.rem as remarks,
    main.contract_owner,
    main.contract_holder,          
    main.project_region,
    main.po_id_csv,
    main.customer_po_no,
    cast(main.execution_date as date) as etl_load_date,
    {{ run_date }} as model_created_date,
    {{ run_date }} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM (
SELECT 
    PROJ.PROJ_NO,
    PROJ.PROJ_ID,                  
    PROJ.DESCR AS PROJ_ID_DESCR,
    HDR.PROC_SCH_HDR_NO,
    HDR.PROC_SCH_HDR_ID,                  
    HDR.VER,
    HDR.TITLE,
    COALESCE(
        CASE WHEN TMR.tmr_mr_id IS NULL THEN CPV.ORIGINATOR ELSE TH.req_originator END, 
        CPV.ORIGINATOR
    ) AS REQ_ORIGINATOR,
    CASE 
        WHEN PHH.po_max_issd_ver = POH.ver THEN COALESCE(POH.merc_hand_usr_id, HDR.responsible_usr_id, '(empty)')
        WHEN PHH.po_max_issd_ver IS NULL THEN COALESCE(HDR.responsible_usr_id, '(empty)')
        ELSE COALESCE(PHH.merc_hand_usr_id, HDR.responsible_usr_id, '(empty)')
    END AS BUYER,
    COALESCE(HDR.responsible_usr_id, '(empty)') AS ASSIGNED_BUYER,
    CPV.EXPEDITER AS PROC_SCH_EXPEDITOR,
    COALESCE(PO.EXPEDITE_USR_ID, CPV.EXPEDITER) AS EXPEDITER,
    CPV.CRITICALITY,
    COALESCE(DD.DESCR, '(empty)') AS DISCIPLINE,
    S.NAME_1 AS SUPPLIER,
    '' AS SOURCING_OPTION,
    '' AS PLANNED_CONTRACT_TYPE,
    CPV.CLIENT_REF_NO,
    CPV.BUDGET_CURRENCY,
    CPV.POTENTIAL_SUPPLIERS,
    CPV.BUDGET,
    FORMAT_NUMBER(CPV.BUDGET, 2) || ' ' || CPV.BUDGET_CURRENCY AS PSHC_BUDGET,
    HDR.REM,
    PPTH.proj_planned_tmr_hdr_id,
    TMR.tmr_mr_id AS MR_TMR,
    PO.po_id AS PO_NUMBER,
    FORMAT_NUMBER(PO.po_gross_value, 2) || ' ' || PO.cur_id AS PO_GROSS_VALUE,                                     
    HDR.deliv_deadline AS DELIV_DEADLINE,
    PPTH.PROJ_PLANNED_TMR_HDR_NO,
    PROJ.CLIENT_LOGO AS PROJ_CLIENT_LOGO,
    LE.logo_file,
    TMR.rev,
    TH.ver AS TMR_MAX_ISSD_VER,
    PHH.po_max_issd_ver,
    SCSV.PO_CNT,
    CASE WHEN HDR.proc_sch_stat_no = 4 THEN 1 ELSE 0 END AS PROC_SCH_STAT_NO,
    COALESCE(CPV.sub_project, 'None') AS SUB_PROJECT,
    CPV.LEAD_TIME,
    CFD.fct_date,
    MSTONE.proc_mstone_id,
    MSTONE.proc_milestone_id,
    MSTONE.planned_date AS M_PLANNED_DATE,
    MSTONE.forecast_date AS M_FORECAST_DATE,
    MSTONE.actual_date AS M_ACTUAL_DATE,
    MSTONE.duration AS M_DURATION,
    CASE 
        WHEN MSTONE.actual_date IS NOT NULL AND MSTONE.planned_date IS NOT NULL THEN
            DATEDIFF(MSTONE.actual_date, MSTONE.planned_date)
        ELSE MSTONE.dd_float
    END AS M_FLOAT,
    MSTONE.milestone_comment,
    CASE WHEN MSTONE.schedule_item_type_no = 2 THEN 1 ELSE 0 END AS SCHEDULE_ITEM_TYPE_NO,
    MSTONE.CALC_SEQ,
    CPV.client_lead AS CONTRACT_OWNER,
    CPV.package_eng AS CONTRACT_HOLDER,
    CVLI.value AS PROJECT_REGION,
    SCSV.po_id_csv,
    COALESCE(PHC.cust_po_no, CAST(CPV.cust_po_no AS STRING)) AS CUSTOMER_PO_NO,
    CPV.execution_date
FROM {{ source('curated_erm', 'proc_sch_hdr') }} HDR
LEFT JOIN {{ source('curated_erm', 'proj') }} PROJ ON HDR.PROJ_NO = PROJ.PROJ_NO and PROJ.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'proj_cpv') }} PC ON PROJ.proj_no = PC.proj_no  and PC.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} CVLI ON PC.proj_region = CVLI.value_list_item_no  and CVLI.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'proc_sch_hdr_cpv') }} CPV ON HDR.PROC_SCH_HDR_NO = CPV.PROC_SCH_HDR_NO  and CPV.is_current = 1 
LEFT JOIN {{ ref('transformed_psr_dext_w_asv_r50039') }} TMR ON HDR.PROC_SCH_HDR_NO = TMR.PROC_SCH_HDR_NO  
LEFT JOIN {{ ref('transformed_psr_dext_w_asv_r50039_po') }} PO ON HDR.PROC_SCH_HDR_NO = PO.PROC_SCH_HDR_NO  
LEFT JOIN (
    SELECT 
        PROC_SCH_HDR_NO,
        COUNT(po_id) AS PO_CNT,                          
        ARRAY_JOIN(COLLECT_LIST(PO_ID), ',') AS po_id_csv
    FROM {{ ref('transformed_psr_dext_w_asv_r50039_po') }}
    GROUP BY PROC_SCH_HDR_NO
) SCSV ON HDR.PROC_SCH_HDR_NO = SCSV.PROC_SCH_HDR_NO
LEFT JOIN {{ ref('transformed_erm_suppl') }} S ON PO.SUPPL_ID = S.SUPPL_ID
LEFT JOIN {{ source('curated_erm', 'proj_planned_tmr_hdr') }} PPTH ON HDR.proj_planned_tmr_hdr_no = PPTH.proj_planned_tmr_hdr_no   and PPTH.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'draw_discipline') }} DD ON PPTH.draw_discipline_no = DD.draw_discipline_no   and DD.is_current = 1 
LEFT JOIN {{ ref('transformed_psr_wp_proc_st_vert_tmrros') }} TH ON HDR.proc_sch_hdr_no = TH.proc_sch_hdr_no 
LEFT JOIN {{ source('curated_erm', 'po_hdr') }} POH ON PO.po_id = POH.po_id   and POH.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'po_hdr_cpv') }} PHC ON POH.po_no = PHC.po_no   and PHC.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'legal_entity') }} LE ON PROJ.LEGAL_ENTITY_NO = LE.LEGAL_ENTITY_NO   and LE.is_current = 1 
LEFT JOIN {{ ref('transformed_psr_cext_wp_milestone_items_api_new') }} MSTONE ON HDR.proc_sch_hdr_no = MSTONE.proc_sch_hdr_no 
LEFT JOIN (
    SELECT 
        po_no, 
        merc_hand_usr_id, 
        ver AS po_max_issd_ver 
    FROM {{ source('curated_erm', 'po_hdr_hist') }} 
    WHERE ver = (
        SELECT MAX(ver) 
        FROM {{ source('curated_erm', 'po_hdr_hist') }} 
        WHERE po_no = po_no and  is_current = 1 
    )
) PHH ON POH.po_no = PHH.po_no
LEFT JOIN (
    SELECT 
        PROJ.proj_id,
        PROJ.proj_no,
        PSI.proc_sch_hdr_no,
        PM.DESCR AS proc_milestone_id,
        PM.proc_milestone_id AS proc_mstone_id,
        PSI.forecast_on_dd AS fct_date
    FROM {{ source('curated_erm', 'proc_milestone') }} PM
    JOIN {{ source('curated_erm', 'proc_sch_item') }} PSI ON PM.proc_milestone_no = PSI.proc_milestone_no and PSI.is_current = 1 
    JOIN {{ source('curated_erm', 'proc_sch_hdr') }} PSH ON PSI.proc_sch_hdr_no = PSH.proc_sch_hdr_no and PSH.is_current = 1 
    JOIN {{ source('curated_erm', 'proj') }} PROJ ON PSH.proj_no = PROJ.proj_no and PROJ.is_current = 1 
    WHERE PSI.block_reporting != 1 and PM.is_current = 1 
    AND PM.proc_milestone_id = 'PO COMPLETELY DELIVERED'
) CFD ON HDR.proc_sch_hdr_no = CFD.proc_sch_hdr_no
WHERE (CPV.template <> 1 OR CPV.template IS NULL)
AND PROJ.template_proj != 1
 and HDR.is_current = 1 
    ) MAIN
LEFT JOIN (
    SELECT 
        CAST(duration AS STRING) AS lead_time, 
        pi.proc_sch_hdr_no
    FROM {{ source('curated_erm', 'proc_sch_item') }} pi
    JOIN {{ source('curated_erm', 'proc_milestone') }} pm
    ON pi.proc_milestone_no = pm.proc_milestone_no  
    WHERE pm.proc_milestone_id = 'MANUFACTURE LEAD TIME'
	 and pi.is_current = 1 
	  and pm.is_current = 1 
) LT ON MAIN.PROC_SCH_HDR_NO = LT.PROC_SCH_HDR_NO

