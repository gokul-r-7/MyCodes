{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_psr_cext_wp_milestone_items_api_new/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

SELECT 
    proj.proj_id,
    proj.proj_no,
    psi1.proc_sch_hdr_no,
    psi1.calc_seq,
    PM1.DESCR AS proc_milestone_id,
    PM1.proc_milestone_id AS proc_mstone_id,
    psi1.orig_plan AS planned_date,
    psi1.forecast_on_dd AS forecast_date,
    psi1.actual_date,
    psi1.dd_float,
    psi1.duration,      
    cpv.milestone_comment,
    pm1.schedule_item_type_no,
    CAST(psi1.forecast_on_dd - psi1.actual_date AS INT) AS new_float,
    cl.no_of_working_days,
    cpv.ms_percent_progress,
    SUM(
        CASE 
            WHEN psi1.actual_date IS NOT NULL THEN 
                COALESCE(NULLIF(CAST(cpv.ms_percent_progress AS double), 0), 0)
            ELSE 0 
        END
    ) OVER (PARTITION BY psi1.proc_sch_hdr_no) AS total_prgs,
    'Actual % Progress (Total)' AS prgs_total_label,
    psi1.orig_duration AS baseline_duration,
    CAST(COALESCE(PM1.execution_date, CURRENT_DATE) AS DATE) AS etl_load_date,
    CURRENT_DATE AS model_created_date,
    CURRENT_DATE AS model_updated_date,
    UUID() AS model_load_id
FROM 
    {{ source('curated_erm', 'proc_milestone') }} PM1
LEFT JOIN {{ source('curated_erm', 'proc_sch_item') }} psi1 
    ON PM1.proc_milestone_no = psi1.proc_milestone_no
    AND psi1.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proc_sch_hdr') }} psh1 
    ON psi1.proc_sch_hdr_no = psh1.proc_sch_hdr_no
    AND psh1.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proj') }} proj 
    ON psh1.proj_no = proj.proj_no
    AND proj.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proc_sch_item_cpv') }} cpv 
    ON psi1.proc_sch_item_no = cpv.proc_sch_item_no
    AND cpv.is_current = 1
LEFT JOIN {{ ref('transformed_psr_cext_w_calendar_days') }} cl 
    ON psi1.proc_sch_item_no = cl.proc_sch_item_no
    AND psi1.proj_no = cl.proj_no
    AND psi1.proc_sch_hdr_no = cl.proc_sch_hdr_no
WHERE 
    psi1.block_reporting != 1
    AND PM1.is_current = 1
