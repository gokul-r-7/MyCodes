{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_exp_date_summary_api/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT 
    poli.po_no,
    DATE_TRUNC('DAY', MIN(DATE_ADD(CAST(expli.deliv_deadline AS DATE), CAST(expli.ship_time AS INT)))) AS first_forecast_date,
    DATE_TRUNC('DAY', MAX(DATE_ADD(CAST(expli.deliv_deadline AS DATE), CAST(expli.ship_time AS INT)))) AS last_forecast_date,
    DATE_TRUNC('DAY', MIN(earliros.deadline)) AS earliest_ros_date, 
    DATE_TRUNC('DAY', MAX(expli.deadline)) AS last_ros_date,
    DATEDIFF(
        DATE_TRUNC('DAY', MIN(expli.deadline)), 
        DATE_TRUNC('DAY', MIN(DATE_ADD(CAST(expli.deliv_deadline AS DATE), CAST(expli.ship_time AS INT))))
    ) AS floats_earlros_firstforecast,
    DATEDIFF(
        DATE_TRUNC('DAY', MAX(expli.deadline)), 
        DATE_TRUNC('DAY', MAX(DATE_ADD(CAST(expli.deliv_deadline AS DATE), CAST(expli.ship_time AS INT))))
    ) AS floats_lastros_lastforecast,
    COUNT(DISTINCT expli.expedite_item_no) AS line_item_count,
    CAST(expli.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'po_item') }} poli
JOIN {{ source('curated_erm', 'expedite_hdr') }} exph 
    ON exph.po_no = poli.po_no AND exph.is_current = 1
JOIN {{ source('curated_erm', 'expedite_item') }} expli 
    ON exph.expedite_hdr_no = expli.expedite_hdr_no AND expli.is_current = 1
LEFT JOIN (
    SELECT 
        expli.expedite_hdr_no, 
        MIN(CAST(expli.deadline AS DATE)) AS deadline
    FROM {{ source('curated_erm', 'expedite_item') }} expli  
    WHERE expli.quan <> 0  
      AND expli.is_deliverable = 1 AND expli.is_current = 1 
    GROUP BY expli.expedite_hdr_no
) earliros 
    ON exph.expedite_hdr_no = earliros.expedite_hdr_no

WHERE expli.is_deliverable = 1
AND poli.is_current = 1  

GROUP BY poli.po_no, expli.execution_date