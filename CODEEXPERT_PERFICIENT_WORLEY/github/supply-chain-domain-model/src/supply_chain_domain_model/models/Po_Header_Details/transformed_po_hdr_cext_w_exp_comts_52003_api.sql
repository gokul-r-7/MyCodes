{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_po_hdr_cext_w_exp_comts_52003_api/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}   


SELECT   
    sub.expedite_hdr_no,
    CONCAT_WS(', ', COLLECT_LIST(SUBSTRING(sub.comnt, 1, 1000))) AS comnt,
    CAST(sub.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM (
    SELECT 
        eh.expedite_hdr_no,
        CASE 
            WHEN ehl.txt LIKE '**ARCH%' THEN TRIM(ehl.def_date || ' ' || 
                CASE WHEN ehl.def_usr_id IS NULL THEN ''
                     ELSE ehl.def_usr_id || ':' 
                END || ' ' || '**' || REGEXP_REPLACE(ehl.txt, '[^0-9A-Za-z]', ' ')) 
            ELSE TRIM(ehl.def_date || ' ' || 
                CASE WHEN ehl.def_usr_id IS NULL THEN ''
                     ELSE ehl.def_usr_id || ':' 
                END || ' ' || REGEXP_REPLACE(ehl.txt, '[^0-9A-Za-z]', ' ')) 
        END AS comnt,            
        ehl.def_date,
        eh.execution_date
    FROM {{ source('curated_erm', 'expedite_hdr') }} eh
    LEFT JOIN {{ source('curated_erm', 'expedite_hdr_log') }} ehl
        ON eh.expedite_hdr_no = ehl.expedite_hdr_no
    WHERE eh.is_current = 1
) AS sub
GROUP BY sub.expedite_hdr_no, sub.execution_date


