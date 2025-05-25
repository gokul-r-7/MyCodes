{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_w_exp_cwp_view_dhub/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}
 

 
  SELECT  
    proj_no, 
    proj_comp_no, 
    CASE 
        WHEN COUNT(DISTINCT cwp) > 1 THEN 'Multiple' 
        ELSE CONCAT_WS(', ', COLLECT_SET(cwp)) 
    END AS cwp,        
    CASE 
        WHEN COUNT(DISTINCT area) > 1 THEN 'Multiple' 
        ELSE CONCAT_WS(', ', COLLECT_SET(area)) 
    END AS gen_area,    
    CAST(MIN(execution_date) AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id  
FROM 
(
    SELECT DISTINCT 
        ph.proj_no,
        pi.mat_no AS proj_comp_no,
        cpv.cwp,
        cpv.area,
        ph.execution_date
    FROM {{ source('curated_erm', 'pl_hdr') }} ph
    INNER JOIN {{ source('curated_erm', 'pl_item') }} pi 
        ON ph.pl_hdr_no = pi.pl_hdr_no
    INNER JOIN {{ source('curated_erm', 'pl_hdr_cpv') }} cpv 
        ON ph.pl_hdr_no = cpv.pl_hdr_no
    WHERE ph.mto_status = 'A'
        AND ph.is_current = 1
        AND pi.is_current = 1
        AND cpv.is_current = 1
)
GROUP BY proj_no, proj_comp_no