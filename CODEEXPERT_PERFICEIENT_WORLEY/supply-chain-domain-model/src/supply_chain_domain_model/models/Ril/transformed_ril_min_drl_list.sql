{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ril_min_drl_list/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



WITH minlist AS (
    SELECT
         rml.drawing_number,
        'INT_TEST' AS PROJECT_ID,
        stm.site_mto_hdr_id AS SITEMTOHDRID,
        --TO_CHAR(TO_DATE(rml.issued_date, 'YYYY-MM-DD'), 'DD.MM.YYYY') AS PRODUCTIONDATE,
        DATE_FORMAT(TO_DATE(rml.issued_date, 'yyyy-MM-dd'), 'dd.MM.yyyy') AS PRODUCTIONDATE,
        rml.pts_code AS MATERIALID,
        rml.final_qty AS QUAN,
        rml.uom AS UOM,
        smi.line_no AS SITEMTOLINEITEMID,
        'FITTING YARD' AS STORE,
        dd.deliv_desig_id AS FABCAT,
        'FY_BIN_001' AS LOCATION,
        rml.min_number AS VOUCHERNO,
        ROW_NUMBER() OVER (PARTITION BY rml.min_number ORDER BY smi.line_no) AS VOUCHERITEMNO,
        'INS' AS TRANSTYPE,
        'N' AS erm_txn_status,
        CASE WHEN smi.site_mto_item_no IS NULL THEN 'N' ELSE 'Y' END AS at_erm,
        cast(rml.execution_date as date) AS etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id
    FROM {{ source('curated_erm', 'ril_min_list') }} rml
    LEFT JOIN {{ source('curated_erm', 'draw') }} d
        ON d.draw_name = rml.drawing_number
        AND d.is_current = 1
    LEFT JOIN {{ source('curated_erm', 'site_mto_hdr') }} stm
        ON d.draw_no = stm.draw_no
        AND stm.is_current = 1
    LEFT JOIN {{ source('curated_erm', 'site_mto_item') }} smi
        ON smi.site_mto_hdr_no = stm.site_mto_hdr_no
        AND smi.is_current = 1
    JOIN {{ source('curated_erm', 'mat') }} mat
        ON mat.mat_id = rml.pts_code AND smi.mat_no = mat.mat_no
        AND mat.is_current = 1
    JOIN {{ source('curated_erm', 'deliv_desig') }} dd
        ON dd.deliv_desig_no = stm.deliv_desig_no
        AND dd.is_current = 1
)

SELECT * FROM minlist
