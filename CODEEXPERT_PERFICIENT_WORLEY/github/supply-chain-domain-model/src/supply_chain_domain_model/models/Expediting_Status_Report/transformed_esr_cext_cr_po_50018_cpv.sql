{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_cr_po_50018_cpv/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT 
    proj.proj_id,
    cpv.po_no,
    ph.po_id,
    ph.ver,
    cpv.client_ref_no,
    cpv.point_of_origin,
    cpv.cust_po_no,
    cpv.issue_date,
    cvli_t.VALUE AS tax_status,
    cvli_e.VALUE AS expediting_level,
    cvli_i.VALUE AS inspection_level,
    cvli_o.VALUE AS origin,
    COALESCE(cpv.page1bsign, 'Signed for and on behalf of Company') AS buyer_sign,
    REPLACE(cpv.page1vsign, '<SUPPLIER_NAME>', cont.name_1) AS supp_sign,
    CASE 
        WHEN cpv.po_total_label IS NULL THEN 'New Total Amount:'
        ELSE cpv.po_total_label || ':'
    END AS po_total_label,
    cpv.po_ship_to,
    COALESCE(cpv.sub_project, 'None') AS sub_project,
    'PC' AS cpv_type,  /* Current CPV */
    CAST(proj.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'po_hdr') }} ph
LEFT JOIN {{ source('curated_erm', 'po_hdr_cpv') }} cpv ON cpv.po_no = ph.po_no AND cpv.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proj') }} proj ON ph.proj_no = proj.proj_no AND proj.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_t ON cpv.tax_status = cvli_t.value_list_item_no AND cvli_t.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_e ON cpv.expediting_level = cvli_e.value_list_item_no AND cvli_e.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_i ON cpv.inspection_level = cvli_i.value_list_item_no AND cvli_i.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_o ON cpv.origin = cvli_o.value_list_item_no AND cvli_o.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cont') }} cont ON ph.suppl_no = cont.cont_no AND cont.is_current = 1 
AND ph.is_current = 1 

   UNION ALL
SELECT 
    proj.proj_id,
    phh.po_no,
    phh.po_id,
    phh.ver,
    cpv.client_ref_no,
    cpv.point_of_origin,
    cpv.cust_po_no,
    NULL AS issue_date,  -- Removed TO_DATE('') since it's invalid
    cvli_t.VALUE AS tax_status,
    cvli_e.VALUE AS expediting_level,
    cvli_i.VALUE AS inspection_level,
    cvli_o.VALUE AS origin,
    COALESCE(cpv.page1bsign, 'Signed for and on behalf of Company') AS buyer_sign,
    REPLACE(cpv.page1vsign, '<SUPPLIER_NAME>', cont.name_1) AS supp_sign,
    CASE 
        WHEN cpv.po_total_label IS NULL THEN 'New Total Amount:'
        ELSE cpv.po_total_label || ':'
    END AS po_total_label,
    cpv.po_ship_to,
    COALESCE(cpv.sub_project, 'None') AS sub_project,
    'PCH' AS cpv_type,  /* History CPV */
    CAST(proj.execution_date AS DATE) AS etl_load_date,
    {{ run_date }} AS model_created_date,
    {{ run_date }} AS model_updated_date,
    {{ generate_load_id(model) }} AS model_load_id
FROM {{ source('curated_erm', 'po_hdr_hist') }} phh
LEFT JOIN {{ source('curated_erm', 'po_hdr_hist_cpv') }} cpv ON cpv.po_hdr_hist_no = phh.po_hdr_hist_no AND cpv.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'proj') }} proj ON phh.proj_no = proj.proj_no AND proj.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_t ON cpv.tax_status = cvli_t.value_list_item_no AND cvli_t.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_e ON cpv.expediting_level = cvli_e.value_list_item_no AND cvli_e.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_i ON cpv.inspection_level = cvli_i.value_list_item_no AND cvli_i.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli_o ON cpv.origin = cvli_o.value_list_item_no AND cvli_o.is_current = 1 
LEFT JOIN {{ source('curated_erm', 'cont') }} cont ON phh.suppl_no = cont.cont_no AND cont.is_current = 1 
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ source('curated_erm', 'po_hdr') }} ph
    WHERE ph.po_no = phh.po_no 
      AND ph.ver = phh.ver
)
AND phh.is_current = 1 