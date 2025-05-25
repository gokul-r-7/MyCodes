{%- set run_date = "CURRENT_TIMESTAMP()" -%}



{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_po_line_item/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


WITH po_line_item_data AS (
    SELECT
        CAST(ROUND(po_item.po_item_no) AS STRING) || '-' || CAST(ROUND(po_hdr.ver) AS STRING) || '-' || CAST(ROUND(po_item.ver) as STRING) AS constraint_key,
        proj.proj_no AS proj_no,
        proj.proj_id AS project_number,
        po_hdr.po_id AS po_number,
        COALESCE(si.po_item_id, po_item.po_item_id) AS po_line_number,
        COALESCE(tt2.trans_term_id, tt1.trans_term_id) AS transportation_term,
        CASE 
            WHEN MAT.mat_grp_type_no = 1 THEN STD_MAT_CPV.uns_no
            WHEN MAT.mat_grp_type_no = 2 THEN CVLI1.value
            ELSE NULL 
        END AS financial_account_code,
        psb.sys_brkdwn_id AS coa,
        pca.cost_acc_id AS cost_type,
        po_item.quan AS po_line_quantity,
        ROUND(po_item.price, 2) AS line_item_unit_price,
        po_item.deadline AS required_on_site_date,
        po_item.agreed_deadline AS promised_date,
        COALESCE(NULLIF(po_item.deliv_place,''), NULLIF(po_hdr.deliv_place,'')) AS deliver_to_location_id,
        UPPER(pfacc.acc_wbs) AS wbs_code,
        tmr_hdr.def_date AS expenditure_item_date,
        po_item.ver AS item_revision,
        '' AS man_hours,
        po_hdr.issue_date AS processed_date,
        si.batch_id AS sub_component,
        po_hdr.cur_id AS currency_type,
        po_item.proc_unit_id AS unit_of_measure,
        COALESCE(dt2.deliv_term_id,dt1.deliv_term_id) AS incoterm_tax_terms,
        UPPER(proj_fin_acc_code.proj_fin_acc_code_id) AS control_account,
        commodity.commodity_id AS item_commodity,
        po_item.mat_id AS item_tag,
        po_item.mat_grp_type_no AS item_category,
        po_item.descr AS item_description,
        po_hdr.tmr_id AS tmr_number,
        tmr_hdr.ver_raised_date AS tmr_date,
        tmr_item.quan AS tmr_quantity,
        tmr_item.tmr_item_id AS tmr_line_number,
        po_item.po_item_no AS item_internal_id,
        po_item.po_item_no AS purchase_order_line_item_internal_id,
        tmr_item.tmr_item_no AS requisition_line_internal_id,
        tmr_hdr.tmr_no AS requisition_internal_id,
        po_hdr.ver AS po_revision,
        proc_int_stat.proc_int_stat_id AS po_status,
        po_hdr.po_no AS po_internal_id,
        po_item.agreed_deadline AS contract_delivery_date,
        cvli3.value as jip33,
        cast(po_item.execution_date as date) AS etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id
    FROM
        {{ source('curated_erm', 'po_item') }} po_item
        LEFT JOIN (
            SELECT
                pi1.po_item_no,
                sub.po_item_id,
                sub.batch_id
            FROM
                (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) pi1
            LEFT JOIN (
                SELECT
                    si.batch_po_item_no,
                    si.po_item_no,
                    pi.po_item_id,
                    si.batch_id
                FROM
                    (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) pi,
                    (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) si
                WHERE
                    pi.po_item_no = si.batch_po_item_no
            ) sub ON pi1.po_item_no = sub.po_item_no
        ) si ON po_item.po_item_no = si.po_item_no
        INNER JOIN (SELECT * FROM {{ source('curated_erm', 'po_hdr') }} WHERE is_current = 1) po_hdr ON po_item.po_no = po_hdr.po_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj') }} WHERE is_current = 1) proj ON po_hdr.proj_no = proj.proj_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'tmr_hdr') }} WHERE is_current = 1) tmr_hdr ON po_hdr.tmr_id = tmr_hdr.tmr_id
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'tmr_item_po_item') }} WHERE is_current = 1) tmr_item_po_item ON po_item.po_item_no = tmr_item_po_item.po_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'tmr_item') }} WHERE is_current = 1) tmr_item ON tmr_item_po_item.tmr_item_no = tmr_item.tmr_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_fin_acc_code_cpv') }} WHERE is_current = 1) proj_fin_acc_code_cpv ON proj_fin_acc_code_cpv.proj_fin_acc_code_no = tmr_item.proj_fin_acc_code_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_fin_acc_code') }} WHERE is_current = 1) proj_fin_acc_code ON po_item.proj_fin_acc_code_no = proj_fin_acc_code.proj_fin_acc_code_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'trans_term') }} WHERE is_current = 1) tt1 ON po_hdr.trans_term_no = tt1.trans_term_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'trans_term') }} WHERE is_current = 1) tt2 ON po_item.trans_term_no = tt2.trans_term_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_cost_acc') }} WHERE is_current = 1) pca ON po_item.proj_cost_acc_no = pca.proj_cost_acc_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_sys_brkdwn') }} WHERE is_current = 1) psb ON po_item.proj_sys_brkdwn_no = psb.proj_sys_brkdwn_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_fin_acc_code_cpv') }} WHERE is_current = 1) pfacc ON pfacc.proj_fin_acc_code_no = po_item.proj_fin_acc_code_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'deliv_term') }} WHERE is_current = 1) dt1 ON po_hdr.deliv_term_no = dt1.deliv_term_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'deliv_term') }} WHERE is_current = 1) dt2 ON po_item.deliv_term_no = dt2.deliv_term_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proc_int_stat') }} WHERE is_current = 1) proc_int_stat ON po_hdr.proc_int_stat_no = proc_int_stat.proc_int_stat_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'mat') }} WHERE is_current = 1) mat ON po_item.mat_no = mat.mat_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'part') }} WHERE is_current = 1) part ON mat.part_no = part.part_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'commodity') }} WHERE is_current = 1) commodity ON part.commodity_no = commodity.commodity_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'std_mat') }} WHERE is_current = 1) std_mat ON mat.std_mat_no = std_mat.std_mat_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'std_mat_cpv') }} WHERE is_current = 1) std_mat_cpv ON std_mat.std_mat_no = std_mat_cpv.std_mat_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_comp_cpv') }} WHERE is_current = 1) proj_comp_cpv ON mat.proj_comp_no = proj_comp_cpv.proj_comp_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli1 ON proj_comp_cpv.unspsc = cvli1.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'tmr_hdr_cpv') }} WHERE is_current = 1) tmr_hdr_cpv ON tmr_item_po_item.tmr_no=tmr_hdr_cpv.tmr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli2 ON std_mat_cpv.unspsc = cvli2.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli3 ON tmr_hdr_cpv.jip33_doc = cvli3.value_list_item_no
    WHERE
        proj.stat = 1 and po_item.is_current=1
)


SELECT * FROM po_line_item_data