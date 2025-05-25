{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_po_header/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



WITH po_header_data AS (
    SELECT
        CAST(ROUND(po_hdr.ver) AS STRING) || '-' || CAST(ROUND(po_hdr.po_no) as STRING) AS constraint_key,
        proj.proj_no AS proj_no,
        proj.proj_id AS project_number,
        po_hdr.po_id AS po_number,
        pc.po_code_id AS order_type,
        po_hdr.ver AS po_revision,
        phc.point_of_origin AS material_point_of_origin,
        cvli_o.value || '-' || cvli_o.description AS worley_office_origin,
        po_hdr.def_date AS po_creation_date,
        cvli_e.value || '-' || cvli_e.description AS expediting_level,
        cvli_i.value || '-' || cvli_i.description AS inspection_level,
        po_hdr.title AS title,
        cvli_t.description AS tax_status,
        npip.po_gross_value AS gross_value,
        'NULL' AS credit_cost_value,
        'NULL' AS discount_cost_value,
        'NULL' AS documentation_cost_value,
        'NULL' AS inspection_cost_value,
        'NULL' AS packing_cost_value,
        'NULL' AS refund_cost_value,
        'NULL' AS shipping_cost_value,
        'NULL' AS tagging_cost_value,
        'NULL' AS testing_cost_value,
        'NULL' AS other_cost_value,
        'NULL' AS note_to_vendor,
        'NULL' AS note_to_receiver,
        po_hdr.rem AS comments,
        NVL(phc.rev_date, po_hdr.ver_raised_date) AS revision_date,
        po_hdr.issue_date AS award_date,
        'NULL' AS is_je_paper,
        'NULL' AS receipt_required_flag,
        c.name_1 AS client,
        suppl.name_1 AS supplier,
        CASE
            WHEN cvli_p.value = 'Worley Paper' THEN 1
            WHEN cvli_p.value = 'Client' THEN 0
            ELSE 2
        END AS onpaper,
        cvli_h.value AS heritage,
        suppl.suppl_id AS supplier_code,
        ihc.ora_supplier_no AS gbs_supplier_number,
        suppl_addr.suppl_addr_id AS supplier_location,
        pt.pay_term_id AS payment_term,
        po_hdr.approve_date AS po_approval_date,
        proj.fin_proj_id AS gbs_project_number,
        po_hdr.cur_id AS currency_type,
        po_hdr.po_first_issue_date AS po_issue_date,
        'NULL' AS bill_to_location,
        po_hdr.merc_hand_usr_id AS buyer_name,
        po_hdr.approver_usr_id AS approver,
        phc.signer AS signer,
        'ERM' AS database_instance_name,
        proj.proj_no AS project_internal_id,
        po_hdr.po_no AS purchase_order_internal_id,
        po_hdr.suppl_no AS supplier_internal_id,
        po_hdr.suppl_addr_no AS supplier_site_internal_id,
        pis.proc_int_stat_id AS po_status,
        cast(po_hdr.execution_date as date) AS etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id	
    FROM
        {{ source('curated_erm', 'po_hdr') }} po_hdr
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj') }} WHERE is_current = 1) proj ON po_hdr.proj_no = proj.proj_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj_cpv') }} WHERE is_current = 1) proj_cpv ON proj.proj_no = proj_cpv.proj_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'po_hdr_cpv') }} WHERE is_current = 1) phc ON po_hdr.po_no = phc.po_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cont') }} WHERE is_current = 1) c ON proj.client_no = c.cont_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'pay_term') }} WHERE is_current = 1) pt ON po_hdr.pay_term_no = pt.pay_term_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'po_code') }} WHERE is_current = 1) pc ON po_hdr.po_code_no = pc.po_code_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_t ON phc.tax_status = cvli_t.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_e ON phc.expediting_level = cvli_e.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_i ON phc.inspection_level = cvli_i.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_o ON phc.origin = cvli_o.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_p ON proj_cpv.purchase_paper = cvli_p.value_list_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'cusp_value_list_item') }} WHERE is_current = 1) cvli_h ON proj_cpv.transition_from = cvli_h.value_list_item_no
        LEFT JOIN (
            SELECT
                ROW_NUMBER() OVER(PARTITION BY po_no ORDER BY po_no) AS rnk,
                inv_hdr.*
            FROM
                {{ source('curated_erm', 'inv_hdr') }} inv_hdr
            WHERE inv_hdr.is_current = 1
        ) ih ON po_hdr.po_no = ih.po_no AND ih.rnk = 1
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'inv_hdr_cpv') }} WHERE is_current = 1) ihc ON ih.inv_hdr_no = ihc.inv_hdr_no
        LEFT JOIN {{ ref('transformed_erm_suppl') }} suppl on po_hdr.suppl_no = suppl.suppl_no
        LEFT JOIN {{ ref('transformed_erm_suppl_addr') }} suppl_addr on po_hdr.suppl_addr_no = suppl_addr.suppl_addr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proc_int_stat') }} WHERE is_current = 1) pis ON po_hdr.proc_int_stat_no = pis.proc_int_stat_no
        LEFT JOIN {{ ref('transformed_net_po_issd_price') }} npip ON npip.po_no = po_hdr.po_no AND npip.ver=po_hdr.ver
    WHERE
        proj.stat = 1 AND po_hdr.is_current = 1
)

SELECT * FROM po_header_data
