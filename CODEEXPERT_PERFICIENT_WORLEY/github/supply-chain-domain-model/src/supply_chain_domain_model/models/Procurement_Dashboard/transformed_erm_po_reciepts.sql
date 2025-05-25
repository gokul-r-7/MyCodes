{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_po_reciepts/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


WITH  receipt_data AS (
    SELECT
        CAST(ROUND(ri.receipt_item_no) as STRING) || '-' || CAST(ROUND(rh.receipt_hdr_no) as STRING)  AS constraint_key,
        p.proj_no AS proj_no,
        p.proj_id AS project_number,
        ph.po_id AS po_number,
        COALESCE(si.po_item_id,pi.po_item_id) AS po_line_item,
        ei.expedite_item_id AS eo,
        si.batch_id AS so,
        rh.receipt_hdr_id AS receipt_number,
        rh.arrival_date AS received_date,
        'N/A' AS receipt_revision_number,
        ri.receive_unit_quan AS received_quantity,
        cp.received_by AS receiver_employee_id,
        COALESCE(ri.qa_non_conformance_quan, 0) AS damaged_quantity,
        pi.deliv_place AS deliver_to_location,
        mh.mmt_hdr_id AS shipment_ref,
        ei.expedite_item_id AS shipment_line_number,
        ri.receipt_item_no AS material_receipt_item_id,
        rh.receipt_hdr_no AS material_receipt_id,
        mh.mmt_hdr_no AS shipment_id,
        ph.po_no AS po_internal_id,
        ph.ver AS po_revision,
        pi.ver AS item_revision,
        pis.proc_int_stat_id AS po_status,
        pi.po_item_no AS po_line_item_internal_id,
        cast(rh.execution_date as date) AS etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id
    FROM
        {{ source('curated_erm', 'receipt_hdr') }}  rh
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'receipt_item') }} WHERE is_current = 1) ri ON rh.receipt_hdr_no = ri.receipt_hdr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'mmt_hdr') }} WHERE is_current = 1) mh ON rh.mmt_hdr_no = mh.mmt_hdr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'po_hdr') }} WHERE is_current = 1) ph ON rh.po_no = ph.po_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) pi ON rh.po_no = pi.po_no AND pi.po_item_no = ri.po_item_no
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
        ) si ON pi.po_item_no = si.po_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj') }} WHERE is_current = 1) p ON ph.proj_no = p.proj_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'receipt_hdr_cpv') }} WHERE is_current = 1) cp ON rh.receipt_hdr_no = cp.receipt_hdr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'expedite_item') }} WHERE is_current = 1) ei ON mh.mmt_hdr_no = ei.mmt_hdr_no AND pi.po_item_no = ei.po_item_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proc_int_stat') }} WHERE is_current = 1) pis ON ph.proc_int_stat_no = pis.proc_int_stat_no
    WHERE
        p.stat = '1'
        AND rh.receipt_stat_no = '6'
        AND rh.is_current=1
)

SELECT * FROM receipt_data
