{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_po_shipment/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



WITH shipment_data AS (
    SELECT
        CAST(ROUND(expedite_item.expedite_item_no) AS STRING) || '-' || CAST(ROUND(mmt_hdr_cdc.mmt_hdr_no) AS STRING)  AS constraint_key,
        proj.proj_no AS project_internal_id,
        proj.proj_id AS project_number,
        po_hdr.po_id AS po_number,
        COALESCE(si.po_item_id,po_item.po_item_id) AS po_line_item,
        mmt_hdr_cdc.mmt_hdr_id AS shipment_number,
        expedite_item.expedite_item_id AS shipment_line_number,
        expedite_item.quan AS shipment_quantity,
        ROUND(expedite_item.quan * po_item.price, 2) AS total_shipment_price,
        expedite_item.deliv_place AS ship_to_location,
        'NULL' AS gbs_location_id,
        mmt_hdr_cdc.forecast_ex_work_dispatch_date AS promised_ship_date,
        expedite_item.expedite_item_no AS purchase_order_shipment_internal_id,
        mmt_hdr_cdc.def_date AS srn_created_date,
        mmt_hdr_cdc.actual_ex_work_dispatch_date AS actual_ship_date,
        po_hdr.po_no AS po_internal_id,
        po_hdr.ver AS po_revision,
        po_item.ver AS item_revision,
        proc_int_stat.proc_int_stat_id AS po_status,
        ei.expedite_item_id AS eo,
        si.batch_id AS so,
        po_item.po_item_no AS po_line_item_internal_id,
        cast(mmt_hdr_cdc.execution_date as date) AS etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id
    FROM
        {{ source('curated_erm', 'mmt_hdr') }}  mmt_hdr_cdc
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'expedite_item') }} WHERE is_current = 1) AS expedite_item ON mmt_hdr_cdc.mmt_hdr_no = expedite_item.mmt_hdr_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) AS po_item ON po_item.po_item_no = expedite_item.po_item_no
        INNER JOIN (
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
                        (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) pi
                        JOIN (SELECT * FROM {{ source('curated_erm', 'po_item') }} WHERE is_current = 1) si ON pi.po_item_no = si.batch_po_item_no
                ) sub ON pi1.po_item_no = sub.po_item_no
        ) si ON po_item.po_item_no = si.po_item_no
        INNER JOIN (SELECT * FROM {{ source('curated_erm', 'po_hdr') }} WHERE is_current = 1) po_hdr ON po_item.po_no = po_hdr.po_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proj') }} WHERE is_current = 1) proj ON mmt_hdr_cdc.proj_no = proj.proj_no  
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'proc_int_stat') }} WHERE is_current = 1) proc_int_stat ON po_hdr.proc_int_stat_no = proc_int_stat.proc_int_stat_no
        LEFT JOIN (SELECT * FROM {{ source('curated_erm', 'expedite_item') }} WHERE is_current = 1) ei ON mmt_hdr_cdc.mmt_hdr_no = ei.mmt_hdr_no AND po_item.po_item_no = ei.po_item_no
    WHERE
        proj.stat = '1'
        AND mmt_hdr_cdc.is_current=1

)

SELECT  * FROM shipment_data
