{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_inv_tax_fright/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT   ih.po_no
 ,SUM(ii.amount) inv_tax_fright
 FROM {{ source('curated_erm', 'inv_item') }} ii
 JOIN {{ source('curated_erm', 'inv_hdr') }} ih ON ih.inv_hdr_no=ii.inv_hdr_no
 AND ii.po_item_no IS NULL
 AND ih.inv_hdr_stat_no IN (7,9)
 and  po_pay_plan_no IS NULL
 GROUP BY ih.po_no       