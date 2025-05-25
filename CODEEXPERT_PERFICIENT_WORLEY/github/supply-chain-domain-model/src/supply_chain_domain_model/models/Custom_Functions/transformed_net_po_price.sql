{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_net_po_price/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT 
    ph.po_no,
    round(
        CASE 
            WHEN ph.disc_percent IS NULL OR ph.disc_percent = 0 
            THEN sum(quan * COALESCE(net_price, 0) * COALESCE(factor, 1))
            ELSE sum(quan * COALESCE(net_price, 0) * COALESCE(factor, 1)) - 
                 sum(quan * COALESCE(net_price, 0) * COALESCE(factor, 1)) * ph.disc_percent/100 
        END, 
        2
    ) as po_gross_value

FROM {{ source('curated_erm', 'po_hdr') }} ph 
JOIN {{ source('curated_erm', 'po_item') }} pi 
    ON ph.po_no = pi.po_no
    where ph.is_current = 1 and pi.is_current = 1
GROUP BY 
    ph.po_no,
    ph.ver,
    ph.disc_percent