{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_net_po_issd_price/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}




SELECT 
  ph.po_no,
  ph.ver,
  ROUND(
    CASE 
      WHEN ph.disc_percent IS NULL OR ph.disc_percent = 0 THEN 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1))
      ELSE 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1)) - 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1)) * ph.disc_percent / 100 
    END, 2
  ) AS po_gross_value
FROM 
  {{ source('curated_erm', 'po_hdr') }} ph
JOIN 
  {{ source('curated_erm', 'po_item') }} pi ON ph.po_no = pi.po_no
WHERE 
  ph.is_current = 1
  AND pi.is_current = 1
GROUP BY 
  ph.po_no, ph.ver, ph.disc_percent

UNION ALL

SELECT 
  ph.po_no,
  ph.ver,
  ROUND(
    CASE 
      WHEN ph.disc_percent IS NULL OR ph.disc_percent = 0 THEN 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1))
      ELSE 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1)) - 
        SUM(quan * NVL(net_price, 0) * NVL(factor, 1)) * ph.disc_percent / 100 
    END, 2
  ) AS po_gross_value
FROM 
  {{ source('curated_erm', 'po_hdr_hist') }} ph
JOIN 
  {{ source('curated_erm', 'po_hdr_hist') }} ph1 ON ph.po_no = ph1.po_no
JOIN 
  {{ source('curated_erm', 'po_item_hist') }} pi ON ph1.po_hdr_hist_no = pi.po_hdr_hist_no
LEFT JOIN 
  {{ source('curated_erm', 'po_hdr') }} ph2 ON ph2.po_no = ph.po_no AND ph2.ver = ph.ver AND ph2.is_current = 1
WHERE 
  ph2.po_no IS NULL
  AND ph.is_current = 1
  AND ph1.is_current = 1
  AND pi.is_current = 1
  AND pi.ver = (
    SELECT MAX(ih2.ver)
    FROM {{ source('curated_erm', 'po_item_hist') }} ih2
    WHERE ih2.po_item_no = pi.po_item_no
    AND ih2.ver <= ph.ver
    AND ih2.is_current = 1
  )
GROUP BY 
  ph.po_no, ph.ver, ph.disc_percent
