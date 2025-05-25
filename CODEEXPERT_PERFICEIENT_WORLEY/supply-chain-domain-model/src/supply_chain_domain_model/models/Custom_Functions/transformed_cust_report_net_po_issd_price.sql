{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_cust_report_net_po_issd_price/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT po_no
	   ,ver
      ,po_net_price as net_po_cost
FROM {{ source('curated_erm', 'po_hdr_hist') }}  phh
WHERE EXISTS (
			  SELECT 1
              FROM 
			     (
			     SELECT ph.po_no,MAX(ph.ver) ver
                 FROM {{ source('curated_erm', 'po_hdr_hist') }}  ph
                 WHERE NOT EXISTS 
				 (
				 SELECT 1 FROM {{ source('curated_erm', 'po_hdr') }} po_hdr 
				 WHERE po_hdr.po_no=ph.po_no 
				 AND po_hdr.ver=ph.ver 
				 AND po_hdr.proc_int_stat_no !=3
				 )
                 GROUP BY ph.po_no
				 ) ph
                 WHERE ph.po_no=phh.po_no
                 AND ph.ver=phh.ver
			 )