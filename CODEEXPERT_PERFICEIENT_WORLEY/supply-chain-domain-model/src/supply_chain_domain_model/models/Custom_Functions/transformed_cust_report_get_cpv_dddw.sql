{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_cust_report_get_cpv_dddw/', 
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        )
}}

select po_no,ph.ver,cvli.value as origin,cvli.description as origin_desc
		from {{ source('curated_erm', 'po_hdr_hist') }} ph
		LEFT JOIN {{ source('curated_erm', 'po_hdr_hist_cpv') }} hhc on ph.po_hdr_hist_no=hhc.po_hdr_hist_no
		LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli ON hhc.origin=value_list_item_no
		WHERE EXISTS (SELECT 1
					  FROM (SELECT po_no,MAX(ver) ver
							FROM {{ source('curated_erm', 'po_hdr_hist') }} po_hdr_hist
							WHERE NOT EXISTS (SELECT 1 FROM {{ source('curated_erm', 'po_hdr') }} po_hdr 
							WHERE po_hdr.po_no=po_hdr_hist.po_no AND po_hdr.ver=po_hdr_hist.ver AND po_hdr.proc_int_stat_no!=3)
							GROUP BY po_no
							) src	
              WHERE src.po_no=ph.po_no
              AND src.ver=ph.ver)
              