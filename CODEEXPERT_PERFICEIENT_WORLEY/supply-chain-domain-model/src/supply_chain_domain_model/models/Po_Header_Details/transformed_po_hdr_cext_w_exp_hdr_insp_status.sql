{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_po_hdr_cext_w_exp_hdr_insp_status/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with exp_item_status as 
(
  select
        item_status.expedite_hdr_no,
        count(item_status.expedite_item_no)     as all_items,
        sum(item_status.unassigned)             as unassigned_items,
        sum(item_status.pending)                as pending_items,
        sum(item_status.failed)                 as failed_items,
        sum(item_status.released)               as released_items
    from (
        select
			expedite_item_no,
            expedite_hdr_no,
            1 unassigned,
            0 pending,
            0 failed,
            0 released
        from {{ source('curated_erm', 'expedite_item') }} expedite_item
		where  expedite_item.is_current = 1 and expedite_item.is_deliverable = 1
    ) item_status
    group by
        item_status.expedite_hdr_no
)


select expedite_hdr.expedite_hdr_no as expedite_hdr_no,
       exp_item_status.all_items,
       exp_item_status.unassigned_items,
       exp_item_status.pending_items,
       exp_item_status.failed_items,
       exp_item_status.released_items,
	   2 as has_technical_deviation,
		cast(expedite_hdr.execution_date as date) as etl_load_date,
		{{run_date}} as model_created_date,
		{{run_date}} as model_updated_date,
		{{ generate_load_id(model) }} as model_load_id	 
  from {{ source('curated_erm', 'expedite_hdr') }} expedite_hdr
  left outer join exp_item_status
    on exp_item_status.expedite_hdr_no = expedite_hdr.expedite_hdr_no
where expedite_hdr.is_current = 1