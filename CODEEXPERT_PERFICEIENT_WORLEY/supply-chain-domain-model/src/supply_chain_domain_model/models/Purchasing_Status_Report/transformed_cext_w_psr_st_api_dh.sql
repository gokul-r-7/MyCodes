{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_cext_w_psr_st_api_dh/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



select 
    api_dh.proj_no,
    api_dh.proj_id,
    api_dh.proj_id_descr,
    api_dh.proc_sch_hdr_no,
    api_dh.proc_sch_hdr_id,
    api_dh.ver,
    api_dh.title,
    api_dh.req_originator,
    api_dh.buyer,
    api_dh.expediter,
    api_dh.criticality,
    api_dh.discipline,
    api_dh.supplier,
    api_dh.potential_suppliers,
    api_dh.budget,
    api_dh.rem,
    api_dh.proj_planned_tmr_hdr_id,
    api_dh.mr_tmr,
    api_dh.po_number,
    api_dh.po_gross_value,
    api_dh.deliv_deadline,
    api_dh.lead_time,
    api_dh.proc_sch_stat_no,
    api_dh.sub_project,
    api_dh.cf_new_float,
    api_dh.proc_mstone_id,
    api_dh.proc_milestone_id,
    api_dh.m_planned_date,
    api_dh.m_forecast_date,
    api_dh.m_actual_date,
    api_dh.m_duration,
    api_dh.m_float,
    api_dh.milestone_comment,
    api_dh.schedule_item_type_no,
    api_dh.calc_seq,
    api_dh.remarks,
    api_dh.contract_owner,
    api_dh.contract_holder,
    api_dh.project_region,
    api_dh.po_id_csv,
    api_dh.customer_po_no,
    dd.descr as rr_discipline,
    ccpv.client_ref_no,
    cast(api_dh.etl_load_date as date) as etl_load_date,
    {{ run_date }} as model_created_date,
    {{ run_date }} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id 
from {{ ref('transformed_psr_cext_w_proc_status_50037_api_dh') }} api_dh
left join {{ source('curated_erm', 'proj_planned_tmr_hdr') }} ppth 
    on api_dh.proj_no = ppth.proj_no 
    and api_dh.proc_sch_hdr_id = ppth.proj_planned_tmr_hdr_id
	and ppth.is_current = 1 
left join {{ source('curated_erm', 'draw_discipline') }} dd 
    on ppth.draw_discipline_no = dd.draw_discipline_no
		and dd.is_current = 1 
left join {{ source('curated_erm', 'proc_sch_hdr_cpv') }} ccpv 
    on api_dh.proc_sch_hdr_no = ccpv.proc_sch_hdr_no
			and ccpv.is_current = 1 
            