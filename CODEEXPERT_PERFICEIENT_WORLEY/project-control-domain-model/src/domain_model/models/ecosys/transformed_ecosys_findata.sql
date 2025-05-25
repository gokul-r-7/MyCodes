{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_findata/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select
    findata_projectinternalid as project_internal_id,
    findata_costobjectid as cost_object_id,
    cast(findata_transactiondate as date) as transaction_date,
    cast(findata_recrevenue as float) as rec_revenue,
    cast(findata_invtodate as float) as invd_to_date,
    cast(findata_recdtodate as float) as recd_to_date,
    cast(findata_wip as float) as wip,
    cast(findata_outstandingtotal as float) as outstanding_total,
    cast(findata_outstandingcurrent as float) as outstanding_current,
    cast(findata_outstanding130 as float) as outstanding_1_to_30_days,
    cast(findata_outstanding3160 as float) as outstanding_31_to_60_days,
    cast(findata_outstanding61above as float) as outstanding_61_plus_days,
    '' as terms,
    cast(findata_dwu as float) as dwu,
    cast(findata_ddo as float) as ddo,
    cast(findata_dso as float) as dso,
    cast(findata_contingencybreakdownservice as float) as contingency_breakdown_service,
    cast(findata_contingencybreakdownproc as float) as contingency_breakdown_proc,
    cast(findata_contingencybreakdownconstruction as float) as contingency_breakdown_construction,
    cast(findata_contingencybreakdownother as float) as contingency_breakdown_other,
    cast(findata_contingencybreakdowncloseout as float) as contingency_breakdown_closeout,
    findata_costobjectid as secorgid,
    findata_pcscomments as pcscomments,
    findata_pcsstatus as pcsstatus,
    execution_date ,
    source_system_name ,
    primary_key ,
    is_current ,
    eff_start_date ,
    eff_end_date,
   	{{run_date}} as created_date,
	{{run_date}} as updated_date,
	{{ generate_load_id(model) }} as load_id
from
{{ source('curated_ecosys', 'curated_ecosys_findata') }}
where
is_current = 1
