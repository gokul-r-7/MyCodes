{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_cost/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select
    project_id,
    cost_object_hierarchy_path_id,
    pa_date,
    pa_date2,
    snapshot_date,
    snapshot_date2,
    cost_category_new as cost_category,
    cast (original_budget_cost as float) as original_budget_cost,
    cast (approved_variations_cost as float) as approved_variations_cost,
    cast (pending_variations_cost as float) as pending_variations_cost,
    cast (current_budget as float) as current_budget,
    cast (actuals_to_date_cost as float) as actuals_to_date_cost,
    cast (estimate_to_complete_cost as float) as estimate_to_complete_cost,
    cast (estimate_at_completion_cost as float) as estimate_at_completion_cost,
    cast (planned_cost as float) as planned_cost,
    cast (earned_cost as float) as earned_cost,
    cast (variance_cost as float) as variance_cost,
    cast (commitments_cost as float) as commitments_cost,
    original_cost_category,
    cast (orig_cost_cat_current_budget_cost as float) as orig_cost_cat_current_budget_cost,
    cast (orig_cost_cat_actual_cost as float) as orig_cost_cat_actual_cost,
    cast (orig_cost_cat_planned_cost as float) as orig_cost_cat_planned_cost,
    cast (orig_cost_cat_earned_cost as float) as orig_cost_cat_earned_cost,
    billable_yes_no_name,
    billing_type_name,
    legacy_wbs,
    cost_type_internal_id,
    control_account_internal_id,
    project_internal_id,
    wbs01,
    wbs02,
    top_task_id,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    (
        select
            hdr.header_projectid as project_id,
            dt.cost_object_hierarchy_path_id as cost_object_hierarchy_path_id,
            dt.pa_date as pa_date,
            dt2.pa_date as pa_date2,
            to_date(substring(dt.pa_date, 7, 11), 'dd-MMM-yyyy') as snapshot_date,
            to_date(substring(dt2.pa_date, 7, 11), 'dd-MMM-yyyy') as snapshot_date2,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when lower(trim(hdr.header_billingtypename)) = 'lump sum'
                or dt.billableyesnoname = true
                or trim(ct.pcs_cost) = '6.4'
                or trim(ct.pcs_cost) like  '10.%'
                or trim(ct.pcs_cost) like  '12.%' then (
                    case
                    when cat1.cost_category is not null then trim(ct.pcs_cost) end
                )
                else (
                    case
                    when cat2.cost_category is not null then trim(ct.pcs_nb_cost) end
                ) end
            ) end as cost_category_new,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when cost_category_new = '6.4'
                or cost_category_new like  '10.%'
                or (
                    cost_category_new like  '11.%'
                    and lower(trim(hdr.header_billingtypename)) <> 'lump sum'
                    and dt.billableyesnoname = false
                )
                or (
                    cost_category_new like  '12.%'
                    and dt.billableyesnoname = false
                )
                or (
                    cost_category_new <> ''
                    and ct.pcs_cost is not null
                    and (lower(trim(hdr.header_billingtypename)) = 'lump sum'
                    or dt.billableyesnoname = true
                )
                ) then 1
                else 0 end
            ) end as valid_cost,
            case
            when valid_cost = 1 then dt.obcost
            else null end as original_budget_cost,
            case
            when valid_cost = 1 then dt.approved_changes_cost
            else null end as approved_variations_cost,
            case
            when valid_cost = 1 then dt.pending_changes_cost
            else null end as pending_variations_cost,
            case
            when valid_cost = 1 then dt.obcost + dt.approved_changes_cost
            else null end as current_budget,
            case
            when valid_cost = 1 then dt.actual_costs
            else null end as actuals_to_date_cost,
            case
            when valid_cost = 1 then dt.working_forecast_cfc - dt.actual_costs
            else null end as estimate_to_complete_cost,
            case
            when valid_cost = 1 then dt.working_forecast_cfc
            else null end as estimate_at_completion_cost,
            case
            when valid_cost = 1 then dt.planned_cost
            else null end as planned_cost,
            case
            when valid_cost = 1 then dt.earned_cost
            else null end as earned_cost,
            case
            when valid_cost = 1 then dt.working_forecast_cfc - (dt.obcost + dt.approved_changes_cost)
            else null end as variance_cost,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when trim(ct.pcs_cost) like '7.%' then dt.committed_cost end
            ) end as commitments_cost,
            ct.pcs_cost as original_cost_category,
            dt.current_budget_cost as orig_cost_cat_current_budget_cost,
            dt.actual_costs as orig_cost_cat_actual_cost,
            dt.planned_cost as orig_cost_cat_planned_cost,
            dt.earned_cost as orig_cost_cat_earned_cost,
            dt.billableyesnoname as billable_yes_no_name,
            hdr.header_billingtypename as billing_type_name,
            hdr.header_legacywbs as legacy_wbs,
            dt.ctinternalid as cost_type_internal_id,
            dt.controlaccountinternalid as control_account_internal_id,
            dt.projectinternalid as project_internal_id,
            case
            when hdr.header_legacywbs = 'true' then ''
            else substring(trim(dt.wbs_id_namel01), 1, 1) end as wbs01,
            case
            when hdr.header_legacywbs = 'true' then ''
            else substring(trim(dt.wbs_id_namel02), 1, 1) end as wbs02,
            dt.top_task_id as top_task_id
        from
	    {{ source('curated_ecosys', 'curated_header') }} hdr
            join {{ ref('transformed_vw_ecosys_secorg_ecodata_latest') }} dt
	    ON dt.projectinternalid = hdr.header_projectinternalid
            and dt.is_current = 1
            and hdr.is_current = 1
            and concat(dt.PROJECT_ID, dt.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                    {{ ref('transformed_ecosys_status') }}
            )
            join {{ source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') }} ct 
	    ON ct.cost_object_category_value_internal_id = dt.ctinternalid
            and ct.is_current = 1
           join {{ source('curated_ecosys', 'curated_ecosys_calendar') }} cal
                on cal.calendar_enddate = to_date(substring(dt.pa_date, 7, 17), 'dd-MMM-yyyy')
                and cal.calendar_datetype = 'Month'
            left join {{ source('curated_ecosys', 'curated_ecosys_calendar') }}  cal2 
                on to_date(cal2.calendar_enddatevalue, 'yyyy - MM') = add_months(to_date(cal.calendar_enddatevalue, 'yyyy - MM'), -1 )
                and cal2.calendar_datetype = 'Month'
            left join {{ source('curated_ecosys', 'curated_ecosys_snapidlist') }} snp2 ON snp2.snapidlist_transactioncategoryid = cal2.calendar_enddate
            and snp2.is_current = 1
            left join {{ source('curated_ecosys', 'curated_ecosys_ecodata') }} dt2 ON dt2.controlaccountinternalid = dt.controlaccountinternalid
            and dt2.is_current = 1
            and dt2.pa_date = snp2.snapidlist_transactioncategoryname
            and concat(dt.PROJECT_ID, dt.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                   {{ ref('transformed_ecosys_status') }}
            )
            left join {{ source('curated_ecosys', 'curated_ecosys_cost_category') }} cat1 ON trim(cat1.cost_category) = trim(ct.pcs_cost)
            left join {{ source('curated_ecosys', 'curated_ecosys_cost_category') }} cat2 ON trim(cat2.cost_category) = trim(ct.pcs_nb_cost)
    ) z
order by
    project_internal_id,
    control_account_internal_id,
    snapshot_date,
    cost_type_internal_id,
    cost_category
