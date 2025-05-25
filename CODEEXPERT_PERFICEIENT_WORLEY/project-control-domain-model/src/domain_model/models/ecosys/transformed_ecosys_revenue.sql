{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_revenue/',
        options={"write.target-file-size-bytes": "268435456"},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

select
    project_id,
    cost_object_hierarchy_path_id,
    pa_date,
    pa_date2,
    snapshot_date,
    snapshot_date2,
    revenue_category,
    cast(actuals_to_date_all_revenue as float) actuals_to_date_all_revenue,
    cast(original_budget_revenue as float) original_budget_revenue,
    cast(approved_variations_revenue as float) approved_variations_revenue,
    cast(pending_variations_revenue as float) pending_variations_revenue,
    cast(current_budget_revenue as float) current_budget_revenue,
    cast(planned_revenue as float) planned_revenue,
    cast(earned_revenue as float) earned_revenue,
    cast(commitments_revenue as float) commitments_revenue,
    cast(actuals_to_date_revenue as float) actuals_to_date_revenue,
    cast(estimate_at_completion_revenue as float) estimate_at_completion_revenue,
    cast(estimate_to_complete_revenue as float) estimate_to_complete_revenue,
    cast(variance_revenue as float) variance_revenue,
    original_budget_start_date,
    original_budget_end_date,
    working_forecast_end_date,
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
                when cat.revenue_category is not null then trim(ct.pcs_revenue) end
            ) end as revenue_category,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.alternate_actual_costs end
            ) end as actuals_to_date_all_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when revenue_category = '1.4'
                    or dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.obsell end
                ) end
            ) end as original_budget_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when revenue_category = '1.4'
                    or dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.approved_changes_sell end
                ) end
            ) end as approved_variations_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when revenue_category = '1.4'
                    or dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.pending_changes_sell end
                ) end
            ) end as pending_variations_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when revenue_category = '1.4'
                    or dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.obsell + dt.approved_changes_sell end
                ) end
            ) end as current_budget_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.planned_sell end
                ) end
            ) end as planned_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when dt.billableyesnoname
                    or lower(trim(hdr.header_billingtypename)) = 'lump sum' then dt.earned_sell end
                ) end
            ) end as earned_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when revenue_category like '2.%' then (
                        case
                        when dt.billableyesnoname
                        or lower(trim(hdr.header_billingtypename)) <> 'lump sum' then dt.committed_sell end
                    ) end
                ) end
            ) end as commitments_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when lower(trim(hdr.header_billingtypename)) <> 'lump sum'
                    and (
                        revenue_category = '1.4'
                        or dt.billableyesnoname
                    )
                    then dt.alternate_actual_costs end
                ) end
            ) end as actuals_to_date_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when lower(trim(hdr.header_billingtypename)) <> 'lump sum'
                    and (
                        revenue_category = '1.4'
                        or dt.billableyesnoname
                    )
                    then dt.working_forecast_cfs
                    else (
                        case
                        when lower(trim(hdr.header_billingtypename)) = 'lump sum'
                        then dt.obsell + dt.approved_changes_sell end
                    ) end
                ) end
            ) end as estimate_at_completion_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then (
                    case
                    when lower(trim(hdr.header_billingtypename)) <> 'lump sum'
                    and (
                        revenue_category = '1.4'
                        or dt.billableyesnoname
                    )
                    then dt.working_forecast_cfs - dt.alternate_actual_costs end
                ) end
            ) end as estimate_to_complete_revenue,
            case
            when hdr.header_legacywbs = 'true'
            or (
                nvl(dt.wbs_id_namel02, ' ') not like 'X%'
                and nvl(dt.wbs_id_namel02, ' ') not like 'Y%'
            ) then (
                case
                when revenue_category <> ''
                and revenue_category is not null then estimate_at_completion_revenue - current_budget_revenue end
            ) end as variance_revenue,
            dt.original_budget_start_date as original_budget_start_date,
            dt.original_budget_end_date as original_budget_end_date,
            dt.working_forecast_end_date as working_forecast_end_date,
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
            join {{ ref('transformed_vw_ecosys_secorg_ecodata_latest') }} dt on dt.projectinternalid = hdr.header_projectinternalid
            and hdr.is_current = 1
            and dt.is_current = 1 and dt.status = 'active'
            and concat(dt.project_id, dt.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                    {{ ref('transformed_ecosys_status') }}        
            )
            join {{ source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') }} ct on ct.cost_object_category_value_internal_id = dt.ctinternalid
            and ct.is_current = 1
            join {{ source('curated_ecosys', 'curated_ecosys_calendar') }} cal on cal.calendar_enddate = to_date(substring(dt.pa_date, 7, 11), 'dd-MMM-yyyy')
            and cal.calendar_datetype = 'Month'
            left join {{ source('curated_ecosys', 'curated_ecosys_calendar') }}  cal2 
            on to_date(cal2.calendar_enddatevalue, 'yyyy - MM') = add_months(to_date(cal.calendar_enddatevalue, 'yyyy - MM'), -1 )
            and cal2.calendar_datetype = 'Month'
            left join  {{ source('curated_ecosys', 'curated_ecosys_snapidlist') }} snp2 on snp2.snapidlist_transactioncategoryid = cal2.calendar_enddate
            and snp2.is_current = 1
            left join {{ ref('transformed_vw_ecosys_secorg_ecodata_latest') }} dt2 on dt2.controlaccountinternalid = dt.controlaccountinternalid
            and dt2.is_current = 1 and dt2.status = 'active'
            and dt2.pa_date = snp2.snapidlist_transactioncategoryname
            and concat(dt2.project_id, dt2.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                 {{ ref('transformed_ecosys_status') }} 
            )
            left join  {{ source('curated_ecosys', 'curated_ecosys_revenue_category') }} cat on trim(cat.revenue_category) = trim(ct.pcs_revenue)
    ) z
