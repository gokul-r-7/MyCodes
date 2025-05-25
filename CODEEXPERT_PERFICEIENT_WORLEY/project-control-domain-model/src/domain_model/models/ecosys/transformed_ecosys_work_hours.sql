{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_work_hours/',
        table_properties={'write.target-file-size-bytes': '268435456'},
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
    hours_category_new as hours_category,
    wpi_category,
    cast (original_budget_work_hours as float) as original_budget_work_hours,
    cast (approved_variations_work_hours as float) as approved_variations_work_hours,
    cast (pending_variations_work_hours as float) as pending_variations_work_hours,
    cast (current_budget_work_hours as float) as current_budget_work_hours,
    cast (actuals_to_date_work_hours as float) as actuals_to_date_work_hours,
    cast (estimate_to_complete_work_hours as float) as estimate_to_complete_work_hours,
    cast (estimate_at_complete_work_hours as float) as estimate_at_complete_work_hours,
    cast (variance_work_hours as float) as variance_work_hours,
    cast (planned_work_hours as float) as planned_work_hours,
    cast (earned_work_hours as float) as earned_work_hours,
    original_hours_category,
    cast (orig_hours_cat_current_budget_hours as float) as orig_hours_cat_current_budget_hours,
    cast (orig_hours_cat_actual_hours as float) as orig_hours_cat_actual_hours,
    cast (orig_hours_cat_planned_hours as float) as orig_hours_cat_planned_hours,
    cast (orig_hours_cat_earned_hours as float) as orig_hours_cat_earned_hours,
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
            when (
                hdr.header_legacywbs = 'false'
                and (
                    dt.billableyesnoname = true
                    or hdr.header_billingtypename = 'Lump Sum'
                )
                and (
                    nvl(dt.wbs_id_namel02, ' ') like 'B%'
                    or nvl(dt.wbs_id_namel02, ' ') like 'C%'
                    or nvl(dt.wbs_id_namel02, ' ') like 'Y%'
                )
            ) then (
                case
                when cat1.hours_category is not null then substring(dt.wbs_id_namel02, 1, 1) end
            )
            when (
                hdr.header_billingtypename <> 'Lump Sum'
                and dt.billableyesnoname = false
            ) then (
                case
                when cat2.hours_category is not null then trim(ct.pcs_nb_hours) end
            )
            else (
                case
                when cat3.hours_category is not null then trim(ct.pcs_hours) end
            ) end as hours_category_new,
            case
            when (
                hdr.header_legacywbs = 'false'
                and dt.billableyesnoname = true
                and (
                    nvl(dt.wbs_id_namel02, ' ') like 'B%'
                    or nvl(dt.wbs_id_namel02, ' ') like 'C%'
                    or nvl(dt.wbs_id_namel02, ' ') like 'Y%'
                )
            ) then (
                case
                when cat3.hours_category is not null then trim(ct.pcs_hours) end
            )
            when (
                hdr.header_billingtypename <> 'Lump Sum'
                and dt.billableyesnoname = false
            ) then null
            else (
                case
                when cat3.hours_category is not null then trim(ct.pcs_hours) end
            ) end as wpi_category,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.original_budget
            else null end as original_budget_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.approved_changes
            else null end as approved_variations_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.pending_changes
            else null end as pending_variations_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.original_budget + dt.approved_changes
            else null end as current_budget_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.actual_units
            else null end as actuals_to_date_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.CFHOURS - dt.actual_units
            else null end as estimate_to_complete_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.CFHOURS
            else null end as estimate_at_complete_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.CFHOURS - (dt.original_budget + dt.approved_changes)
            else null end as variance_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.planned_hours
            else null end as planned_work_hours,
            case
            when (
                (
                    hours_category_new = 'L3.3'
                    and hdr.header_billingtypename <> 'Lump Sum'
                    and dt.billableyesnoname = false
                )
                or (
                    hours_category_new is not null
                    and hours_category_new <> ''
                    and (
                        dt.billableyesnoname = true
                        or hdr.header_billingtypename = 'Lump Sum'
                    )
                )
            ) then dt.earned_hours
            else null end as earned_work_hours,
            ct.pcs_hours as original_hours_category,
            dt.current_budget_hours as orig_hours_cat_current_budget_hours,
            dt.actual_units as orig_hours_cat_actual_hours,
            dt.planned_hours as orig_hours_cat_planned_hours,
            dt.earned_hours as orig_hours_cat_earned_hours,
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
            and dt.is_current = 1
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
                on ct.cost_object_category_value_internal_id = dt.ctinternalid
            and ct.is_current = 1
            join {{ source('curated_ecosys', 'curated_ecosys_calendar') }} cal
                on cal.calendar_enddate = to_date(substring(dt.pa_date, 7, 17), 'dd-MMM-yyyy')
                and cal.calendar_datetype = 'Month'
            left join {{ source('curated_ecosys', 'curated_ecosys_calendar') }}  cal2 
                on to_date(cal2.calendar_enddatevalue, 'yyyy - MM') = add_months(to_date(cal.calendar_enddatevalue, 'yyyy - MM'), -1 )
                and cal2.calendar_datetype = 'Month'
            left join {{ source('curated_ecosys', 'curated_ecosys_snapidlist') }} snp2
                on snp2.snapidlist_transactioncategoryid = cal2.calendar_enddate
                and snp2.is_current = 1
            left join {{ source('curated_ecosys', 'curated_ecosys_ecodata') }} dt2
                on dt2.controlaccountinternalid = dt.controlaccountinternalid
                and dt2.pa_date = snp2.snapidlist_transactioncategoryname
                and dt2.is_current = 1
                and concat(dt2.PROJECT_ID, dt2.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                    {{ ref('transformed_ecosys_status') }}
            )
            left join {{ source('curated_ecosys', 'curated_ecosys_hours_category') }} cat1 on trim(cat1.hours_category) = substring(dt.wbs_id_namel02, 1, 1)
            left join {{ source('curated_ecosys', 'curated_ecosys_hours_category') }} cat2 on trim(cat2.hours_category) = trim(ct.pcs_nb_hours)
            left join {{ source('curated_ecosys', 'curated_ecosys_hours_category') }} cat3 on trim(cat3.hours_category) = trim(ct.pcs_hours)
    ) z
