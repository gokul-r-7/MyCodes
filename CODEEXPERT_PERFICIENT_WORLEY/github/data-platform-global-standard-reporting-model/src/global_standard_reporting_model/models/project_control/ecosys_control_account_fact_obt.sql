{{
     config(
         materialized = "table",
         tags=["project_control"]
         )
}}

select
    hdr.project_internal_id,
    hdr.project_no,
    hdr.project_title,
    hdr.project_no || ' - ' || hdr.project_title as project_display_name,
    -- hdr.project_start_date,
    -- hdr.project_forecast_end_date,
    -- hdr.project_manager,
    -- hdr.project_controller,
    -- hdr.approval_status_id,
    dt.pa_date,
    to_date(substring(dt.pa_date, 7, 11), 'dd-mon-yyyy') as snapshot_date,
    hdr.project_no || '-' || to_char(to_date(substring(dt.pa_date, 7, 11), 'dd-mon-yyyy'),'yyyymmdd') as project_snapshot,
    dt.control_account_internal_id || '-' || to_char(to_date(substring(dt.pa_date, 7, 11), 'dd-mon-yyyy'),'yyyymmdd') as ca_snapshot,
    dt.secorgid,
    dt.control_account_internal_id,
    dt.ct_internal_id,
    dt.coa_internal_id,
    dt.billableyesnoname,
    case
        when hdr.billing_type_name is not null then hdr.billing_type_name
    else 'Unidentified' end as billing_type_name,
    hdr.legacy_wbs,
    -- hdr.ownerorganizationid as sec_org_id,
    -- org.path,
    -- org.region,
    -- org.sub_region,
    -- org.location_l1,
    -- org.location_l2,
    -- org.location_l3,
    -- org.location_l4,
    -- org.location_l5,
    -- org.location_l6,
    -- hdr.project_size_classification,
    -- case
    --     when hdr.project_size_classification is not null then trim(split_part(hdr.project_size_classification,'-',1)) 
    -- else 'Unidentified' end as project_size,
    -- hdr.project_risk_classification,
    -- case
    --     when hdr.project_risk_classification = '01' or hdr.project_risk_classification = '1' then 'Low'
    --     when hdr.project_risk_classification = '02' or hdr.project_risk_classification = '2' then 'Medium'
    --     when hdr.project_risk_classification = '03' or hdr.project_risk_classification = '3' then 'High'
    -- else 'Unidentified' end as project_risk,
    -- hdr.project_type,
    -- trim(split_part(hdr.project_type,'-',1)) as project_type_code,
    -- case
    --     when hdr.project_type is not null then trim(split_part(hdr.project_type,'-',2))
    -- else 'Unidentified' end as project_type_name,
    -- hdr.project_currency,
    trim(split_part(hdr.project_currency,'-',1)) as project_currency_code,
    trim(split_part(hdr.project_currency,'-',2)) as project_currency_name,
    -- hdr.service_type,
    -- trim(split_part(hdr.service_type,'-',1)) as service_type_code,
    -- case
    --     when hdr.service_type is not null then trim(split_part(hdr.service_type,'-',2))
    -- else 'Unidentified' end as service_type_name,
    -- hdr.customer_name,
    -- hdr.customer_type,
    -- trim(split_part(hdr.customer_type,'-',1)) as customer_type_code,
    -- case
    --     when hdr.customer_type is not null then trim(split_part(hdr.customer_type,'-',2))
    -- else 'Unidentified' end as customer_type_name,
    -- hdr.home_office_location,
    -- trim(split_part(hdr.home_office_location,'-',1)) as home_office_location_code,
    -- case
    --     when hdr.home_office_location is not null then trim(split_part(hdr.home_office_location,'-',2))
    -- else 'Unidentified' end as home_office_location_name,
    -- cast(hdr.project_funding as float) as project_funding,
    dt.costobjecthierarchypathid as cost_object_hierarchy_path_id,
    ct.cost_object_category_value_id,
    ct.cost_object_category_value_name,
    ct.display_name,
    ct.library,
    ct.level_1_description,
    ct.level_2_description,
    ct.level_3_description,
    ct.level_4_description,
    ct.level_5_description,
    wh.hours_category,
    case when hc.progress_category = '' then null else hc.progress_category end as hours_progress_category,
    wh.wpi_category as hours_wpi_category,
    wh.original_hours_category,
    hc.hours_parent_category_description,
    hc.hours_category_description,
    cast(wh.original_budget_work_hours as float) as original_budget_work_hours,
    cast(wh.approved_variations_work_hours as float) as approved_variations_work_hours,
    cast(wh.pending_variations_work_hours as float) as pending_variations_work_hours,
    cast(wh.current_budget_work_hours as float) as current_budget_work_hours,
    cast(wh.actuals_to_date_work_hours as float) as actuals_to_date_work_hours,
    cast(wh.estimate_to_complete_work_hours as float) as estimate_to_complete_work_hours,
    cast(wh.estimate_at_complete_work_hours as float) as estimate_at_complete_work_hours,
    cast(wh.variance_work_hours as float) as variance_work_hours,
    cast(wh.planned_work_hours as float) as planned_work_hours,
    cast(wh.earned_work_hours as float) as earned_work_hours,
    cast(wh.orig_hours_cat_current_budget_hours as float) as orig_hours_cat_current_budget_hours,
    cast(wh.orig_hours_cat_actual_hours as float) as orig_hours_cat_actual_hours,
    cast(wh.orig_hours_cat_planned_hours as float) as orig_hours_cat_planned_hours,
    cast(wh.orig_hours_cat_earned_hours as float) as orig_hours_cat_earned_hours,
    ec.cost_category,
    case when cc.progress_category = '' then null else cc.progress_category end as cost_progress_category,
    ec.original_cost_category,
    cc.cost_parent_category_description,
    cc.cost_category_description,
    cast(ec.original_budget_cost as float) as original_budget_cost,
    cast(ec.approved_variations_cost as float) as approved_variations_cost,
    cast(ec.pending_variations_cost as float) as pending_variations_cost,
    cast(ec.current_budget as float) as current_budget_cost,
    cast(ec.actuals_to_date_cost as float) as actuals_to_date_cost,
    cast(ec.estimate_to_complete_cost as float) as estimate_to_complete_cost,
    cast(ec.estimate_at_completion_cost as float) as estimate_at_completion_cost,
    cast(ec.planned_cost as float) as planned_cost,
    cast(ec.earned_cost as float) as earned_cost,
    cast(ec.variance_cost as float) as variance_cost,
    cast(ec.commitments_cost as float) as commitments_cost,
    cast(ec.orig_cost_cat_current_budget_cost as float) as orig_cost_cat_current_budget_cost,
    cast(ec.orig_cost_cat_actual_cost as float) as orig_cost_cat_actual_cost,
    cast(ec.orig_cost_cat_planned_cost as float) as orig_cost_cat_planned_cost,
    cast(ec.orig_cost_cat_earned_cost as float) as orig_cost_cat_earned_cost,
    er.revenue_category,
    case when rc.progress_category = '' then null else rc.progress_category end as revenue_progress_category,
    rc.revenue_parent_category_description,
    rc.revenue_category_description,
    cast(er.actuals_to_date_all_revenue as float) as actuals_to_date_all_revenue,
    cast(er.original_budget_revenue as float) as original_budget_revenue,
    cast(er.approved_variations_revenue as float) as approved_variations_revenue,
    cast(er.pending_variations_revenue as float) as pending_variations_revenue,
    cast(er.current_budget_revenue as float) as current_budget_revenue,
    cast(er.planned_revenue as float) as planned_revenue,
    cast(er.earned_revenue as float) as earned_revenue,
    cast(er.commitments_revenue as float) as commitments_revenue,
    cast(er.actuals_to_date_revenue as float) as actuals_to_date_revenue,
    cast(er.estimate_at_completion_revenue as float) as estimate_at_completion_revenue,
    cast(er.estimate_to_complete_revenue as float) as estimate_to_complete_revenue,
    cast(er.variance_revenue as float) as variance_revenue,
    oh.cost_category as cost_category_overhead,
    oh.original_cost_category as original_cost_category_overhead,
    cast(oh.original_budget_cost as float) as original_budget_overhead,
    cast(oh.approved_variations_cost as float) as approved_variations_overhead,
    cast(oh.pending_variations_cost as float) as pending_variations_overhead,
    cast(oh.current_budget as float) as current_budget_overhead,
    cast(oh.actuals_to_date_cost as float) as actuals_to_date_overhead,
    cast(oh.estimate_to_complete_cost as float) as estimate_to_complete_overhead,
    cast(oh.estimate_at_completion_cost as float) as estimate_at_completion_overhead,
    cast(oh.planned_cost as float) as planned_overhead,
    cast(oh.earned_cost as float) as earned_overhead,
    cast(oh.variance_cost as float) as variance_overhead,
    cast(oh.commitments_cost as float) as commitments_overhead,
    case when dt.costtypeid = 'X999' then cast(dt.alternateactualcosts as float) else null end as unallocated_actual_revenue,
    case when dt.costtypeid = 'X999' then cast(dt.actualcosts as float) else null end as unallocated_actual_cost,
    case when dt.costtypeid = 'X999' then cast(dt.actualunits as float) else null end as unallocated_actual_hours,  
    case
        when hdr.legacy_wbs = 'true' then ''
        else substring(trim(dt.wbsidnamel01), 1, 1) end as wbs01,
    case
        when hdr.legacy_wbs = 'true' then ''
        else substring(trim(dt.wbsidnamel02), 1, 1) end as wbs02,
    dt.toptaskid as top_task_id,
    gp.phase_id || ' - ' || gp.phase_name as wbsid_phase,
    gl.location_id || ' - ' || gl.location_name as wbsid_location,
    to_date(dt.execution_date,'YYYY-MM-DD hh:mi:ss') as execution_date
from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_header') }} hdr
join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_ecodata') }} dt ON dt.project_internal_id = hdr.project_internal_id
            and concat(dt.projectid, dt.snapshotid) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                    {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_status') }}
            )
join {{ ref('ecosys_cost_type_pcs_mapping') }} ct
    on ct.cost_object_category_value_internal_id = dt.ct_internal_id
join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_calendar') }} cal
    on cal.end_date = to_date(substring(dt.pa_date, 7, 11), 'dd-mon-yyyy') and cal.date_type = 'Month'
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_work_hours') }} wh
    on wh.control_account_internal_id = dt.control_account_internal_id and wh.pa_date = dt.pa_date
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_cost') }} ec
    on ec.control_account_internal_id = dt.control_account_internal_id and ec.pa_date = dt.pa_date
    and ec.cost_category not like '12.%'
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_revenue') }} er
    on er.control_account_internal_id = dt.control_account_internal_id and er.pa_date = dt.pa_date
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_cost') }} oh
    on oh.control_account_internal_id = dt.control_account_internal_id and oh.pa_date = dt.pa_date
    and oh.cost_category like '12.%'
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_cost_category') }} cc
    on cc.cost_category = ec.cost_category
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_hours_category') }} hc
    on hc.hours_category = wh.hours_category
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_revenue_category') }} rc
    on rc.revenue_category = er.revenue_category
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_global_phases') }} gp
    on gp.phase_id = case when hdr.legacy_wbs = 'true' then '' else substring(trim(dt.wbsidnamel01), 1, 1) end
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_global_locations') }} gl
    on gl.location_id = case when hdr.legacy_wbs = 'true' then '' else substring(trim(dt.wbsidnamel02), 1, 1) end
where cal.end_date >= add_months(current_date, -13)