{{
     config(
         materialized = "table",
         tags=["project_control"]
         )
}}

with 
cost_types as 
(
    select cost_object_category_value_internal_id cost_type_id, 
           case when pcs_revenue != 'na do not put on pcs' and pcs_revenue not like '12.%' then pcs_revenue end as pcs_revenue,        
           case when pcs_cost != 'NA Do not put on PCS' then pcs_cost end as pcs_cost, 
           case when pcs_nb_cost != 'NA Do not put on PCS' then pcs_nb_cost end as pcs_nb_cost, 
           case when pcs_hours != 'NA Do not put on PCS' then pcs_hours end as pcs_hours,        
           case when pcs_nb_hours!= 'NA Do not put on PCS' then pcs_nb_hours end as pcs_nb_hours, 
           case when pcs_cost like '12%' then true else false end as is_overhead 
    from {{ ref('ecosys_cost_type_pcs_mapping') }}
),
ca_data as 
(
    select 
        h.project_no,
        trim(split_part(project_currency, '-', 1)) currency_code,         
        h.legacy_wbs, 
        cal.end_date,
        case when h.billing_type_name = 'Lump Sum' then true else false end project_is_lump_sum, 
        case when h.legacy_wbs = 'false' then substring(wbsidnamel02, 1, 1) end location_code,
        case when h.legacy_wbs = 'false' then substring(wbsidnamel01, 1, 1) end phase_code,
        case when d.billableyesnoname = true then true else false end  ca_is_billable,
        case when location_code is null or (location_code != 'Y' and location_code != 'X') then true else false end is_included_location, 
        case when is_included_location then ct.pcs_revenue end as revenue_category, 
        case when is_included_location and (project_is_lump_sum or ca_is_billable or ct.pcs_cost ='6.4' or ct.pcs_cost like '10.%' or ct.pcs_cost like '12.%')
            then ct.pcs_cost 
            else ct.pcs_nb_cost
        end as cost_category,      
        case when is_included_location and (project_is_lump_sum or ca_is_billable)
            then ct.pcs_hours
            else ct.pcs_nb_hours
        end as hours_category, 
        
        d.costobjecthierarchypathid,
        d.ct_internal_id as cost_type_internal_id,
    	d.originalbudgetstartdate as original_budget_start_date,
    	d.originalbudgetenddate as original_budget_end_date,
    	d.workingforecastenddate as working_forecast_end_date,

        -- identify non sub contract cas
        case when cost_category <> '7.3' and revenue_category <> '2.2'then 1 else 0 end non_sub_contract_ca_count, 
        case when ca_is_billable = true then 1 else 0 end billable_ca_count,
        
        --revenue
        case when revenue_category is not null and (ca_is_billable or project_is_lump_sum or revenue_category = '1.4') then true else false end revenue_is_included,
    	case when revenue_is_included then d.obsell end as original_budget_revenue,
    	case when revenue_is_included then d.approvedchangessell end as approved_variations_revenue,
    	case when revenue_is_included then d.pendingchangessell end as pending_variations_revenue,	
    	case when revenue_is_included then d.alternateactualcosts end as actuals_to_date_revenue,
    	case when revenue_is_included then case when project_is_lump_sum then d.obsell + d.approvedchangessell else d.workingforecastcfs end end as estimate_at_completion_revenue,
    	case when revenue_is_included then d.plannedsell end as planned_revenue,
    	case when revenue_is_included then d.earnedsell end as earned_revenue,
    	case when revenue_is_included and revenue_category like '2%' then d.obsell end as commitments_revenue, 
        
        -- costs
        case when cost_category is not null and (
                   cost_category = '6.4'
                or cost_category like '10.%'
                or (cost_category like '11.%' and ca_is_billable = false and project_is_lump_sum = false) 
                or (cost_category like '12.%' and ca_is_billable = false) 
                or (ca_is_billable or project_is_lump_sum)
           ) 
        then true
        else false 
        end as cost_is_included,       
        case when cost_is_included and is_overhead = false then d.obcost end as original_budget_cost,
        case when cost_is_included and is_overhead = false then d.approvedchangescost end as approved_variations_cost,
        case when cost_is_included and is_overhead = false then d.pendingchangescost end as pending_variations_cost,
        case when cost_is_included and is_overhead = false  then d.actualcosts end as actuals_to_date_cost,
        case when cost_is_included and is_overhead = false  then d.workingforecastcfc end as estimate_at_completion_cost,     
        case when cost_is_included and is_overhead = false  then d.plannedcost end as planned_cost,        
        case when cost_is_included and is_overhead = false  then d.earnedcost end as earned_cost,
        case when cost_category like '7.%' then d.committedcost end as commitments_cost,
        
        -- overheads
        case when cost_is_included and is_overhead then d.obcost end as original_budget_overhead,
        case when cost_is_included and is_overhead then d.approvedchangescost end as approved_variations_overhead,
        case when cost_is_included and is_overhead then d.pendingchangescost end as pending_variations_overhead,
        case when cost_is_included and is_overhead then d.actualcosts end as actuals_to_date_overhead,
        case when cost_is_included and is_overhead then d.workingforecastcfc end as estimate_at_completion_overhead,     
        case when cost_is_included and is_overhead then d.plannedcost end as planned_overhead,        
        case when cost_is_included and is_overhead then d.earnedcost end as earned_overhead, 
        
        --hours
        case when hours_category is not null then true else false end hours_are_included, 
        case when hours_are_included then d.originalbudgeth end as original_budget_work_hours,
        case when hours_are_included then d.approvedchangesh end as approved_variations_work_hours,
        case when hours_are_included then d.pendingchangesh end as pending_variations_work_hours,
        case when hours_are_included then d.actualunits end as actuals_to_date_work_hours,
        case when hours_are_included then d.cfhours end as estimate_at_completion_work_hours,     
        case when hours_are_included then d.plannedhours end as planned_work_hours,        
        case when hours_are_included then d.earnedhours end as earned_work_hours
        
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_header') }} h, 
         {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_ecodata') }} d,
         cost_types ct, 
         {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_calendar') }} cal		
    where h.project_no = d.projectid
    and  d.ct_internal_id = ct.cost_type_id
    and  cal.end_date = to_date(substring(d.pa_date,7,11),'dd-mon-yyyy')
    and  cal.date_type='Month'
    and  end_date > add_months(current_date, -13)
    -- and  d.projectid not in ('Nov-2025','Portfolio 15')
    -- and  d.pa_date not in ('HOUS-B2d90','412.5692','418095-50786OB','5127898','325933.2459','0.0000','87832.5212','5-D-1C-COM-K-85-85700-006-MB-161-9131')
),
project_level as (
    select project_no, end_date, 
           
           min(original_budget_start_date) original_budget_start_date,
           max(original_budget_end_date) original_budget_end_date, 
           max(working_forecast_end_date) working_forecast_end_date,

           sum(non_sub_contract_ca_count) non_sub_contract_ca_count,
           sum(billable_ca_count) billable_ca_count,
              
           sum(original_budget_revenue) original_budget_revenue, 
           sum(actuals_to_date_revenue) actuals_to_date_revenue,
           sum(approved_variations_revenue) approved_variations_revenue,
           sum(pending_variations_revenue) pending_variations_revenue,
           sum(estimate_at_completion_revenue) estimate_at_completion_revenue,
           sum(planned_revenue) planned_revenue,
           sum(earned_revenue) earned_revenue, 

           sum(original_budget_cost) original_budget_cost, 
           sum(actuals_to_date_cost) actuals_to_date_cost,
           sum(approved_variations_cost) approved_variations_cost,
           sum(pending_variations_cost) pending_variations_cost,
           sum(estimate_at_completion_cost) estimate_at_completion_cost,
           sum(planned_cost) planned_cost,
           sum(earned_cost) earned_cost, 

           sum(original_budget_work_hours) original_budget_work_hours, 
           sum(actuals_to_date_work_hours) actuals_to_date_work_hours,
           sum(approved_variations_work_hours) approved_variations_work_hours,
           sum(pending_variations_work_hours) pending_variations_work_hours,
           sum(estimate_at_completion_work_hours) estimate_at_completion_work_hours,
           sum(planned_work_hours) planned_work_hours,
           sum(earned_work_hours) earned_work_hours
           
    from ca_data
    group by project_no, end_date),
projectdata as
(
    select
        project_no as projectid,
        snapshot_date,
        project_currency_code,
        coalesce(sum(original_budget_work_hours),0) as project_original_budget_work_hours,
        coalesce(sum(approved_variations_work_hours),0) as project_approved_variations_work_hours,
        coalesce(sum(pending_variations_work_hours),0) as project_pending_variations_work_hours,
        coalesce(sum(current_budget_work_hours),0) as project_current_budget_work_hours,
        coalesce(sum(actuals_to_date_work_hours),0) as project_actuals_to_date_work_hours,
        coalesce(sum(estimate_to_complete_work_hours),0) as project_estimate_to_complete_work_hours,
        coalesce(sum(estimate_at_complete_work_hours),0) as project_estimate_at_complete_work_hours,
        coalesce(sum(variance_work_hours),0) as project_variance_work_hours,
        coalesce(sum(planned_work_hours),0) as project_planned_work_hours,
        coalesce(sum(earned_work_hours),0) as project_earned_work_hours,
        coalesce(sum(original_budget_cost),0) as project_original_budget_cost,
        coalesce(sum(approved_variations_cost),0) as project_approved_variations_cost,
        coalesce(sum(pending_variations_cost),0) as project_pending_variations_cost,
        coalesce(sum(current_budget_cost),0) as project_current_budget_cost,
        coalesce(sum(actuals_to_date_cost),0) as project_actuals_to_date_cost,
        coalesce(sum(estimate_to_complete_cost),0) as project_estimate_to_complete_cost,
        coalesce(sum(estimate_at_completion_cost),0) as project_estimate_at_completion_cost,
        coalesce(sum(planned_cost),0) as project_planned_cost,
        coalesce(sum(earned_cost),0) as project_earned_cost,
        coalesce(sum(variance_cost),0) as project_variance_cost,
        coalesce(sum(commitments_cost),0) as project_commitments_cost,
        coalesce(sum(actuals_to_date_all_revenue),0) as project_actuals_to_date_all_revenue,
        coalesce(sum(original_budget_revenue),0) as project_original_budget_revenue,
        coalesce(sum(approved_variations_revenue),0) as project_approved_variations_revenue,
        coalesce(sum(pending_variations_revenue),0) as project_pending_variations_revenue,
        coalesce(sum(current_budget_revenue),0) as project_current_budget_revenue,
        coalesce(sum(planned_revenue),0) as project_planned_revenue,
        coalesce(sum(earned_revenue),0) as project_earned_revenue,
        coalesce(sum(commitments_revenue),0) as project_commitments_revenue,
        coalesce(sum(actuals_to_date_revenue),0) as project_actuals_to_date_revenue,
        coalesce(sum(estimate_at_completion_revenue),0) as project_estimate_at_completion_revenue,
        coalesce(sum(estimate_to_complete_revenue),0) as project_estimate_to_complete_revenue,
        coalesce(sum(variance_revenue),0) as project_variance_revenue,
        coalesce(sum(original_budget_overhead),0) as project_original_budget_overhead,
        coalesce(sum(approved_variations_overhead),0) as project_approved_variations_overhead,
        coalesce(sum(pending_variations_overhead),0) as project_pending_variations_overhead,
        coalesce(sum(current_budget_overhead),0) as project_current_budget_overhead,
        coalesce(sum(actuals_to_date_overhead),0) as project_actuals_to_date_overhead,
        coalesce(sum(estimate_to_complete_overhead),0) as project_estimate_to_complete_overhead,
        coalesce(sum(estimate_at_completion_overhead),0) as project_estimate_at_completion_overhead,
        coalesce(sum(planned_overhead),0) as project_planned_overhead,
        coalesce(sum(earned_overhead),0) as project_earned_overhead,
        coalesce(sum(variance_overhead),0) as project_variance_overhead,
        coalesce(sum(commitments_overhead),0) as project_commitments_overhead,
        coalesce(sum(unallocated_actual_revenue),0) as project_unallocated_actual_revenue,
        coalesce(sum(unallocated_actual_cost),0) as project_unallocated_actual_cost,
        coalesce(sum(unallocated_actual_hours),0) as project_unallocated_actual_hours
    from {{ ref('ecosys_control_account_fact_obt') }}
    group by
        project_no,
        snapshot_date,
        project_currency_code
)
select 
    project_no,
    end_date,
    project_no||'-'||to_char(end_date,'yyyymmdd') as project_snapshot,
    pd.project_currency_code,
    original_budget_start_date, 
    original_budget_end_date, 
    working_forecast_end_date,
    non_sub_contract_ca_count, 
    billable_ca_count,
    case when billable_ca_count = 0 or original_budget_revenue > 0 then 'Yes' else 'No' end has_original_budget_revenue, 
    case when billable_ca_count = 0 or actuals_to_date_revenue > 0 then 'Yes' else 'No' end has_actuals_to_date_revenue,  
    case when billable_ca_count = 0 or approved_variations_revenue > 0 then 'Yes' else 'No' end has_approved_variations_revenue,  
    case when billable_ca_count = 0 or pending_variations_revenue > 0 then 'Yes' else 'No' end has_pending_variations_revenue,  
    case when billable_ca_count = 0 or estimate_at_completion_revenue > 0 then 'Yes' else 'No' end has_estimate_at_completion_revenue,  
    case when billable_ca_count = 0 or planned_revenue > 0 then 'Yes' else 'No' end has_planned_revenue,  
    case when billable_ca_count = 0 or earned_revenue > 0 then 'Yes' else 'No' end has_earned_revenue,
    case when has_original_budget_revenue = 'Yes' or has_approved_variations_revenue = 'Yes' then 'Yes' else 'No' end has_current_budget_revenue, 

    case when billable_ca_count = 0 or earned_revenue - lag(earned_revenue, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_earned_revenue_in_period,
    case when billable_ca_count = 0 or actuals_to_date_revenue - lag(actuals_to_date_revenue, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_actuals_to_date_revenue_in_period,
    case when billable_ca_count = 0 or estimate_at_completion_revenue - lag(estimate_at_completion_revenue, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_estimate_at_completion_revenue_in_period,
    
    case when original_budget_cost > 0 then 'Yes' else 'No' end has_original_budget_cost, 
    case when actuals_to_date_cost > 0 then 'Yes' else 'No' end has_actuals_to_date_cost,  
    case when approved_variations_cost > 0 then 'Yes' else 'No' end has_approved_variations_cost,  
    case when pending_variations_cost > 0 then 'Yes' else 'No' end has_pending_variations_cost,  
    case when estimate_at_completion_cost > 0 then 'Yes' else 'No' end has_estimate_at_completion_cost,  
    case when planned_cost > 0 then 'Yes' else 'No' end has_planned_cost,  
    case when earned_cost > 0 then 'Yes' else 'No' end has_earned_cost,
    case when original_budget_cost > 0 or approved_variations_cost > 0 then 'Yes' else 'No' end has_current_budget_cost,

    case when earned_cost - lag(earned_cost, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_earned_cost_in_period,
    case when actuals_to_date_cost - lag(actuals_to_date_cost, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_actuals_to_date_cost_in_period,
    case when estimate_at_completion_cost - lag(estimate_at_completion_cost, 1) over (partition by project_no order by end_date) <> 0 then 'Yes' else 'No' end as has_estimate_at_completion_cost_in_period,

    -- Sub contracts will not have hours, if all CA are subcontract then treat hours as existing
    case when original_budget_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_original_budget_work_hours, 
    case when actuals_to_date_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_actuals_to_date_work_hours,  
    case when approved_variations_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_approved_variations_work_hours,  
    case when pending_variations_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_pending_variations_work_hours,  
    case when estimate_at_completion_work_hours > 0 or non_sub_contract_ca_count = 0then 'Yes' else 'No' end has_estimate_at_completion_work_hours,  
    case when planned_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_planned_work_hours,  
    case when earned_work_hours > 0 or non_sub_contract_ca_count = 0 then 'Yes' else 'No' end has_earned_work_hours,
    case when has_original_budget_work_hours = 'Yes' or has_approved_variations_work_hours = 'Yes' then 'Yes' else 'No' end has_current_budget_hours,

    case when non_sub_contract_ca_count = 0 or (earned_work_hours - lag(earned_work_hours, 1) over (partition by project_no order by end_date)) <> 0 then 'Yes' else 'No' end as has_earned_work_hours_in_period,
    case when non_sub_contract_ca_count = 0 or (actuals_to_date_work_hours - lag(actuals_to_date_work_hours, 1) over (partition by project_no order by end_date)) <> 0 then 'Yes' else 'No' end as has_actuals_to_date_work_hours_in_period,
    case when non_sub_contract_ca_count = 0 or (estimate_at_completion_work_hours - lag(estimate_at_completion_work_hours, 1) over (partition by project_no order by end_date)) <> 0 then 'Yes' else 'No' end as has_estimate_at_completion_work_hours_in_period,
    project_original_budget_work_hours,
    project_approved_variations_work_hours,
    project_pending_variations_work_hours,
    project_current_budget_work_hours,
    project_actuals_to_date_work_hours,
    project_estimate_to_complete_work_hours,
    project_estimate_at_complete_work_hours,
    project_variance_work_hours,
    project_planned_work_hours,
    project_earned_work_hours,
    project_original_budget_cost,
    project_approved_variations_cost,
    project_pending_variations_cost,
    project_current_budget_cost,
    project_actuals_to_date_cost,
    project_estimate_to_complete_cost,
    project_estimate_at_completion_cost,
    project_planned_cost,
    project_earned_cost,
    project_variance_cost,
    project_commitments_cost,
    project_actuals_to_date_all_revenue,
    project_original_budget_revenue,
    project_approved_variations_revenue,
    project_pending_variations_revenue,
    project_current_budget_revenue,
    project_planned_revenue,
    project_earned_revenue,
    project_commitments_revenue,
    project_actuals_to_date_revenue,
    project_estimate_at_completion_revenue,
    project_estimate_to_complete_revenue,
    project_variance_revenue,
    project_original_budget_overhead,
    project_approved_variations_overhead,
    project_pending_variations_overhead,
    project_current_budget_overhead,
    project_actuals_to_date_overhead,
    project_estimate_to_complete_overhead,
    project_estimate_at_completion_overhead,
    project_planned_overhead,
    project_earned_overhead,
    project_variance_overhead,
    project_commitments_overhead,
    project_unallocated_actual_revenue,
    project_unallocated_actual_cost,
    project_unallocated_actual_hours,
    project_current_budget_revenue - project_current_budget_cost as project_current_budget_gross_margin,
    project_original_budget_revenue - project_original_budget_cost as project_original_budget_gross_margin,
    project_estimate_at_completion_revenue - project_estimate_at_completion_cost as project_estimate_at_completion_gross_margin,
    (project_actuals_to_date_all_revenue + project_actuals_to_date_revenue) - project_actuals_to_date_cost as project_actuals_to_date_gross_margin,
    er.aed,
    er.ars,
    er.aud,
    er.bhd,
    er.bnd,
    er.brl,
    er.cad,
    er.clp,
    er.cny,
    er.cop,
    er.eur,
    er.gbp,
    er.idr,
    er.inr,
    er.kwd,
    er.kzt,
    er.mxn,
    er.myr,
    er.ngn,
    er.nzd,
    er.omr,
    er.qar,
    er.sar,
    er.sek,
    er.sgd,
    er.thb,
    er.ttd,
    er.usd,
    er.uzs,
    er.zar,
    (project_estimate_at_completion_revenue - (project_current_budget_revenue + project_pending_variations_revenue)) / 
    nullif((project_current_budget_revenue + project_pending_variations_revenue) , 0) as eacPercentDiff,
    ((project_actuals_to_date_all_revenue + project_actuals_to_date_revenue) - project_current_budget_revenue) / nullif(project_current_budget_revenue , 0) as itdPercentDiff,
    case
        when eacPercentDiff is not null and itdPercentDiff is not null then
        case
            when eacPercentDiff > 0.1 or itdPercentDiff > 0.05 then 'Red'
            when eacPercentDiff > 0 or itdPercentDiff > 0 then 'Yellow'
            else 'Green'
    end end as revenue_traffic_light_color,
    (project_estimate_at_completion_cost - (project_current_budget_cost + project_pending_variations_cost)) / 
    nullif((project_current_budget_cost + project_pending_variations_cost) , 0) as cost_eacPercentDiff,
    (project_actuals_to_date_cost - project_current_budget_cost) / nullif(project_current_budget_cost , 0) as cost_itdPercentDiff,
    case
        when cost_eacPercentDiff is not null and cost_itdPercentDiff is not null then
        case
            when cost_eacPercentDiff > 0.1 or cost_itdPercentDiff > 0.05 then 'Red'
            when cost_eacPercentDiff > 0 or cost_itdPercentDiff > 0 then 'Yellow'
            else 'Green'
    end end as cost_traffic_light_color,
    (project_estimate_at_complete_work_hours - (project_current_budget_work_hours + project_pending_variations_work_hours)) / 
    nullif((project_current_budget_work_hours + project_pending_variations_work_hours) , 0) as hours_eacPercentDiff,
    (project_actuals_to_date_work_hours - project_current_budget_work_hours) / nullif(project_current_budget_work_hours , 0) as hours_itdPercentDiff,
    case
        when hours_eacPercentDiff is not null and hours_itdPercentDiff is not null then
        case
            when hours_eacPercentDiff > 0.1 or hours_itdPercentDiff > 0.05 then 'Red'
            when hours_eacPercentDiff > 0 or hours_itdPercentDiff > 0 then 'Yellow'
            else 'Green'
    end end as hours_traffic_light_color,
    (project_estimate_at_completion_gross_margin - project_original_budget_gross_margin) /  nullif(project_original_budget_gross_margin,0) as gm_eacPercentDiff,
    case
        when gm_eacPercentDiff is not null then
        case
            when gm_eacPercentDiff < -0.04 then 'Red'
            when gm_eacPercentDiff < -0.02 then 'Yellow'
            else 'Green'
    end end as gm_traffic_light_color
from project_level pl
join projectdata pd on pl.project_no = pd.projectid and pl.end_date = pd.snapshot_date
left join {{ ref('dim_ecosys_exchange_rates_unpivot') }} er
on lower(pd.project_currency_code)  = er.cur and 
    pl.end_date = er.snapshot_date