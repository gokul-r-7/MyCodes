{{
     config(
         materialized = "table",
         tags=["project_control"]
         )
}}


select
    dt.*,
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
    (project_estimate_at_completion_revenue - (dt.project_current_budget_revenue + project_pending_variations_revenue)) / nullif((dt.project_current_budget_revenue + project_pending_variations_revenue) , 0) as eacPercentDiff,
    ((project_actuals_to_date_all_revenue + project_actuals_to_date_revenue) - project_current_budget_revenue) / nullif(dt.project_current_budget_revenue , 0) as itdPercentDiff,
    case
        when eacPercentDiff is not null and itdPercentDiff is not null then
        case
            when eacPercentDiff > 0.1 or itdPercentDiff > 0.05 then 'Red'
            when eacPercentDiff > 0 or itdPercentDiff > 0 then 'Yellow'
            else 'Green'
    end end as revenue_traffic_light_color,
    (project_estimate_at_completion_cost - (dt.project_current_budget_cost + project_pending_variations_cost)) / 
    nullif((dt.project_current_budget_cost + project_pending_variations_cost) , 0) as cost_eacPercentDiff,
    (project_actuals_to_date_cost - project_current_budget_cost) / nullif(dt.project_current_budget_cost , 0) as cost_itdPercentDiff,
    case
        when cost_eacPercentDiff is not null and cost_itdPercentDiff is not null then
        case
            when cost_eacPercentDiff > 0.1 or cost_itdPercentDiff > 0.05 then 'Red'
            when cost_eacPercentDiff > 0 or cost_itdPercentDiff > 0 then 'Yellow'
            else 'Green'
    end end as cost_traffic_light_color,
    (project_estimate_at_complete_work_hours - (dt.project_current_budget_work_hours + project_pending_variations_work_hours)) / 
    nullif((dt.project_current_budget_work_hours + project_pending_variations_work_hours) , 0) as hours_eacPercentDiff,
    (project_actuals_to_date_work_hours - project_current_budget_work_hours) / nullif(dt.project_current_budget_work_hours , 0) as hours_itdPercentDiff,
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
from
    {{ ref('ecosys_project_data_availability') }} dt
left join
    {{ ref('dim_ecosys_exchange_rates_unpivot') }} er
on lower(dt.project_currency_code) = er.cur and 
    dt.end_date = er.snapshot_date