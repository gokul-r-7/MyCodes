{{
     config(
         materialized = "table",
         tags=["project_control"]
         )

}}

with ecosys_data as
( 
    select
        *
    from {{ ref('ecosys_control_account_fact_obt') }}
),
ExchangeRateToAUD as
(
    select 
        startminorperiod as Period_date,
        Case extract(dow from Last_day(startminorperiod)) 
        when extract(dow from Last_day(startminorperiod)) = 5 then Last_day(startminorperiod)    
        when extract(dow from Last_day(startminorperiod)) = 6 then Dateadd(day,-1 ,Last_day(startminorperiod))
        when extract(dow from Last_day(startminorperiod)) > 0  and extract(dow from Last_day(startminorperiod)) < 5 then Dateadd(day, -extract(dow from Last_day(startminorperiod))-2 ,Last_day(startminorperiod))
        else  Dateadd(day,-2 ,Last_day(startminorperiod))
        end as snapshot_date,
        Currency,
        Value
    FROM {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_exchange_rates') }}
    UNPIVOT (
        Value FOR Currency IN (
                aed,ars,aud,bhd,bnd,brl,
                cad,clp,cny,cop,eur,gbp,
                idr,inr,kwd,kzt,mxn,myr,
                ngn,nzd,omr,qar,sar,sek,
                sgd,thb,ttd,usd,uzs,zar
        )
    ) AS unpvt
),
ExchangeRates AS
(
    select 
        startminorperiod as Period_date,
        Case extract(dow from Last_day(startminorperiod)) 
        when extract(dow from Last_day(startminorperiod)) = 5 then Last_day(startminorperiod)    
        when extract(dow from Last_day(startminorperiod)) = 6 then Dateadd(day,-1 ,Last_day(startminorperiod))
        when extract(dow from Last_day(startminorperiod)) > 0  and extract(dow from Last_day(startminorperiod)) < 5 then Dateadd(day, -extract(dow from Last_day(startminorperiod))-2 ,Last_day(startminorperiod))
        else  Dateadd(day,-2 ,Last_day(startminorperiod))
        end as snapshot_date,
        GBP,EUR,CAD,USD,SAR,BRL,SEK,CNY 
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_exchange_rates') }}
)

Select 
    project_internal_id,
    ecosys_data.snapshot_Date,
    original_budget_cost,
    Project_currency_code,
    ecosys_data.control_account_internal_id,
    ecosys_data.ca_snapshot,
    Value, 
    GBP,EUR,CAD,USD,SAR,BRL,SEK,CNY,
    original_budget_cost / Value as original_budget_cost_aud,
    approved_variations_cost / Value as approved_variations_cost_aud,
    pending_variations_cost / Value as pending_variations_cost_aud,
    current_budget_cost / Value as current_budget_cost_aud,
    actuals_to_date_cost / Value as actuals_to_date_cost_aud,
    estimate_to_complete_cost / Value as estimate_to_complete_cost_aud,
    estimate_at_completion_cost / Value as estimate_at_completion_cost_aud,
    planned_cost / Value as planned_cost_aud,
    earned_cost / Value as earned_cost_aud,
    variance_cost / Value as variance_cost_aud,
    commitments_cost / Value as commitments_cost_aud,
    orig_cost_cat_current_budget_cost / Value as orig_cost_cat_current_budget_cost_aud,
    orig_cost_cat_actual_cost / Value as orig_cost_cat_actual_cost_aud,
    orig_cost_cat_planned_cost / Value as orig_cost_cat_planned_cost_aud,
    orig_cost_cat_earned_cost / Value as orig_cost_cat_earned_cost_aud,
    actuals_to_date_all_revenue / Value as actuals_to_date_all_revenue_aud,
    original_budget_revenue / Value as original_budget_revenue_aud,
    approved_variations_revenue / Value as approved_variations_revenue_aud,
    pending_variations_revenue / Value as pending_variations_revenue_aud,
    current_budget_revenue / Value as current_budget_revenue_aud,
    planned_revenue / Value as planned_revenue_aud,
    earned_revenue / Value as earned_revenue_aud,
    commitments_revenue / Value as commitments_revenue_aud,
    actuals_to_date_revenue / Value as actuals_to_date_revenue_aud,
    estimate_at_completion_revenue / Value as estimate_at_completion_revenue_aud,
    estimate_to_complete_revenue / Value as estimate_to_complete_revenue_aud,
    variance_revenue / Value as variance_revenue_aud,
    original_budget_overhead / Value as original_budget_overhead_aud,
    approved_variations_overhead / Value as approved_variations_overhead_aud,
    pending_variations_overhead / Value as pending_variations_overhead_aud,
    current_budget_overhead / Value as current_budget_overhead_aud,
    actuals_to_date_overhead / Value as actuals_to_date_overhead_aud,
    estimate_to_complete_overhead / Value as estimate_to_complete_overhead_aud,
    estimate_at_completion_overhead / Value as estimate_at_completion_overhead_aud,
    planned_overhead / Value as planned_overhead_aud,
    earned_overhead / Value as earned_overhead_aud,
    variance_overhead / Value as variance_overhead_aud,
    commitments_overhead / Value as commitments_overhead_aud,


    --usd
    original_budget_cost / Value * usd as original_budget_cost_usd,
    approved_variations_cost / Value * usd as approved_variations_cost_usd,
    pending_variations_cost / Value * usd as pending_variations_cost_usd,
    current_budget_cost / Value * usd as current_budget_cost_usd,
    actuals_to_date_cost / Value * usd as actuals_to_date_cost_usd,
    estimate_to_complete_cost / Value * usd as estimate_to_complete_cost_usd,
    estimate_at_completion_cost / Value * usd as estimate_at_completion_cost_usd,
    planned_cost / Value * usd as planned_cost_usd,
    earned_cost / Value * usd as earned_cost_usd,
    variance_cost / Value * usd as variance_cost_usd,
    commitments_cost / Value * usd as commitments_cost_usd,
    orig_cost_cat_current_budget_cost / Value * usd as orig_cost_cat_current_budget_cost_usd,
    orig_cost_cat_actual_cost / Value * usd as orig_cost_cat_actual_cost_usd,
    orig_cost_cat_planned_cost / Value * usd as orig_cost_cat_planned_cost_usd,
    orig_cost_cat_earned_cost / Value * usd as orig_cost_cat_earned_cost_usd,
    actuals_to_date_all_revenue / Value * usd as actuals_to_date_all_revenue_usd,
    original_budget_revenue / Value * usd as original_budget_revenue_usd,
    approved_variations_revenue / Value * usd as approved_variations_revenue_usd,
    pending_variations_revenue / Value * usd as pending_variations_revenue_usd,
    current_budget_revenue / Value * usd as current_budget_revenue_usd,
    planned_revenue / Value * usd as planned_revenue_usd,
    earned_revenue / Value * usd as earned_revenue_usd,
    commitments_revenue / Value * usd as commitments_revenue_usd,
    actuals_to_date_revenue / Value * usd as actuals_to_date_revenue_usd,
    estimate_at_completion_revenue / Value * usd as estimate_at_completion_revenue_usd,
    estimate_to_complete_revenue / Value * usd as estimate_to_complete_revenue_usd,
    variance_revenue / Value * usd as variance_revenue_usd,
    original_budget_overhead / Value * usd as original_budget_overhead_usd,
    approved_variations_overhead / Value * usd as approved_variations_overhead_usd,
    pending_variations_overhead / Value * usd as pending_variations_overhead_usd,
    current_budget_overhead / Value * usd as current_budget_overhead_usd,
    actuals_to_date_overhead / Value * usd as actuals_to_date_overhead_usd,
    estimate_to_complete_overhead / Value * usd as estimate_to_complete_overhead_usd,
    estimate_at_completion_overhead / Value * usd as estimate_at_completion_overhead_usd,
    planned_overhead / Value * usd as planned_overhead_usd,
    earned_overhead / Value * usd as earned_overhead_usd,
    variance_overhead / Value * usd as variance_overhead_usd,
    commitments_overhead / Value * usd as commitments_overhead_usd,

    --cad
    original_budget_cost / Value * cad as original_budget_cost_cad,
    approved_variations_cost / Value * cad as approved_variations_cost_cad,
    pending_variations_cost / Value * cad as pending_variations_cost_cad,
    current_budget_cost / Value * cad as current_budget_cost_cad,
    actuals_to_date_cost / Value * cad as actuals_to_date_cost_cad,
    estimate_to_complete_cost / Value * cad as estimate_to_complete_cost_cad,
    estimate_at_completion_cost / Value * cad as estimate_at_completion_cost_cad,
    planned_cost / Value * cad as planned_cost_cad,
    earned_cost / Value * cad as earned_cost_cad,
    variance_cost / Value * cad as variance_cost_cad,
    commitments_cost / Value * cad as commitments_cost_cad,
    orig_cost_cat_current_budget_cost / Value * cad as orig_cost_cat_current_budget_cost_cad,
    orig_cost_cat_actual_cost / Value * cad as orig_cost_cat_actual_cost_cad,
    orig_cost_cat_planned_cost / Value * cad as orig_cost_cat_planned_cost_cad,
    orig_cost_cat_earned_cost / Value * cad as orig_cost_cat_earned_cost_cad,
    actuals_to_date_all_revenue / Value * cad as actuals_to_date_all_revenue_cad,
    original_budget_revenue / Value * cad as original_budget_revenue_cad,
    approved_variations_revenue / Value * cad as approved_variations_revenue_cad,
    pending_variations_revenue / Value * cad as pending_variations_revenue_cad,
    current_budget_revenue / Value * cad as current_budget_revenue_cad,
    planned_revenue / Value * cad as planned_revenue_cad,
    earned_revenue / Value * cad as earned_revenue_cad,
    commitments_revenue / Value * cad as commitments_revenue_cad,
    actuals_to_date_revenue / Value * cad as actuals_to_date_revenue_cad,
    estimate_at_completion_revenue / Value * cad as estimate_at_completion_revenue_cad,
    estimate_to_complete_revenue / Value * cad as estimate_to_complete_revenue_cad,
    variance_revenue / Value * cad as variance_revenue_cad,
    original_budget_overhead / Value * cad as original_budget_overhead_cad,
    approved_variations_overhead / Value * cad as approved_variations_overhead_cad,
    pending_variations_overhead / Value * cad as pending_variations_overhead_cad,
    current_budget_overhead / Value * cad as current_budget_overhead_cad,
    actuals_to_date_overhead / Value * cad as actuals_to_date_overhead_cad,
    estimate_to_complete_overhead / Value * cad as estimate_to_complete_overhead_cad,
    estimate_at_completion_overhead / Value * cad as estimate_at_completion_overhead_cad,
    planned_overhead / Value * cad as planned_overhead_cad,
    earned_overhead / Value * cad as earned_overhead_cad,
    variance_overhead / Value * cad as variance_overhead_cad,
    commitments_overhead / Value * cad as commitments_overhead_cad,
    --eur
    original_budget_cost / Value * eur as original_budget_cost_eur,
    approved_variations_cost / Value * eur as approved_variations_cost_eur,
    pending_variations_cost / Value * eur as pending_variations_cost_eur,
    current_budget_cost / Value * eur as current_budget_cost_eur,
    actuals_to_date_cost / Value * eur as actuals_to_date_cost_eur,
    estimate_to_complete_cost / Value * eur as estimate_to_complete_cost_eur,
    estimate_at_completion_cost / Value * eur as estimate_at_completion_cost_eur,
    planned_cost / Value * eur as planned_cost_eur,
    earned_cost / Value * eur as earned_cost_eur,
    variance_cost / Value * eur as variance_cost_eur,
    commitments_cost / Value * eur as commitments_cost_eur,
    orig_cost_cat_current_budget_cost / Value * eur as orig_cost_cat_current_budget_cost_eur,
    orig_cost_cat_actual_cost / Value * eur as orig_cost_cat_actual_cost_eur,
    orig_cost_cat_planned_cost / Value * eur as orig_cost_cat_planned_cost_eur,
    orig_cost_cat_earned_cost / Value * eur as orig_cost_cat_earned_cost_eur,
    actuals_to_date_all_revenue / Value * eur as actuals_to_date_all_revenue_eur,
    original_budget_revenue / Value * eur as original_budget_revenue_eur,
    approved_variations_revenue / Value * eur as approved_variations_revenue_eur,
    pending_variations_revenue / Value * eur as pending_variations_revenue_eur,
    current_budget_revenue / Value * eur as current_budget_revenue_eur,
    planned_revenue / Value * eur as planned_revenue_eur,
    earned_revenue / Value * eur as earned_revenue_eur,
    commitments_revenue / Value * eur as commitments_revenue_eur,
    actuals_to_date_revenue / Value * eur as actuals_to_date_revenue_eur,
    estimate_at_completion_revenue / Value * eur as estimate_at_completion_revenue_eur,
    estimate_to_complete_revenue / Value * eur as estimate_to_complete_revenue_eur,
    variance_revenue / Value * eur as variance_revenue_eur,
    original_budget_overhead / Value * eur as original_budget_overhead_eur,
    approved_variations_overhead / Value * eur as approved_variations_overhead_eur,
    pending_variations_overhead / Value * eur as pending_variations_overhead_eur,
    current_budget_overhead / Value * eur as current_budget_overhead_eur,
    actuals_to_date_overhead / Value * eur as actuals_to_date_overhead_eur,
    estimate_to_complete_overhead / Value * eur as estimate_to_complete_overhead_eur,
    estimate_at_completion_overhead / Value * eur as estimate_at_completion_overhead_eur,
    planned_overhead / Value * eur as planned_overhead_eur,
    earned_overhead / Value * eur as earned_overhead_eur,
    variance_overhead / Value * eur as variance_overhead_eur,
    commitments_overhead / Value * eur as commitments_overhead_eur
from ecosys_data 
left join ExchangeRateToAUD on
        ecosys_data.snapshot_date = ExchangeRateToAUD.snapshot_date and
        ecosys_data.project_currency_code=upper(ExchangeRateToAUD.currency)
left join ExchangeRates on
    ecosys_data.snapshot_date = ExchangeRates.snapshot_date 
