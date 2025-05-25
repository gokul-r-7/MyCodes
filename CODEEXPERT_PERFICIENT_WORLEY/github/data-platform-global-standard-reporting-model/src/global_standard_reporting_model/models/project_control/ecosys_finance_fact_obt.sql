{{
     config(
         materialized = "table",
         tags=["project_control"]
         )

}}

select
    cost_object_id as project_no,
    project_internal_id,
    transaction_date,
    project_no || '-' || to_char(transaction_date,'yyyymmdd') as project_snapshot,
    '' as contingency_category,
    cast(0.00 as float) as contingency_weighting,
    cast(rec_revenue as float) as rec_revenue,
    cast(invd_to_date as float) as invd_to_date,
    cast(wip as float) as wip,
    cast(recd_to_date as float) as recd_to_date,
    cast(outstanding_current as float) as outstanding_current,
    cast(outstanding_1_to_30_days as float) as outstanding_1_to_30_days,
    cast(outstanding_31_to_60_days as float) as outstanding_31_to_60_days,
    cast(outstanding_61_plus_days as float) as outstanding_61_plus_days,
    cast(dso as float) as dso,
    cast(dwu as float) as dwu,
    cast(ddo as float) as ddo,
    terms as terms,
    cast(outstanding_current + outstanding_1_to_30_days + outstanding_31_to_60_days + outstanding_61_plus_days as float) as outstanding_total,
    cast(outstanding_1_to_30_days + outstanding_31_to_60_days + outstanding_61_plus_days as float) as outstanding_overdue,
    case
        when rec_revenue + invd_to_date + wip + recd_to_date + outstanding_current + outstanding_1_to_30_days + outstanding_31_to_60_days + outstanding_61_plus_days > 1
        then 'Yes' else 'No'
    end as has_financial_data,
    'Finance Outstanding' as type,
    fin.execution_date
from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_findata') }} fin
join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_calendar') }} cal
    on fin.transaction_date = cal.end_date
    and cal.date_type = 'Month'
where
    cal.end_date >= add_months(current_date, -13)
and cal.end_date <= current_date

union all

select
    cost_object_id as project_no,
    project_internal_id,
    transaction_date,
    project_no || '-' || to_char(transaction_date,'yyyymmdd') as project_snapshot,
    contingency_category,
    cast(contingency_weighting as float) as contingency_weighting,
    0.00 as rec_revenue,
    0.00 as invd_to_date,
    0.00 as wip,
    0.00 as recd_to_date,
    0.00 as outstanding_current,
    0.00 as outstanding_1_to_30_days,
    0.00 as outstanding_31_to_60_days,
    0.00 as outstanding_61_plus_days,
    0.00 as dso,
    0.00 as dwu,
    0.00 as ddo,
    '' as terms,
    0.00 as outstanding_total,
    0.00 as outstanding_overdue, 
    'No' as has_financial_data,
    'Finance Contigency' as type,
    execution_date
from
    {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_finance_contingency') }}
where
    transaction_date >= add_months(current_date, -13)
and transaction_date <= current_date