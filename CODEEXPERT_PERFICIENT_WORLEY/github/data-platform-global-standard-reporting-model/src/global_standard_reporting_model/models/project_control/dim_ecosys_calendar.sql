{{
     config(
         materialized = "table",
         tags=["project_control"]
         )

}}

select
    to_char(end_date,'yyyymmdd') :: int as date_key,
    year :: int,
    end_date as snapshot_date,
    to_char(end_date,'Mon-yyyy') as snapshot_month,
    rank() over (order by date_key) as index
from 
    {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_calendar') }}
where 
    date_type = 'Month'
and end_date >= add_months(current_date, -13)
and end_date <= current_date