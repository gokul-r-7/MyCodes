{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select 
project_id as project_name,
project_code as project_id,
maturity_step,
activity, 
cast(step_percent as NUMERIC(38, 2)) * 100 as step_percent,
cast(cum_percent as NUMERIC(38, 2)) * 100 as cum_percent,
phase,
source_system_name,
extracted_date
FROM {{ source('engineering_dim', 'curated_roc_status_projectname') }}