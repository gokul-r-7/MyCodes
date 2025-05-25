{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select 
project_id as project_name,
cast(project_code as varchar(100)) AS project_id,
maturity_step,
activity, 
cast(step_percent as NUMERIC(38, 2)) as step_percent,
cast(cum_percent as NUMERIC(38, 2)) as cum_percent,
discipline,
phase,
source_system_name,
extracted_date
FROM {{ source('engineering_dim', 'transformed_roc_status_projectname') }}