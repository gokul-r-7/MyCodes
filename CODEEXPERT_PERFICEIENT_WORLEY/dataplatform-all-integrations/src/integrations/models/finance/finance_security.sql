{{
     config(
         materialized = "incremental",
         tags=["finance"]
         )
}}

with empty_table as (
    select
        ''::varchar(16383) as integration_id,
        ''::varchar(500) as user_name

)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0