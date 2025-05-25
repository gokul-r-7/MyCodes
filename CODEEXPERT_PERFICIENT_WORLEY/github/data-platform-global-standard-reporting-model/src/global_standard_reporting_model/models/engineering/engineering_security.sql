{{
    config(
        materialized = 'incremental',
        unique_key= ['project_id', 'user_name'],
        on_schema_change='fail',
        incremental_strategy='merge',
        tags=["engineering"]
    )
}}

with empty_table as (
    select
    ''::varchar(100) as project_id,
    ''::varchar(250) as user_name
)

select * from empty_table
where 1 = 0