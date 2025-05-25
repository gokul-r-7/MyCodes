{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized = 'incremental',
        incremental_strategy='append',
        tags=["audit"]

    )
}}

with empty_table as (
    select
        'test_load_id'::varchar(max) as load_id,
        'test_invocation_id'::varchar(max) as invocation_id,
        'test_database_name'::varchar(max) as database_name,
        'test_schema_name'::varchar(max) as schema_name,
        'test_model_name'::varchar(max) as name,
        'test_resource_type'::varchar(max) as resource_type,
        'test_status'::varchar(max) as status,
        cast('12122012' as float) as execution_time,
        cast('100' as int) as rows_affected,
        {{run_date}} as model_execution_date
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0
