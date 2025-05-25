{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'audit_table/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["audit"]
        ) 
}}

with empty_table as (
    select
        CAST('test_load_id' AS varchar(100)) as load_id,
        CAST('test_invocation_id' AS varchar(100)) as invocation_id,
        CAST('test_database_name' AS varchar(100)) as database_name,
        CAST('test_schema_name' AS varchar(100)) as schema_name,
        CAST('test_model_name' AS varchar(100)) as name,
        CAST('test_resource_type' AS varchar(100)) as resource_type,
        CAST('test_status' AS varchar(100)) as status,
        cast('12122012' as FLOAT) as execution_time,
        cast('100' as INTEGER) as rows_affected,
        {{run_date}} as model_execution_date
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0
