{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_budget/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}


 
    select * from {{ref('transformed_budget_july_24')}}
    UNION ALL 
    select * from {{ref('transformed_budget_aug_24')}}
    UNION ALL
    select * from {{ref('transformed_budget_sep_24')}}
    UNION ALL 
    select * from {{ref('transformed_budget_oct_24')}}
    UNION ALL 
    select * from {{ref('transformed_budget_nov_24')}}
    UNION ALL 
    select * from {{ref('transformed_budget_dec_24')}}
    UNION ALL 
    select * from {{ref('transformed_budget_jan_25')}}
    UNION ALL 
    select * from {{ref('transformed_budget_feb_25')}}
    UNION ALL 
    select * from {{ref('transformed_budget_march_25')}}
    UNION ALL 
    select * from {{ref('transformed_budget_april_25')}}
    UNION ALL 
    select * from {{ref('transformed_budget_may_25')}}
    UNION ALL 
    select * from {{ref('transformed_budget_june_25')}}