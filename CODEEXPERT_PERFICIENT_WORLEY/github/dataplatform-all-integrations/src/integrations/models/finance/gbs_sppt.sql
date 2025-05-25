{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{ config(
    materialized = "table",
    tags=["finance"]
) }}

select * from
{{ source('integrations_finance', 'transformed_gbs_sppt') }} 


{%- if execution_date_arg != "" %}
    and etl_load_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
    {%- endif %}
{%- endif %}