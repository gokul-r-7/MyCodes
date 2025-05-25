{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{ config(
    materialized = "table",
    tags=["finance"]
) }}

select * from
{{ source('integrations_finance', 'transformed_gbs_shell_program') }} 


