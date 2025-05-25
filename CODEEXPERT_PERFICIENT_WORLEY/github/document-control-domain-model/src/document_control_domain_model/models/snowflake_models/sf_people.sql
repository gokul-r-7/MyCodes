
{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}

        SELECT
            CAST(userid AS VARCHAR) AS user_sid
            ,email AS assignee_email
            ,username AS display_name
            ,organizationname AS organization
            ,CAST(projectid AS VARCHAR) AS meta_project_rls_key           
            ,CAST(execution_date AS TIMESTAMP) AS execution_date
            ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
            ,{{run_date}} as dbt_created_date
            ,{{run_date}} as dbt_updated_date
            ,{{ generate_load_id(model) }} as dbt_load_id
        FROM
            {{ source('curated_aconex', 'curated_userproject') }} 
        WHERE   
            is_current = 1 
            AND email != 'NULL'