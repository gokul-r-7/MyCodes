{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_total_headcount/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}


SELECT
  TO_TIMESTAMP(
    CONCAT(
        '20',  
        SUBSTRING(total_headcount_month_year, 1, 2),  
        '-',
        SUBSTRING(total_headcount_month_year, 4, 2),  
        '-01'  
    ),
    'yyyy-MM-dd'  -- Use lowercase 'yyyy' and 'MM' to conform to the updated pattern
) AS total_headcount_timestamp,
    CASE
        WHEN UPPER(total_headcount_month_year) = '' THEN NULL
        ELSE total_headcount_month_year
    END AS total_headcount_month_year,
    
    FLOOR(CASE
        WHEN UPPER(total_headcount_total_headcount) = '' THEN NULL
        ELSE total_headcount_total_headcount
    END) AS total_headcount_total_headcount,
    
    FLOOR(CASE
        WHEN UPPER(total_headcount_region_headcount) = '' THEN NULL
        ELSE total_headcount_region_headcount
    END) AS total_headcount_region_headcount,
    
    FLOOR(CASE
        WHEN UPPER(total_headcount_americas) = '' THEN NULL
        ELSE total_headcount_americas
    END) AS total_headcount_americas,
    
    FLOOR(CASE
        WHEN UPPER(total_headcount_apac) = '' THEN NULL
        ELSE total_headcount_apac
    END) AS total_headcount_apac,
    
    FLOOR(CASE
        WHEN UPPER(total_headcount_emea) = '' THEN NULL
        ELSE total_headcount_emea
    END) AS total_headcount_emea,
    
    CASE
        WHEN UPPER(source_system_name) = '' THEN NULL
        ELSE source_system_name
    END AS source_system_name,
    
    CASE
        WHEN UPPER(custom_project_id) = '' THEN NULL
        ELSE custom_project_id
    END AS custom_project_id,
    
    CASE
        WHEN execution_date IS NULL THEN NULL
        ELSE CAST(execution_date AS TIMESTAMP)
    END AS execution_date,
    
  {{run_date}} as model_created_date,
  {{run_date}} as model_updated_date,
  {{ generate_load_id(model) }} as model_load_id
    
from {{ source('curated_hexagon', 'curated_entitlement_total_headcount') }}