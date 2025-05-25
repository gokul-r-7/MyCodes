{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
  
{{
      config(
        alias='transformed_constraint_type',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_constraint_type/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
        ) 
}}
    
with distinct_constraint_type as (
    select distinct
        constrainttypeid,
        is_current,
        constrainttype,
        '' AS projectcode
    from {{ source('curated_o3', 'curated_constraints') }}
    where is_current = 1
)

select
    constrainttypeid as constraint_key,
    CAST(constrainttypeid as INT) as constrainttypeid,
    constrainttype,
    projectcode,
    is_current,
    cast({{run_date}} as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from distinct_constraint_type  