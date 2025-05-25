{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}

select distinct
    project,
    projectname,
    projectcode,
    projectdescription,
    projectshortname,
    source_system_name,
    is_current AS meta_journal_current, 
    project AS meta_project_rls_key,
    cast(execution_date as timestamp) as meta_ingestion_date,
    getdate() AS meta_snapshot_date,
    {{run_date}} as dbt_created_date,
    {{run_date}} as dbt_updated_date,
    {{ generate_load_id(model) }} as dbt_load_id
from {{ source('curated_aconex', 'curated_project') }}
where is_current = 1