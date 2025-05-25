{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_progress_category/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

SELECT
    progress_category,
    progress_category_description,
    display_order,
    '' as etl_job_status_pk,
    'true' as active_indicator,
    '' as created_by,
    '' as updated_by,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_ecosys', 'curated_ecosys_progress_category') }}