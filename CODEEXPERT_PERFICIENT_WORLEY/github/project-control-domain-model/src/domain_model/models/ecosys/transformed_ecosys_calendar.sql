{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_calendar/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}
SELECT 
calendar_year as YEAR,
    calendar_month as MONTH,
    calendar_startdate as START_DATE,
    calendar_enddatevalue as END_DATE_VALUE,
    to_date(calendar_enddate,'yyyy-MM-dd') as END_DATE,
    calendar_transactioncategoryinternalid as TRANSACTION_CATEGORY_INTERNAL_ID,
    calendar_datetype as DATE_TYPE,
    '' as ETL_JOB_STATUS_PK,
    '' as ACTIVE_INDICATOR,
    '' as CREATED_BY,
    '' as UPDATED_BY,
    {{run_date}} as CREATED_DATE,
    {{run_date}} as  UPDATED_DATE,
    {{ generate_load_id(model) }} as load_id
FROM {{ source('curated_ecosys', 'curated_ecosys_calendar') }}