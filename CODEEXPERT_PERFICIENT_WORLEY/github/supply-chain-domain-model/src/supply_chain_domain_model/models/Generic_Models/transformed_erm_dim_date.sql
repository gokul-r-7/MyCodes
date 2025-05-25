{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_dim_date/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select 
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date_pk, 'M/d/yyyy'))) AS date_pk,
    cast(dim_week_pk as INT) as dim_week_pk,
    cast(dim_month_pk as INT) as dim_month_pk,
    cast(dim_year_pk as INT) as dim_year_pk, 
    cast(financial_year_pk as INT) as financial_year_pk,
    cast(execution_date as date) as execution_date,
    source_system_name,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from
    {{ source('curated_erm', 'dim_date') }}