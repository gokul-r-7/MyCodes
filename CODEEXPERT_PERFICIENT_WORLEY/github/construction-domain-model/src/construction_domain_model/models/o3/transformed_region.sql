{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}

{{
    config(
        alias='transformed_region',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_region/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
        )  
}}
   
  
with distinct_region as (
    select distinct
        zoneid,
        zone,
        CAST(areaid as int ) as areaid,
        area,
        source_system_name
    from {{ source('curated_o3', 'curated_cwps') }}
    where is_current = 1
)

select 
    {{dbt_utils.generate_surrogate_key(['zoneid', 'areaid']) }} as RegionID,
    CAST(zoneid as int) as zoneid,
    zone as zonedescription,
    areaid,
    area as areadescription,
    source_system_name,
    cast({{run_date}} as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from distinct_region
  