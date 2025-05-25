{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_location_hierarchy/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_location_hierarchy as ( 
    select  lh.loc_id as loc_id,
            lh.loc_parentid as loc_parentid,
            lh.location as location,
            cast(lh.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id 		
    from   
         {{ source('curated_erm', 'location_hierarchy') }}  lh
)


select * from cte_location_hierarchy