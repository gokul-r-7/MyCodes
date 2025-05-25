{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_taxonomy_master/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_taxonomy_master as ( 
    select  tm.group_l1 as group_l1,
            tm.category_l2 as category_l2,
            tm.sub_category_l3 as sub_category_l3,
            tm.family_l4 as family_l4,
            tm.commodity_l5 as commodity_l5,
            tm.source_system_name as source_system_name,
            tm.unspsc as unspsc,
            cast(tm.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id			
    from   
         {{ source('curated_erm', 'taxonomy_master') }}  tm
)

select * from cte_taxonomy_master