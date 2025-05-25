{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_project_location/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_project_location as ( 
    select  pl.project_id as project_id,
            pl.description as description,
            pl.project_type as project_type,
            pl.worley_project_region as worley_project_region,
            pl.sub_region as sub_region,
            pl.country as country,
            pl.worley_project_office as worley_project_office,
            pl.worley_csg as worley_csg,
            pl.client as client,
            pl.project_scope as project_scope,
            pl.buy_on_whose_paper as buy_on_whose_paper,
            pl.customer_category as customer_category,
            cast(pl.execution_date  as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id				
    from   
         {{ source('curated_erm', 'project_location') }}  pl
)

select * from cte_project_location