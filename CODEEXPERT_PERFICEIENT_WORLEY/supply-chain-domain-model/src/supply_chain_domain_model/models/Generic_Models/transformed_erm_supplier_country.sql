{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_supplier_country/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

with cte_supplier_country as ( 
    select  sc.project as project,
            sc.supplier_no as supplier_no,
            sc.supplier_name as supplier_name,
            sc.state_province as state_province,
            sc.country as country,
            cast (sc.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id			
    from   
         {{ source('curated_erm', 'supplier_country') }}  sc
)

select * from cte_supplier_country