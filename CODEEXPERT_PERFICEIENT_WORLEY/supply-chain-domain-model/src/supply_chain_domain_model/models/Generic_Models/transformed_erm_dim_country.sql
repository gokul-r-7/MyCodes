{%- set run_date = "CURRENT_TIMESTAMP()" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_dim_country/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_cont as ( 
    select  cont.country_no as country_no,
            cont.country_id as country_id,
            cont.name as name,
            cont.stat as stat,
            cont.in_eu as in_eu,
            cont.states_restricted as states_restricted,
            cont.states_mandatory as states_mandatory,
            cast(cont.def_date as date) as def_date,
            cont.def_usr_id as def_usr_id,
            cast(cont.upd_date as date) as upd_date,
            cont.upd_usr_id as upd_usr_id,
            cast(cont.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id  
    from   
         {{ source('curated_erm', 'country') }} cont
    where cont.is_current = 1
)

select * from cte_cont