{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_mat_grp_type_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


with cte_mat_grp_type as ( 
    select  *,
            cast(mat_grp_type.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id
    from   
         {{ source('curated_erm', 'mat_grp_type') }} mat_grp_type
    where mat_grp_type.is_current = 1
)


select * from cte_mat_grp_type