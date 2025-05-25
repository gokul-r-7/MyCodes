{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_snapidlist/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}
SELECT
        snapidlist_transactioncategoryinternalid as transaction_category_internal_id,
        snapidlist_transactioncategoryid as transaction_category_id,
        snapidlist_transactioncategoryname as transaction_category_name,
        execution_date ,
        source_system_name ,
        primary_key ,
        is_current ,
        eff_start_date ,
        eff_end_date ,
   		{{run_date}} as created_date,
		{{run_date}} as updated_date,
		{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_ecosys', 'curated_ecosys_snapidlist') }}
where
is_current = 1
