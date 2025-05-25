{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_req_doc_ctrl_supplier_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


with cte_req_doc_ctrl_supplier_dim as (
select 
    CAST(NULL AS STRING) AS id,
    project_code,
    CONCAT(project_code, ' - ', supplier) AS supplier,
    supplier as supplier_name,
    model_created_date as created_date,
    model_updated_date as last_updated_date,
    ROW_NUMBER() OVER (PARTITION BY project_code, supplier ORDER BY COALESCE(model_updated_date, CURRENT_TIMESTAMP) DESC) AS rank,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from 
    {{ ref('transformed_erm_requisition_doc_ctrl') }}
where 
    supplier is not null and supplier <> ''
)



select 
    id,
    project_code,
    supplier,
    supplier_name,
    created_date,
    last_updated_date,
    model_created_date,
    model_updated_date,
    model_load_id
from 
    cte_req_doc_ctrl_supplier_dim 
where 
    rank=1