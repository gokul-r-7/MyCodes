{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_suppl_addr/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

with cte_cont_addr as ( 
    select 
        cont_addr.cont_addr_no as suppl_addr_no,
        cont_addr.cont_no as suppl_no,
        cont_addr.cont_addr_id as suppl_addr_id,
        cont_addr.addr_code_no as addr_code_no,
        (cont_addr.addr_addr_1||cont_addr.addr_addr_2||cont_addr.addr_addr_3||cont_addr.addr_addr_4) as addr,
        cont_addr.phone as phone_no,
        cont_addr.fax as fax_no,
        cont_addr.email as e_mail_addr,
        cont_addr.ref_person as ref_person,
        cont_addr.rem as rem,
        cast(cont_addr.execution_date as date) as etl_load_date,
		{{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id			
from (SELECT * FROM {{ source('curated_erm', 'cont_addr') }} WHERE is_current = 1) AS cont_addr
INNER JOIN (SELECT * FROM {{ source('curated_erm', 'cont') }} WHERE is_current = 1) AS cont
on cont_addr.cont_no = cont.cont_no where cont.cont_is_suppl = '1'
)

select * from cte_cont_addr