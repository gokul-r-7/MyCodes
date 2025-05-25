{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_suppl/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


with cte_cont as ( 
    select  cont.cont_no as suppl_no,
            cont.cont_id as suppl_id,
            cont.suppl_stat as stat,
            cont.name_1 as name_1,
            cont.name_2 as name_2,
            cont.suppl_main_cont_addr_no as main_addr_no,
            cont.country_id as country_id,
            cont.lang_id as lang_id,
            cont.suppl_pay_term_no as pay_term_no,
            cont.suppl_trans_term_no as trans_term_no,
            cont.suppl_deliv_term_no as deliv_term_no,
            cont.deliv_place as deliv_place,
            cont.creditor_no as creditor_no,
            cont.vat_reg_no as vat_reg_no,
            cont.suppl_cur_id as cur_id,
            cont.proc_hand_usr_id as proc_hand_usr_id,
            cont.suppl_rating as rating,
            cont.suppl_rem as rem,
            cont.suppl_agent_no as agent_no,
            cont.suppl_portal_access as suppl_portal_access,
            cast(cont.execution_date as date) as etl_load_date,
			{{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id	
    from   
         {{ source('curated_erm', 'cont') }}  cont
    where 
    cont.cont_is_suppl = '1' and is_current = 1
)

select * from cte_cont