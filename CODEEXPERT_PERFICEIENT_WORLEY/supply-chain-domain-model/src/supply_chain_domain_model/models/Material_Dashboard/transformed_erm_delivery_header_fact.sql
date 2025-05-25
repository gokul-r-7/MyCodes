{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_delivery_header_fact/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_drl_hdr as ( 
    select 
            dh.drl_hdr_no,
            dh.pick_lst_stat_no,
            dh.proj_no,
            dh.act_no,
            dh.act_corr_time,
            dh.fabricate_no,
            dh.manual_set_prod_date,
            dh.deliv_date,
            dh.site_no,
            dh.prod_grp_no,
            dh.mat_all_run_no,
            dh.planner_usr_id,
            dh.foreman,
            dh.int_trans_addr_id,
            dh.rem,
            dh.job_act_no,
            dh.site_mto_hdr_no,
            dh.subcontractor_no,
            dh.def_usr_id,
            dh.def_date,
            dh.upd_usr_id,
            dh.upd_date,
            dhc.material_request_no,
            dhc.deliver_to,
            dh.drl_hdr_type_no,
            dh.drl_close_date,
            dh.pick_lst_gen_date,
            dh.pick_usr_id,
            dh.sec_no,
            dh.so_id,
            dh.customer_col_0,
            dh.customer_col_1,
            dh.customer_col_2,
            dh.customer_col_3,
            dh.customer_col_4,
            dh.customer_col_5,
            dh.customer_col_6,
            dh.customer_col_7,
            dh.customer_col_8,
            dh.customer_col_9,
            cast(dh.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id
    from   
         {{ source('curated_erm', 'drl_hdr') }} dh
         left join {{ source('curated_erm', 'drl_hdr_cpv') }} dhc
            on dh.drl_hdr_no=dhc.drl_hdr_no
        WHERE 
            dh.is_current = 1
            AND dhc.is_current = 1
)

select * from cte_drl_hdr

