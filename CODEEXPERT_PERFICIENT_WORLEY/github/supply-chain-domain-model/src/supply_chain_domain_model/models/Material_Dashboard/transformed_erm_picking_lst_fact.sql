{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_picking_lst_fact/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


    select 
            pick_lst_hdr.pick_lst_hdr_no,
            pick_lst_hdr.drl_hdr_no,
            idx,
            pick_lst_hdr_type_no,
            pick_lst_hdr.proj_no,
            pick_lst_hdr.act_no,
            pick_lst_hdr.act_corr_time,
            pick_lst_hdr.deliv_place,
            pick_lst_hdr.deliv_date,
            pick_lst_hdr.store_hdr_no as store_hdr_no,
            pick_lst_hdr.foreman,
            pick_lst_hdr.rem as pick_lst_hdr_rem,
            pick_lst_hdr.pick_lst_stat_no,
            pick_lst_hdr.prod_grp_no,
            pick_lst_hdr.customer_col_0 as pick_lst_hdr_customer_col_0,
            pick_lst_hdr.customer_col_1 as pick_lst_hdr_customer_col_1,
            pick_lst_hdr.customer_col_2 as pick_lst_hdr_customer_col_2,
            pick_lst_hdr.customer_col_3 as pick_lst_hdr_customer_col_3,
            pick_lst_hdr.customer_col_4 as pick_lst_hdr_customer_col_4,
            pick_lst_hdr.customer_col_5 as pick_lst_hdr_customer_col_5,
            pick_lst_hdr.customer_col_6 as pick_lst_hdr_customer_col_6,
            pick_lst_hdr.customer_col_7 as pick_lst_hdr_customer_col_7,
            pick_lst_hdr.customer_col_8 as pick_lst_hdr_customer_col_8,
            pick_lst_hdr.customer_col_9 as pick_lst_hdr_customer_col_9,
            pick_lst_hdr.def_date as pick_lst_hdr_def_date,
            pick_lst_hdr.def_usr_id as pick_lst_hdr_def_usr_id,
            pick_lst_hdr.upd_date as pick_lst_hdr_upd_date,
            pick_lst_hdr.upd_usr_id as pick_lst_hdr_upd_usr_id,
            pick_lst_hdr.subcontractor_no,
            pick_lst_ready,
            pick_lst_hdr_id,
            pick_lst_item.pick_lst_item_no,
            pick_lst_item.pl_item_no,
            pick_lst_item.drl_item_no,
            pick_lst_item.mat_grp_type_no,
            pick_lst_item.mat_no,
            pick_lst_item.proj_sys_brkdwn_no,
            pick_lst_item.po_item_no,
            pick_lst_item.store_item_no,
            requested_quan,
            pick_lst_item.proj_cost_acc_no,
            picked_quan,
            pick_lst_item.owner_proj_no,
            pick_lst_item.unit_id,
            requested_quan_stock_unit,
            pick_lst_item.relate_code,
            send_item_no,
            pick_lst_item.customer_col_0 as pick_lst_item_customer_col_0,
            pick_lst_item.customer_col_1 as pick_lst_item_customer_col_1,
            pick_lst_item.customer_col_2 as pick_lst_item_customer_col_2,
            pick_lst_item.customer_col_3 as pick_lst_item_customer_col_3,
            pick_lst_item.customer_col_4 as pick_lst_item_customer_col_4,
            pick_lst_item.customer_col_5 as pick_lst_item_customer_col_5,
            pick_lst_item.customer_col_6 as pick_lst_item_customer_col_6,
            pick_lst_item.customer_col_7 as pick_lst_item_customer_col_7,
            pick_lst_item.customer_col_8 as pick_lst_item_customer_col_8,
            pick_lst_item.customer_col_9 as pick_lst_item_customer_col_9,
            pick_lst_item.def_date as pick_lst_item_def_date,
            pick_lst_item.def_usr_id as pick_lst_item_def_usr_id,
            pick_lst_item.upd_date as pick_lst_item_upd_date,
            pick_lst_item.upd_usr_id as pick_lst_item_upd_usr_id,
            pick_lst_item.stock_unit_id,
            pick_quan,
            erm_pick_lst_hdr_cpv_fact.issue_date,
            erm_pick_lst_hdr_cpv_fact.ready_to_issue,
            proj.descr,
            store_hdr.store_hdr_id as  store_hdr_id,
            drl_hdr.rem as drl_hdr_rem,
            commodity.commodity_id,
            std_mat_scheme.size_1_value,  
            std_mat_scheme.size_2_value,
            std_mat_scheme.size_3_value,  
            std_mat_scheme.size_4_value,
            std_mat.std_mat_id as mat_id,
            mat.descr as descr_mat,
            drl_item.drl_item_id as drl_item_id,
            po_item.batch_id as batch_id,
            po_hdr.po_id as po_id,
            po_item.po_item_id as po_item_id,
            cont.name_1 as name_1,
            site_mto_hdr.site_mto_hdr_id as site_mto_hdr_id,
            mat_grp_type.mat_grp_type_id as mat_grp_type_id,
            case when 
            stock_trx.stock_trx_type_no=2 then stock_trx.quan 
            end as return_to_inv_quan,
            case when 
            stock_trx.stock_trx_type_no=5 then stock_trx.quan
            end as total_issued_quan,
            site.site_no,
            cast(pick_lst_item.execution_date as date) as etl_load_date,
            {{run_date}} as model_created_date,
            {{run_date}} as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id
        from {{ source('curated_erm', 'pick_lst_item') }} as pick_lst_item
            left join {{ source('curated_erm', 'pick_lst_hdr') }} as pick_lst_hdr
                on pick_lst_item.pick_lst_hdr_no=pick_lst_hdr.pick_lst_hdr_no
                and pick_lst_hdr.is_current = 1
            left join {{ source('curated_erm', 'pick_lst_hdr_cpv') }} as erm_pick_lst_hdr_cpv_fact
                on pick_lst_hdr.pick_lst_hdr_no=erm_pick_lst_hdr_cpv_fact.pick_lst_hdr_no
                and erm_pick_lst_hdr_cpv_fact.is_current = 1
            left join {{ source('curated_erm', 'proj') }} as proj
                on pick_lst_hdr.proj_no= proj.proj_no
                and proj.is_current = 1
            left join {{ source('curated_erm', 'store_hdr') }} as store_hdr
                on pick_lst_hdr.store_hdr_no=store_hdr.store_hdr_no
                and store_hdr.is_current = 1
            left join {{ source('curated_erm', 'drl_hdr') }} as drl_hdr
                on pick_lst_hdr.drl_hdr_no=drl_hdr.drl_hdr_no
                and drl_hdr.is_current = 1
            left join {{ source('curated_erm', 'std_mat') }} as std_mat
                on pick_lst_item.mat_no=std_mat.std_mat_no
                and std_mat.is_current = 1
            left join {{ source('curated_erm', 'commodity') }} as commodity
                on std_mat.commodity_no=commodity.commodity_no
                and commodity.is_current = 1
            left join {{ source('curated_erm', 'std_mat_scheme') }} as std_mat_scheme
                on std_mat.std_mat_no=std_mat_scheme.std_mat_no
                and std_mat_scheme.is_current = 1
            left join {{ source('curated_erm', 'mat') }} as mat
                on pick_lst_item.mat_no=mat.mat_no
                and mat.is_current = 1
            left join {{ source('curated_erm', 'drl_item') }} as drl_item
                on pick_lst_item.drl_item_no= drl_item.drl_item_no
                and drl_item.is_current = 1
            left join {{ source('curated_erm', 'po_item') }} as po_item
                on pick_lst_item.po_item_no=po_item.po_item_no
                and po_item.is_current = 1
            left join {{ source('curated_erm', 'po_hdr') }} as po_hdr
                on po_item.po_no=po_hdr.po_no
                and po_hdr.is_current = 1
            left join {{ source('curated_erm', 'cont') }} as cont
                on po_hdr.suppl_no=cont.cont_no
                and cont.is_current = 1
            left join {{ source('curated_erm', 'site_mto_item') }} as site_mto_item
                on drl_item.site_mto_item_no=site_mto_item.site_mto_item_no
                and site_mto_item.is_current = 1
            left join {{ source('curated_erm', 'site_mto_hdr') }} as site_mto_hdr
                on site_mto_item.site_mto_hdr_no=site_mto_hdr.site_mto_hdr_no
                and site_mto_hdr.is_current = 1
            left join {{ source('curated_erm', 'mat_grp_type') }} as mat_grp_type
                on pick_lst_item.mat_grp_type_no=mat_grp_type.mat_grp_type_no
                and mat_grp_type.is_current = 1
            left join {{ source('curated_erm', 'stock_trx') }} as stock_trx
                on pick_lst_item.pick_lst_item_no=stock_trx.pick_lst_item_no
                and stock_trx.is_current = 1
            left join {{ source('curated_erm', 'site') }} as site
                on drl_hdr.site_no=site.site_no
                and site.is_current = 1
            where pick_lst_item.is_current = 1 