{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_delivery_item_fact/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with cte_drl_item as ( 

        SELECT 
        drl_item.drl_item_id, 
        drl_item.mat_grp_type_no, 
        drl_item.mat_no, 
        DECODE(drl_item.mat_grp_type_no, 1, std_mat.descr, 2, proj_comp.descr) AS descr,
        drl_item.proj_sys_brkdwn_no,
        drl_item.quan as drl_item_quan ,
        drl_item.unit_id, 
        drl_item.no_pick, 
        pick_lst_item.requested_quan, 
        po_item.quan as po_item_quan,
        drl_item.close_out_quan,
        drl_item.close_out_disp,
        drl_item.drl_item_id as linked_to_line_no,
        drl_item.ref_type,
        drl_item.relate_code,
        site_mto_hdr.draw_no,
        pl_item.ver,
        pl_item.line_no,
        pl_item.draw_pos_id,
        std_mat.std_mat_id,
        drl_item.proj_cost_acc_no,
        std_mat.dim,
        std_mat.mat_type_dflt_no,
        std_mat.type_descr,
        drl_item.proj_compartment_no,
        std_mat.wgt,
        std_mat.wgt * drl_item.quan,
        drl_item.norms_quan,
        drl_item.norms_unit_id,
        drl_item.def_usr_id,
        drl_item.def_date,
        drl_item.upd_usr_id,
        drl_item.upd_date,
        pl_hdr_cpv.cwp,
        site_mto_hdr_cpv.iwp,
        draw.draw_id,
        pl_item.line_no as mto_pos,
        drl_item.drl_item_no,
        drl_item.pl_item_no,
        drl_item.site_mto_item_no,
        drl_hdr.drl_hdr_no,
        cast(drl_item.execution_date as date) as etl_load_date,
        {{run_date}} as model_created_date,
        {{run_date}} as model_updated_date,
        {{ generate_load_id(model) }} as model_load_id
    FROM 
        {{ source('curated_erm', 'drl_item') }} AS drl_item
    LEFT JOIN 
        {{ source('curated_erm', 'proj_comp') }} AS proj_comp
        ON drl_item.mat_no = proj_comp.proj_comp_no
        AND proj_comp.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'std_mat') }} AS std_mat
        ON drl_item.mat_no = std_mat.std_mat_no
        AND std_mat.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'pick_lst_item') }} AS pick_lst_item
        ON drl_item.drl_item_no = pick_lst_item.drl_item_no
        AND pick_lst_item.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'po_item') }} AS po_item
        ON pick_lst_item.po_item_no = po_item.po_item_no
        AND po_item.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'site_mto_item') }} AS site_mto_item
        ON drl_item.site_mto_item_no = site_mto_item.site_mto_item_no
        AND site_mto_item.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'site_mto_hdr') }} AS site_mto_hdr
        ON site_mto_item.site_mto_hdr_no = site_mto_hdr.site_mto_hdr_no
        AND site_mto_hdr.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'draw') }} AS draw
        ON site_mto_hdr.draw_no = draw.draw_no
        AND draw.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'pl_item') }} AS pl_item
        ON drl_item.pl_item_no = pl_item.pl_item_no
        AND pl_item.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'drl_hdr') }} AS drl_hdr
        ON drl_item.drl_hdr_no = drl_hdr.drl_hdr_no
        AND drl_hdr.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'site_mto_hdr_cpv') }} AS site_mto_hdr_cpv
        ON site_mto_hdr.site_mto_hdr_no = site_mto_hdr_cpv.site_mto_hdr_no
        AND site_mto_hdr_cpv.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'pl_hdr') }} AS pl_hdr
        ON site_mto_hdr.pl_hdr_no = pl_hdr.pl_hdr_no
        AND pl_hdr.is_current = 1
    LEFT JOIN 
        {{ source('curated_erm', 'pl_hdr_cpv') }} AS pl_hdr_cpv 
        ON pl_hdr.pl_hdr_no = pl_hdr_cpv.pl_hdr_no
        AND pl_hdr_cpv.is_current = 1
    WHERE 
        drl_item.is_current = 1
)

select * from cte_drl_item 