{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT 
    DISTINCT
    epdd.proj_id AS project_id,
    edhf.drl_hdr_no,
    edhf.deliv_date AS required_date,
    edhf.mat_all_run_no,
    edhf.planner_usr_id AS entered_by,
    edhf.foreman AS requested_by,
    edhf.rem AS remarks,
    edhf.material_request_no,
    edhf.deliver_to,
    edif.drl_item_id AS linked_to_line_no,
    edif.mat_no AS item_no,
    edif.descr AS description,
    edif.drl_item_quan AS requested_qty,
    edif.draw_no AS pmto_drawing_sheet,
    edif.mto_pos AS pmto_pos,
    edif.drl_item_no,
    edif.drl_hdr_no AS drl_hdr_no_item_fact,
    emgtd.descr AS material_type,
    eplsd.pick_lst_stat_id,
    esd.site_id AS site,
    eplf.pick_lst_hdr_id,  
    eplf.pick_lst_ready,
    eplf.deliv_date AS pick_list_required_date,  
    eplf.issue_date,
    eplf.ready_to_issue,
    esmhd.site_mto_hdr_id,
    eshd.store_hdr_id AS warehouse,
    es.suppl_id AS cont_id,
    es.name_1,
    CASE                        
        WHEN eplf.issue_date IS NULL OR edhf.deliv_date IS NULL THEN 'In Progress'
        WHEN eplf.issue_date > edhf.deliv_date THEN 'Late'
        WHEN eplf.issue_date = edhf.deliv_date THEN 'On Time'
        WHEN eplf.issue_date < edhf.deliv_date THEN 'Early'
        ELSE NULL 
    END AS "Issue Status",
    t1.rank as index,
    t2.ready_to_issue_chk,
    t2.Issuedt_present,
    CASE
        WHEN eplsd.pick_lst_stat_id = 'O-DRL' THEN 'O-DRL - DRL Not Completed'
        WHEN eplsd.pick_lst_stat_id = 'C-DRL' THEN 'C-DRL - DRL Completed'
        WHEN eplsd.pick_lst_stat_id = 'WAREH' THEN 'WAREH - DRL In Warehouse Dispatch List'
        WHEN eplsd.pick_lst_stat_id = 'CPICK' AND t2.ready_to_issue_chk = 0 AND t2.Issuedt_present = 0 THEN 'CPICK - Picking List Created'
        WHEN eplsd.pick_lst_stat_id = 'CPICK' AND t2.ready_to_issue_chk = 1 AND t2.Issuedt_present = 0 THEN 'READY - Material Physically Picked'
        WHEN eplsd.pick_lst_stat_id = 'CPICK' AND t2.ready_to_issue_chk = 0 AND t2.Issuedt_present = 1 THEN 'CPICK - Material Physically Issued, Not Picked in ERM'
        WHEN eplsd.pick_lst_stat_id = 'CPICK' AND t2.ready_to_issue_chk = 1 AND t2.Issuedt_present = 1 THEN 'CPICK - Material Physically Issued, Not Picked in ERM'
        WHEN eplsd.pick_lst_stat_id = 'PPICK' AND t2.ready_to_issue_chk = 0 AND t2.Issuedt_present = 0 THEN 'PPICK - Material Picked in ERM'
        WHEN eplsd.pick_lst_stat_id = 'PPICK' AND t2.ready_to_issue_chk = 1 AND t2.Issuedt_present = 0 THEN 'PPICK - Material Picked in ERM'
        WHEN eplsd.pick_lst_stat_id = 'PPICK' AND t2.ready_to_issue_chk = 0 AND t2.Issuedt_present = 1 THEN 'PPICK - Material Physically Issued, Picked in ERM'
        WHEN eplsd.pick_lst_stat_id = 'PPICK' AND t2.ready_to_issue_chk = 1 AND t2.Issuedt_present = 1 THEN 'PPICK - Material Physically Issued, Picked in ERM'
        ELSE NULL
    END AS pick_list_status,
    edhf.etl_load_date AS dataset_refreshed_date
FROM
    {{ source('supply_chain_dim', 'erm_delivery_header_fact') }} edhf
    LEFT JOIN {{ source('supply_chain_dim', 'erm_delivery_item_fact') }} edif ON edhf.drl_hdr_no = edif.drl_hdr_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_mat_grp_type_dim') }} emgtd ON edif.mat_grp_type_no = emgtd.mat_grp_type_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_pick_lst_stat_dim') }} eplsd ON edhf.pick_lst_stat_no = eplsd.pick_lst_stat_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_picking_lst_fact') }} eplf ON edhf.drl_hdr_no = eplf.drl_hdr_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_store_hdr_dim') }} eshd ON eplf.store_hdr_no = eshd.store_hdr_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_site_dim') }} esd ON edhf.site_no = esd.site_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_site_mto_hdr_dim') }} esmhd ON edhf.site_mto_hdr_no = esmhd.site_mto_hdr_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_suppl') }} es ON edhf.subcontractor_no = es.suppl_no
    LEFT JOIN {{ source('supply_chain_dim', 'erm_project_details_dim') }} epdd ON edhf.proj_no = epdd.proj_no
    LEFT JOIN(
        SELECT
        DISTINCT
        drl_hdr_no,
        issue_date,
        ready_to_issue,
        CASE 
            when 
        COUNT(drl_hdr_no) over (PARTITION BY drl_hdr_no)  =  COUNT(CASE WHEN ready_to_issue IS NOT NULL THEN drl_hdr_no END) OVER (PARTITION BY drl_hdr_no)
        then 1 else 0 end as ready_to_issue_chk ,
        CASE
            when
        COUNT(CASE WHEN issue_date IS NOT NULL THEN drl_hdr_no END) OVER (PARTITION BY drl_hdr_no)=  COUNT(CASE WHEN ready_to_issue IS NOT NULL THEN drl_hdr_no END) OVER (PARTITION BY drl_hdr_no)
        then 1 else 0 end as Issuedt_present 

FROM
    {{ source('supply_chain_dim', 'erm_picking_lst_fact') }}) t2 ON edhf.drl_hdr_no = t2.drl_hdr_no
    LEFT JOIN (
        SELECT 
            DISTINCT
            drl_hdr_no,
            deliv_date,
            RANK() OVER (PARTITION BY deliv_date ORDER BY drl_hdr_no) AS rank
        FROM 
            {{ source('supply_chain_dim', 'erm_delivery_header_fact') }}
        GROUP BY 
            drl_hdr_no, deliv_date
    ) t1 ON 
     edhf.drl_hdr_no = t1.drl_hdr_no AND edhf.deliv_date = t1.deliv_date
