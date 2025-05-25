{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_w_exp_po_master_api/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

select
                  po.project,                  
                  po.client_name,
                  po.project_title,
                  po.jobsite_location,                  
                  po.project_number,
                  po.po_no,
                  po.purchase_order_no,
                  po.revision_date,
                  po.revision,
                  po.suppl_no,
                  po.supplier,
                  po.cont_no,
                  po.supp_addres_line1,
                  po.supp_addres_line2,
                  po.supp_addres_line3,
                  po.supp_addres_line4,
                  po.supp_postal_code,
                  po.supp_city,
                  po.supp_country,
                  po.county,
                  po.region,
                  po.state_province,
                  po.supplier_code,
                  po.attention,
                  po.mobile_phone,
                  po.fax,
                  po.email,                  
                  po.lcountry,                                    
                  po.buyer_name,
                  po.buyer_id,
                  po.buyer_phone,
                  po.buyer_fax,
                  po.buyer_email,
                  po.po_item_id,
                  po.po_item_no,
                  po.ver pi_ver,
                  po.quan,
                  po.commodity_code,
                  po.tag_number,
                  po.ident,
                  po.unit_price,
                  po.unit,
                  po.cur_id,
                  po.deliv_deadline,
                  po.site_id,
                  po.payment_term,                     
                  po.delivery_term,
                  po.purchase_order_descr,
                  po.approve_date,
                  po.ext_status,
                  po.mat_grp,
                  po.item_deliv_term,                  
                  po.int_status,
                  po.proj_fin_acc_code_id,
                  po.po_issue_date,
                  po.ver_raised_date,
                  po.coa,
                  po.deadline,                  
                  po.po_first_issue_date,
                 po.batch_po_item_no,
                 po.mat_no,
                 po.various_id,
                 req.req_no  ,  
                 short_descr,
                 long_descr,
                 proj_no,
				 cast(po.execution_date as date) as etl_load_date,
                {{run_date}} as model_created_date,
                {{run_date}} as model_updated_date,
                {{ generate_load_id(model) }} as model_load_id
from
(
  SELECT 
    proj.proj_id AS project,                  
    cont1.name_1 AS client_name,
    proj.descr AS project_title,
    proj.installation_loc AS jobsite_location,                  
    COALESCE(
        proj.fin_proj_id, 
        SUBSTRING(po_hdr.po_id, 1, CAST(proj_cpv.sub_project_no_length AS INTEGER))
    ) AS project_number,
    po_hdr.po_no,
    po_hdr.po_id AS purchase_order_no,
    po_hdr.upd_date AS revision_date,
    po_hdr.ver AS revision,
    po_hdr.suppl_no,
    cont.name_1 AS supplier,
    cont.cont_no,
    ca.addr_addr_1 AS supp_addres_line1,
    ca.addr_addr_2 AS supp_addres_line2,
    ca.addr_addr_3 AS supp_addres_line3,
    ca.addr_addr_4 AS supp_addres_line4,
    ca.addr_postal_code AS supp_postal_code,
    ca.addr_city AS supp_city,
    cntry.name AS supp_country,
    ca.addr_county AS county,
    ca.addr_region AS region,
    ca.addr_state_province AS state_province,
    cont.cont_id AS supplier_code,
    cont_person.name AS attention,
    cont_person.mobile_phone,
    cont_person.fax,
    cont_person.email,                  
    cntry.name AS lcountry,                  
    usr.name AS buyer_name,
    usr.usr_id AS buyer_id,
    usr.phone AS buyer_phone,
    usr.fax AS buyer_fax,
    usr.email AS buyer_email,
    pi.po_item_id,
    pi.po_item_no,
    pi.ver,
    pi.quan,
    c.commodity_id AS commodity_code,
    COALESCE(pi.mat_id, m.mat_id) AS tag_number,
    p.part_id AS ident,
    pi.price AS unit_price,
    pi.proc_unit_id AS unit,
    cur.cur_id,
    pi.deliv_deadline,
    COALESCE(s.site_id, da.deliv_addr_id) AS site_id,                                    
    pt.descr AS payment_term,                     
    COALESCE(dti.deliv_term_id, dt.deliv_term_id) 
        || ' ' || COALESCE(pi.deliv_place, po_hdr.deliv_place) AS delivery_term,
    po_hdr.title AS purchase_order_descr,
    po_hdr.approve_date,
    ext.po_ext_stat_id AS ext_status,
    mgt.mat_grp_type_id AS mat_grp,
    dti.deliv_term_id || '-' || pi.deliv_place AS item_deliv_term,                  
    CASE
        WHEN po_hdr.proc_int_stat_no = 1 THEN 'create/change'
        WHEN po_hdr.proc_int_stat_no = 2 THEN 'approval'
        WHEN po_hdr.proc_int_stat_no = 3 THEN 'ready/issued'
        WHEN po_hdr.proc_int_stat_no = 4 THEN 'retired/expired'
    END AS int_status,
    pfac.proj_fin_acc_code_id,
    po_hdr.issue_date AS po_issue_date,
    po_hdr.ver_raised_date,
    sb.sys_brkdwn_id AS coa,
    pi.deadline,                  
    po_hdr.po_first_issue_date,
    pi.batch_po_item_no,
    pi.mat_no,
    pi.various_id,
    pi.descr AS short_descr,
    pi.mat_txt AS long_descr,
    proj.proj_no,
    proj.execution_date
FROM {{ source('curated_erm', 'proj') }} proj
LEFT JOIN {{ source('curated_erm', 'proj_cpv') }} proj_cpv 
    ON proj.proj_no = proj_cpv.proj_no AND proj_cpv.is_current = 1 
INNER JOIN {{ source('curated_erm', 'po_hdr') }} po_hdr 
    ON proj.proj_no = po_hdr.proj_no AND po_hdr.is_current = 1
LEFT JOIN {{ source('curated_erm', 'po_item') }} pi 
    ON po_hdr.po_no = pi.po_no AND pi.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont') }} cont 
    ON po_hdr.suppl_no = cont.cont_no AND cont.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont') }} cont1 
    ON proj.client_no = cont1.cont_no AND cont1.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont_addr') }} ca 
    ON cont.cont_no = ca.cont_no 
	AND po_hdr.suppl_addr_no = ca.cont_addr_no  
	AND ca.is_current = 1
LEFT JOIN {{ source('curated_erm', 'country') }} cntry 
    ON ca.addr_country_id = cntry.country_id AND cntry.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont_person') }} cont_person 
    ON cont.cont_no = cont_person.cont_no 
    AND po_hdr.suppl_ref = cont_person.name
	AND cont_person.is_current = 1
INNER JOIN {{ source('curated_erm', 'usr') }} usr 
    ON po_hdr.merc_hand_usr_id = usr.usr_id AND usr.is_current = 1
LEFT JOIN {{ source('curated_erm', 'mat') }} m 
    ON pi.mat_no = m.mat_no AND m.is_current = 1		 
LEFT JOIN {{ source('curated_erm', 'part') }} p 
    ON m.mat_id = p.part_id AND p.is_current = 1
LEFT JOIN {{ source('curated_erm', 'commodity') }} c 
    ON p.commodity_no = c.commodity_no AND c.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cur') }} cur 
    ON po_hdr.cur_id = cur.cur_id AND cur.is_current = 1
LEFT JOIN {{ source('curated_erm', 'site') }} s 
    ON pi.deliv_addr_no = s.deliv_addr_no AND s.is_current = 1
LEFT JOIN {{ source('curated_erm', 'pay_term') }} pt 
    ON po_hdr.pay_term_no = pt.pay_term_no  AND pt.is_current = 1
LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dt 
    ON po_hdr.deliv_term_no = dt.deliv_term_no  AND dt.is_current = 1
LEFT JOIN {{ source('curated_erm', 'deliv_addr') }} da 
    ON po_hdr.deliv_addr_no = da.deliv_addr_no   AND da.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proc_ext_stat') }} ext 
    ON po_hdr.proc_ext_stat_no = ext.proc_ext_stat_no
LEFT JOIN {{ source('curated_erm', 'mat_grp_type') }} mgt 
    ON pi.mat_grp_type_no = mgt.mat_grp_type_no AND mgt.is_current = 1
LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dti 
    ON pi.deliv_term_no = dti.deliv_term_no  AND dti.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proj_fin_acc_code') }} pfac 
    ON pi.proj_fin_acc_code_no = pfac.proj_fin_acc_code_no  AND pfac.is_current = 1			 
LEFT JOIN {{ source('curated_erm', 'proj_sys_brkdwn') }} sb 
    ON po_hdr.proj_sys_brkdwn_no = sb.proj_sys_brkdwn_no  AND sb.is_current = 1						 
WHERE proj.is_current = 1
AND po_hdr.proc_int_stat_no = 3

union all

SELECT 
    proj.proj_id AS project,                  
    cont1.name_1 AS client_name,
    proj.descr AS project_title,
    proj.installation_loc AS jobsite_location,
    CASE
        WHEN proj_cpv.sub_project_no_length IS NULL 
        THEN proj.fin_proj_id
        ELSE SUBSTRING(po_hdr.po_id, 1, CAST(proj_cpv.sub_project_no_length AS INTEGER))
    END AS project_number,
    po_hdr.po_no,
    po_hdr.po_id AS purchase_order_no,
    po_hdr.def_date AS revision_date,
    po_hdr.ver AS revision,
    po_hdr.suppl_no,
    cont.name_1 AS supplier,
    cont.cont_no,
    ca.addr_addr_1 AS address_line1,
    ca.addr_addr_2 AS address_line2,
    ca.addr_addr_3 AS address_line3,
    ca.addr_addr_4 AS address_line4,
    ca.addr_postal_code AS postal_code,
    ca.addr_city AS city,
    cntry.name AS country,
    ca.addr_county AS county,
    ca.addr_region AS region,
    ca.addr_state_province AS state_province,
    cont.cont_id AS supplier_code,
    cont_person.name AS attention,
    cont_person.mobile_phone,
    cont_person.fax,
    cont_person.email,
    cntry.name AS lcountry,                  
    usr.name AS buyer_name,
    usr.usr_id AS buyer_id,
    usr.phone AS buyer_phone,
    usr.fax AS buyer_fax,
    usr.email AS buyer_email,
    pih.po_item_id,
    pih.po_item_no,
    pih.ver,
    pih.quan,
    c.commodity_id AS commodity_code,
    CASE 
        WHEN c.commodity_id IS NULL THEN COALESCE(pih.mat_id, m.mat_id) 
        ELSE NULL 
    END AS tag_number, 
    p.part_id AS ident,                  
    pih.price AS unit_price,
    pih.proc_unit_id AS unit,
    cur.cur_id,
    pih.deliv_deadline,
    COALESCE(s.site_id, da.deliv_addr_id) AS site_id,                  
    pt.descr AS payment_term,
    COALESCE(dti.deliv_term_id, dt.deliv_term_id) || ' ' || COALESCE(pih.deliv_place, po_hdr.deliv_place) AS delivery_term,
    po_hdr.title AS purchase_order_descr,
    po_hdr.approve_date,
    'issued' AS ext_status,
    mgt.mat_grp_type_id AS mat_grp,
    dti.deliv_term_id || '-' || pih.deliv_place AS item_deliv_term,
    'ready/issued' AS int_status,
    pfac.proj_fin_acc_code_id,
    po_hdr.issue_date AS po_issue_date,
    po_hdr.ver_raised_date,
    sb.sys_brkdwn_id AS coa,
    pih.deadline,
    po_hdr.po_first_issue_date,
    NULL AS batch_po_item_no,
    pih.mat_no,
    pih.various_id,
    pih.descr AS short_descr,
    pih.mat_txt AS long_descr,
    proj.proj_no,
    proj.execution_date
FROM 
    {{ source('curated_erm', 'proj') }} proj
LEFT JOIN 
    {{ source('curated_erm', 'proj_cpv') }} proj_cpv 
    ON proj.proj_no = proj_cpv.proj_no 
    AND proj_cpv.is_current = 1
INNER JOIN 
    {{ source('curated_erm', 'po_hdr_hist') }} po_hdr
    ON proj.proj_no = po_hdr.proj_no 
	  AND po_hdr.is_current = 1		
INNER JOIN 
    {{ source('curated_erm', 'po_hdr_hist') }} hh2
    ON po_hdr.po_no = hh2.po_no
	AND hh2.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'po_item_hist') }} pih
    ON hh2.po_hdr_hist_no = pih.po_hdr_hist_no AND pih.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'cont') }} cont
    ON po_hdr.suppl_no = cont.cont_no AND cont.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'cont') }} cont1
    ON proj.client_no = cont1.cont_no AND cont1.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'cont_addr') }} ca
    ON cont.cont_no = ca.cont_no
    AND po_hdr.suppl_addr_no = ca.cont_addr_no
	AND ca.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'country') }} cntry
    ON ca.addr_country_id = cntry.country_id AND cntry.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'cont_person') }} cont_person
    ON cont.cont_no = cont_person.cont_no
    AND TRIM(UPPER(po_hdr.suppl_ref)) = TRIM(UPPER(cont_person.name))	
	AND cont_person.is_current = 1		
INNER JOIN 
    {{ source('curated_erm', 'usr') }} usr
    ON po_hdr.merc_hand_usr_id = usr.usr_id AND usr.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'mat') }} m
    ON pih.mat_no = m.mat_no  
    AND m.is_current = 1
LEFT JOIN 
    {{ source('curated_erm', 'part') }} p 
    ON m.mat_id = p.part_id AND p.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'commodity') }} c  
    ON p.commodity_no = c.commodity_no AND c.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'cur') }} cur 
    ON po_hdr.cur_id = cur.cur_id 
    AND cur.is_current = 1
LEFT JOIN 
    {{ source('curated_erm', 'site') }} s
    ON pih.deliv_addr_no = s.deliv_addr_no AND s.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'pay_term') }} pt
    ON po_hdr.pay_term_no = pt.pay_term_no AND pt.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'deliv_term') }} dt
    ON po_hdr.deliv_term_no = dt.deliv_term_no AND dt.is_current = 1		                 
LEFT JOIN 
    {{ source('curated_erm', 'deliv_addr') }} da 
    ON po_hdr.deliv_addr_no = da.deliv_addr_no AND da.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'mat_grp_type') }} mgt 
    ON pih.mat_grp_type_no = mgt.mat_grp_type_no AND mgt.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'deliv_term') }} dti 
    ON pih.deliv_term_no = dti.deliv_term_no AND dti.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'proj_fin_acc_code') }} pfac
    ON pih.proj_fin_acc_code_no = pfac.proj_fin_acc_code_no AND pfac.is_current = 1		
LEFT JOIN 
    {{ source('curated_erm', 'proj_sys_brkdwn') }} sb
    ON po_hdr.proj_sys_brkdwn_no = sb.proj_sys_brkdwn_no AND sb.is_current = 1		
WHERE 
    proj.is_current = 1
    AND pih.ver = (
        SELECT MAX(ih2.ver)
        FROM {{ source('curated_erm', 'po_item_hist') }} ih2 
        JOIN {{ source('curated_erm', 'po_hdr_hist') }} po_hdr
        ON ih2.po_hdr_hist_no = po_hdr.po_hdr_hist_no
        WHERE ih2.ver <= po_hdr.ver
		AND ih2.is_current = 1	
		AND po_hdr.is_current = 1	
    )
    AND po_hdr.ver = (
        SELECT MAX(phh2.ver) 
        FROM {{ source('curated_erm', 'po_hdr_hist') }} phh2 
        JOIN {{ source('curated_erm', 'po_hdr_hist') }} po_hdr
        ON phh2.po_no = po_hdr.po_no
        WHERE NOT EXISTS (
            SELECT NULL
            FROM {{ source('curated_erm', 'po_hdr') }} h3
            WHERE h3.po_no = phh2.po_no
            AND h3.ver = phh2.ver
            AND h3.proc_int_stat_no <> 3
        )
        AND phh2.is_current = 1	
		AND po_hdr.is_current = 1	
    )
    AND NOT EXISTS (
        SELECT 1 
        FROM {{ source('curated_erm', 'po_hdr') }} p 
        JOIN {{ source('curated_erm', 'po_hdr_hist') }} po_hdr
        ON p.po_no = po_hdr.po_no 
        AND p.ver = po_hdr.ver
        AND p.is_current = 1	
		AND po_hdr.is_current = 1
    )
          
) po
LEFT OUTER JOIN (
    SELECT 
        po_no, 
        CONCAT_WS(',', COLLECT_LIST(tmr_id)) AS req_no
    FROM (
        SELECT 
            MAX(v1.tmr_id) AS tmr_id,
            v1.po_no
        FROM {{ ref('transformed_esr_cext_wp_po_tmr') }} v1 
        GROUP BY v1.po_no
    ) subquery
    GROUP BY po_no
) req
ON po.po_no = req.po_no

