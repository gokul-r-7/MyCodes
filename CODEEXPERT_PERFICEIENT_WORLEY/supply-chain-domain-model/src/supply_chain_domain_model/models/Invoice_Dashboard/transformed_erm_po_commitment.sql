{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_po_commitment/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select 
po_no,
po_id,
ver,
proj_no,
proj_id,
proj_descr,
title,
suppl_name,
suppl_no,
client_po_no,
issue_date,
ros_date,
contract_delivery_date,
approve_date,
merc_hand_usr_id,
origin,
origin_descr,
origin_no,
disc_percent,
logo_file,
cur_id,
net_po_cost,
net_other_po_cost,
net_po_discount,
net_po_price,
sub_project,
incoterm,
delivery_location   ,        
delivery_location1,
po_close_date,
received_total,
pp_inv_amt,
inv_tot_against_commt as inv_tot_against_commit,
inv_tax_fright,
pct_po_complete,  
inv_stat_flg,
debit_bal,
customer,
name as entity,
bank_guarantee_expires_1 as bg_expiry,
bank_guarantee_expires_2 as bg_needed,
bank_guarantee_descr as bg_description,
bank_guarantee_date as bg_arrival,
bank_guarantee_usr_id as user_accepting_bg,
contact,
cont_email,
def_date,
rev_date,
po_type,
cast(execution_date as date) as etl_load_date,
{{run_date}} as model_created_date,
{{run_date}} as model_updated_date,
{{ generate_load_id(model) }} as model_load_id	


from
(

SELECT 
    h.po_no,
    h.po_id,
    hh.ver,
    p.proj_no,
    p.proj_id,
    CONCAT(p.proj_id, ' - ', p.descr) AS proj_descr,
    h.title,
    TRIM(CONCAT(c.name_1, ' ', c.name_2)) AS suppl_name,
    h.suppl_no,
    hhc.cust_po_no AS client_po_no,
    hh.issue_date AS issue_date,
    hh.deadline AS ros_date,
    cdd.contract_delivery_date,
    hh.approve_date,
    h.merc_hand_usr_id,
    origin_dtl.origin,
    origin_dtl.origin_desc AS origin_descr,
    hhc.origin AS origin_no,
    h.disc_percent,
    l.logo_file,
    h.cur_id,
    net_po_issd_price.net_po_cost AS net_po_cost,
    net_po_issd_price.net_po_cost AS net_other_po_cost,
    0 AS net_po_discount,
    net_po_issd_price.net_po_cost AS net_po_price,
    COALESCE(COALESCE(hhc.sub_project, hc.sub_project), 'none') AS sub_project,
    inc_dl.incoterm,
    inc_dl.delivery_location,           
    inc_dl.delivery_location1,
    ehc.po_close_date,
    r_tot.received_total - COALESCE(cr.credit_bal, 0) AS received_total,
    ppp.pp_inv_amt,
    ii.inv_amt AS inv_tot_against_commt,
    tf.inv_tax_fright AS inv_tax_fright,
    CASE 
        WHEN net_po_issd_price.net_po_cost = 0 THEN NULL
        ELSE ROUND((COALESCE(ii.inv_amt, ppp.pp_inv_amt) / net_po_issd_price.net_po_cost * 100), 1)
    END AS pct_po_complete,  
    0 AS inv_stat_flg,
    debit_bal,
    CASE 
        WHEN COALESCE(COALESCE(hhc.sub_project, hc.sub_project), 'none') <> 'none' AND LENGTH(pstl.text) <> 0 THEN pstl.text          
        WHEN COALESCE(hc.customer, 'none') <> 'none' THEN hc.customer
        ELSE cont.name_1 
    END AS customer,
    l.name,
    hc.bank_guarantee_expires AS bank_guarantee_expires_1,
    CASE WHEN h.bank_guarantee_needed = 1 THEN 'yes' ELSE 'no' END AS bank_guarantee_expires_2,
    h.bank_guarantee_descr,
    h.bank_guarantee_date,
    h.bank_guarantee_usr_id,
    cp.name AS contact,
    cp.email AS cont_email,
    h.def_date,
    hc.rev_date,
    cvli.value AS po_type,
    h.execution_date	
FROM 
     {{ source('curated_erm', 'po_hdr') }}  h
	 INNER JOIN {{ source('curated_erm', 'proj') }}  p 
        ON h.proj_no = p.proj_no 
        AND p.is_current = 1
     
     INNER JOIN {{ source('curated_erm', 'cont') }}  c 
        ON h.suppl_no = c.cont_no 
        AND c.is_current = 1
     
     INNER JOIN {{ source('curated_erm', 'legal_entity') }}  l 
        ON h.legal_entity_no = l.legal_entity_no	
        AND l.is_current = 1  
     
     INNER JOIN {{ source('curated_erm', 'po_hdr_hist') }}  hh 
        ON h.po_no = hh.po_no 
        AND hh.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'po_hdr_cpv') }}  hc 
        ON h.po_no = hc.po_no	
        AND hc.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'po_hdr_hist_cpv') }}  hhc 
        ON hh.po_hdr_hist_no  = hhc.po_hdr_hist_no 
        AND hhc.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'expedite_hdr') }}  eh 
        ON h.po_no = eh.po_no  
        AND eh.is_current = 1	   
     
     LEFT JOIN  {{ source('curated_erm', 'expedite_hdr_cpv') }}  ehc 
        ON eh.expedite_hdr_no = ehc.expedite_hdr_no 
        AND ehc.is_current = 1	
     
     LEFT JOIN  (
         SELECT REPLACE(proj_std_txt_id, '_C', '') AS proj_std_txt_id, proj_std_txt_no, is_current 
         FROM {{ source('curated_erm', 'proj_std_txt') }} 
     ) pstx  
         ON COALESCE(hhc.sub_project, hc.sub_project, 'none') = pstx.proj_std_txt_id 
         AND pstx.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'proj_std_txt_lang') }}  pstl 
        ON pstx.proj_std_txt_no = pstl.proj_std_txt_no 
        AND pstl.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'cont') }}  cont 
        ON p.client_no  = cont.cont_no 
        AND cont.is_current = 1		 
     
     LEFT JOIN  {{ source('curated_erm', 'cont_person') }}  cp 
        ON h.suppl_ref = cp.name 
        AND h.suppl_no  = cp.cont_no 
        AND cp.is_current = 1	
     
     LEFT JOIN  {{ source('curated_erm', 'cusp_value_list_item') }}  cvli 
        ON hc.po_type  = cvli.value_list_item_no 
        AND cvli.is_current = 1	  
 	 
     LEFT JOIN {{ ref('transformed_cust_report_get_cpv_dddw') }} origin_dtl 
        ON h.po_no = origin_dtl.po_no
 	 
     LEFT JOIN {{ ref('transformed_cust_report_net_po_issd_price') }} net_po_issd_price 
        ON h.po_no = net_po_issd_price.po_no 
        AND hh.ver = net_po_issd_price.ver	
     
     LEFT JOIN {{ ref('transformed_inv_tax_fright') }} tf 
        ON h.po_no = tf.po_no
		
     left join 
              (
				SELECT 
					phh.po_no, 
					phh.po_id, 
					phh.ver, 
					MIN(pih.agreed_deadline) AS contract_delivery_date
				FROM {{ source('curated_erm', 'po_item_hist') }} pih
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh 
					ON pih.po_hdr_hist_no = phh.po_hdr_hist_no
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1 
					ON phh.po_no = phh1.po_no
				WHERE 
					pih.delivery_type_no = 1
					AND pih.is_current = 1
					AND phh.is_current = 1
					AND phh1.is_current = 1
					AND pih.ver = (
						SELECT MAX(pih1.ver) 
						FROM {{ source('curated_erm', 'po_item_hist') }} pih1 
						WHERE pih1.po_item_no = pih.po_item_no 
						AND pih1.ver <= phh.ver 
						AND pih1.is_current = 1
					)
					AND phh.ver = (
						SELECT MAX(phh2.ver) 
						FROM {{ source('curated_erm', 'po_hdr_hist') }} phh2
						WHERE phh2.po_no = phh.po_no 
						AND phh2.is_current = 1
					)
				GROUP BY phh.po_no, phh.po_id, phh.ver
				) cdd
		on 	h.po_no = cdd.po_no		

     left join 
              (
					SELECT 
						po_no,
						ARRAY_JOIN(COLLECT_LIST(incoterm), ', ') AS incoterm,
						ARRAY_JOIN(COLLECT_LIST(delivery_term), '; ') AS delivery_location,
						ARRAY_JOIN(COLLECT_LIST(delivery_term), '; ') AS delivery_location1
					FROM (
						SELECT DISTINCT  
							COALESCE(dti.deliv_term_id, dt.deliv_term_id) AS incoterm,  
							COALESCE(pi.deliv_place, ph1.deliv_place) AS delivery_term,  
							ph1.po_no,  
							ph1.ver  
						FROM {{ source('curated_erm', 'po_hdr') }} ph1  
						JOIN {{ source('curated_erm', 'po_item') }} pi  
							ON ph1.po_no = pi.po_no 
						LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dti  
							ON pi.deliv_term_no = dti.deliv_term_no  
						LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dt  
							ON ph1.deliv_term_no = dt.deliv_term_no 
						WHERE pi.is_current = 1  
						  AND dti.is_current = 1 
						  AND dt.is_current = 1 
						  AND ph1.is_current = 1
					) 
					GROUP BY po_no
				) inc_dl
		on 	h.po_no = inc_dl.po_no			
		
     left join 
              (
			  SELECT  
					phh.proj_no,  
					phh.po_no,  
					phh.po_id,  
					SUM(ri.receive_unit_quan * pih.price) AS received_total  
				FROM {{ source('curated_erm', 'receipt_hdr') }} rh  
				JOIN {{ source('curated_erm', 'receipt_item') }} ri  
					ON rh.receipt_hdr_no = ri.receipt_hdr_no  
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh  
					ON rh.po_no = phh.po_no  
				JOIN {{ source('curated_erm', 'po_item_hist') }} pih  
					ON ri.po_item_no = pih.po_item_no  
				LEFT JOIN (  
					SELECT po_item_no, MAX(ver) AS max_ver  
					FROM {{ source('curated_erm', 'po_item_hist') }}  
					WHERE is_current = 1  
					GROUP BY po_item_no  
				) pih_max  
					ON pih.po_item_no = pih_max.po_item_no  
					AND pih.ver = pih_max.max_ver  
				LEFT JOIN (  
					SELECT po_no, MAX(ver) AS max_ver  
					FROM {{ source('curated_erm', 'po_hdr_hist') }}  
					WHERE is_current = 1  
					GROUP BY po_no  
				) phh_max  
					ON phh.po_no = phh_max.po_no  
					AND phh.ver = phh_max.max_ver  
				LEFT JOIN {{ source('curated_erm', 'po_hdr') }} ph  
					ON ph.po_no = phh.po_no  
					AND ph.ver = phh.ver  
					AND ph.proc_int_stat_no <> 3  
					AND ph.is_current = 1  
				WHERE rh.receipt_stat_no = 6  
				AND rh.is_current = 1  
				AND phh.is_current = 1  
				AND pih.is_current = 1  
				AND ri.is_current = 1  
				AND phh_max.max_ver IS NOT NULL  
				AND pih_max.max_ver IS NOT NULL  
				GROUP BY phh.proj_no, phh.po_no, phh.po_id
			) r_tot
		on 	h.po_no = r_tot.po_no		
		
		
     left join 		 
    (
	SELECT  
			phh.proj_no,  
			phh.po_no,  
			phh.po_id,  
			SUM(CASE WHEN pih.price < 0 THEN COALESCE(pih.quan, 0) * ABS(pih.price) ELSE 0 END) AS credit_bal,  
			SUM(CASE WHEN pih.price > 0 THEN COALESCE(pih.quan, 0) * ABS(pih.price) ELSE 0 END) AS debit_bal  
		FROM {{ source('curated_erm', 'po_hdr_hist') }} phh  
		JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1  
			ON phh.po_no = phh1.po_no  
		JOIN {{ source('curated_erm', 'po_item_hist') }} pih  
			ON phh1.po_hdr_hist_no = pih.po_hdr_hist_no  
		WHERE  
			pih.mat_grp_type_no = 3  
			AND phh.is_current = 1  
			AND phh1.is_current = 1  
			AND pih.is_current = 1  
			AND pih.ver = (  
				SELECT MAX(pih2.ver)  
				FROM {{ source('curated_erm', 'po_item_hist') }} pih2  
				WHERE pih2.po_item_no = pih.po_item_no  
				AND pih2.is_current = 1  
				AND pih2.ver <= phh.ver  
			)  
			AND phh.ver = (  
				SELECT MAX(hh2.ver)  
				FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2  
				WHERE hh2.po_no = phh.po_no  
				AND hh2.is_current = 1  
				AND NOT EXISTS (  
					SELECT 1  
					FROM {{ source('curated_erm', 'po_hdr') }} h3  
					WHERE h3.po_no = hh2.po_no  
					AND h3.ver = hh2.ver  
					AND h3.proc_int_stat_no <> 3  
					AND h3.is_current = 1  
				)  
			)  
		GROUP BY phh.proj_no, phh.po_no, phh.po_id
    ) cr
	on h.po_no = cr.po_no		
	
     left join 
              (
	select po_no
      ,sum(pp_inv_amt) pp_inv_amt
      ,sum(tran_acpt_pp_inv_amt) tran_acpt_pp_inv_amt
      ,sum(paid_stat_pp_inv_amt) paid_stat_pp_inv_amt
	 from 
	      (
			   select ii.po_no
			  ,sum(ii.amount) pp_inv_amt
			  ,decode(ih.inv_hdr_stat_no,7,sum(ii.amount),0) tran_acpt_pp_inv_amt
			  ,decode(ih.inv_hdr_stat_no,9,sum(ii.amount),0) paid_stat_pp_inv_amt
				from  {{ source('curated_erm', 'po_hdr') }} ph
					 ,{{ source('curated_erm', 'inv_hdr') }} ih
					 ,{{ source('curated_erm', 'inv_item') }} ii
					 ,{{ source('curated_erm', 'po_pay_plan') }} ppp
				where ph.po_no=ih.po_no
				and   ih.inv_hdr_no = ii.inv_hdr_no
				and   ph.po_no = ppp.po_no
				and   ppp.po_pay_plan_no = ii.po_pay_plan_no
				and ih.inv_hdr_stat_no in (7,9)
				and ii.po_pay_plan_no is not null				
				and ph.is_current = 1
				and ih.is_current = 1
				and ii.is_current = 1
				and ppp.is_current = 1
				group by ii.po_no,ih.inv_hdr_stat_no
				)
				group by po_no
				) ppp
		on 	h.po_no = ppp.po_no	
		
     left join 
              (
				SELECT  
					phh.proj_no,  
					phh.po_no,  
					phh.po_id,  
					SUM(ii.amount) AS inv_amt,  
					SUM(CASE WHEN ih.inv_hdr_stat_no = 7 THEN ii.amount ELSE 0 END) AS trans_accpt_inv_amt,  
					SUM(CASE WHEN ih.inv_hdr_stat_no = 9 THEN ii.amount ELSE 0 END) AS paid_stat_inv_amt  
				FROM {{ source('curated_erm', 'po_hdr_hist') }} phh  
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1  
					ON phh.po_no = phh1.po_no  
				JOIN {{ source('curated_erm', 'po_item_hist') }} pih  
					ON phh1.po_hdr_hist_no = pih.po_hdr_hist_no  
				JOIN {{ source('curated_erm', 'inv_item') }} ii  
					ON pih.po_item_no = ii.po_item_no  
				JOIN {{ source('curated_erm', 'inv_hdr') }} ih  
					ON ih.inv_hdr_no = ii.inv_hdr_no  
					AND phh.po_no = ih.po_no  
				WHERE  
					ih.inv_hdr_stat_no IN (7, 9)  
					AND ii.po_pay_plan_no IS NULL  
					AND phh.is_current = 1  
					AND phh1.is_current = 1  
					AND pih.is_current = 1  
					AND ii.is_current = 1  
					AND ih.is_current = 1  
					AND pih.ver = (  
						SELECT MAX(pih2.ver)  
						FROM {{ source('curated_erm', 'po_item_hist') }} pih2  
						WHERE pih2.po_item_no = pih.po_item_no  
						AND pih2.is_current = 1  
						AND pih2.ver <= phh.ver  
					)  
					AND phh.ver = (  
						SELECT MAX(hh2.ver)  
						FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2  
						WHERE hh2.po_no = phh.po_no  
						AND hh2.is_current = 1  
						AND NOT EXISTS (  
							SELECT 1  
							FROM {{ source('curated_erm', 'po_hdr') }} h3  
							WHERE h3.po_no = hh2.po_no  
							AND h3.ver = hh2.ver  
							AND h3.proc_int_stat_no <> 3  
							AND h3.is_current = 1  
						)  
					)  
				GROUP BY phh.proj_no, phh.po_no, phh.po_id
				) ii
		on 	h.po_no = ii.po_no	
where h.is_current = 1		
and 
hh.ver = (
    SELECT MAX(hh2.ver)
    FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2
    WHERE hh2.po_no = hh.po_no
      AND hh2.is_current = 1
      AND NOT EXISTS (
          SELECT 1
          FROM {{ source('curated_erm', 'po_hdr') }} h3
          WHERE h3.po_no = hh2.po_no
            AND h3.ver = hh2.ver
            AND h3.proc_int_stat_no <> 3
            AND h3.is_current = 1
      )
)

AND NOT EXISTS (
    SELECT 1
    FROM {{ source('curated_erm', 'proj') }} pj
    WHERE pj.proj_trx_po = h.po_no
      AND pj.is_current = 1
)

UNION ALL

select 
    h.po_no,
    h.po_id,
    hh.ver,
    p.proj_no,
    p.proj_id,
    CONCAT(p.proj_id, ' - ', p.descr) AS proj_descr,
    h.title,
    TRIM(CONCAT(c.name_1, ' ', c.name_2)) AS suppl_name,
    h.suppl_no,
    hhc.cust_po_no AS client_po_no,
    hh.issue_date AS issue_date,
    hh.deadline AS ros_date,
	cdd.contract_delivery_date,
    hh.approve_date,
    h.merc_hand_usr_id,
    origin_dtl.origin AS origin,
    origin_dtl.origin_desc AS origin_descr,
    hhc.origin AS origin_no,
    h.disc_percent,
    l.logo_file,
    h.cur_id,
    net_po_issd_price.net_po_cost AS net_po_cost,
    net_po_issd_price.net_po_cost AS net_other_po_cost,
    0 AS net_po_discount,
    net_po_issd_price.net_po_cost AS net_po_price,
	COALESCE(hhc.sub_project, hc.sub_project, 'none') AS sub_project,
    inc_dl.incoterm,
    inc_dl.delivery_location,    
    inc_dl.delivery_location1,
    ehc.po_close_date,
    r_tot.received_total - COALESCE(cr.credit_bal, 0) AS received_total,
    ppp.pp_inv_amt,
    ii.inv_amt AS inv_tot_against_commt,
    tf.paid_stat_taxf AS inv_tax_fright,
    CASE 
        WHEN net_po_issd_price.net_po_cost = 0 THEN NULL
        ELSE ROUND((COALESCE(ii.inv_amt, ppp.pp_inv_amt) / net_po_issd_price.net_po_cost * 100), 1)
    END AS pct_po_complete, 	
    9 AS inv_stat_flg,
    cr.debit_bal,
    CASE 
        WHEN COALESCE(hhc.sub_project, hc.sub_project, 'none') <> 'none' AND LENGTH(pstl.text) <> 0 THEN pstl.text
        WHEN hc.customer IS NOT NULL THEN hc.customer
        ELSE cont.name_1 
    END AS customer,		
    l.name,
    hc.bank_guarantee_expires AS bank_guarantee_expires_1,
    CASE 
        WHEN h.bank_guarantee_needed = 1 THEN 'yes' 
        ELSE 'no' 
    END AS bank_guarantee_expires_2,
    h.bank_guarantee_descr,
    h.bank_guarantee_date,
    h.bank_guarantee_usr_id,
    cp.name AS contact,
    cp.email AS cont_email,
    h.def_date,
    hc.rev_date,
    cvli.value AS po_type,
    h.execution_date		
	
	
FROM {{ source('curated_erm', 'po_hdr') }} h
INNER JOIN {{ source('curated_erm', 'proj') }} p 
    ON h.proj_no = p.proj_no 
    AND p.is_current = 1
INNER JOIN {{ source('curated_erm', 'cont') }} c 
    ON h.suppl_no = c.cont_no 
    AND c.is_current = 1
INNER JOIN {{ source('curated_erm', 'legal_entity') }} l 
    ON h.legal_entity_no = l.legal_entity_no 
    AND l.is_current = 1
INNER JOIN {{ source('curated_erm', 'po_hdr_hist') }} hh 
    ON h.po_no = hh.po_no  
    AND hh.is_current = 1
LEFT JOIN {{ source('curated_erm', 'po_hdr_cpv') }} hc 
    ON h.po_no = hc.po_no
    AND hc.is_current = 1
LEFT JOIN {{ source('curated_erm', 'po_hdr_hist_cpv') }} hhc 
    ON hh.po_hdr_hist_no = hhc.po_hdr_hist_no 
    AND hhc.is_current = 1
LEFT JOIN {{ source('curated_erm', 'expedite_hdr') }} eh 
    ON h.po_no = eh.po_no
    AND eh.is_current = 1
LEFT JOIN {{ source('curated_erm', 'expedite_hdr_cpv') }} ehc 
    ON eh.expedite_hdr_no = ehc.expedite_hdr_no 
    AND ehc.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proj_std_txt') }} pstx  
    ON REPLACE(COALESCE(hhc.sub_project, hc.sub_project, 'none'), '_C', '') = pstx.proj_std_txt_id 
    AND pstx.is_current = 1
LEFT JOIN {{ source('curated_erm', 'proj_std_txt_lang') }} pstl 
    ON pstx.proj_std_txt_no = pstl.proj_std_txt_no 
    AND pstl.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont') }} cont 
    ON p.client_no = cont.cont_no 
    AND cont.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cont_person') }} cp 
    ON h.suppl_ref = cp.name 
    AND h.suppl_no = cp.cont_no 
    AND cp.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli 
    ON hc.po_type = cvli.value_list_item_no
    AND cvli.is_current = 1
LEFT JOIN {{ ref('transformed_cust_report_get_cpv_dddw') }} origin_dtl 
    ON h.po_no = origin_dtl.po_no
LEFT JOIN {{ ref('transformed_cust_report_net_po_issd_price') }} net_po_issd_price 
    ON h.po_no = net_po_issd_price.po_no 
    AND hh.ver = net_po_issd_price.ver	

     left join 
              (
				SELECT 
					phh.po_no,
					phh.po_id,
					phh.ver,
					MIN(pih.agreed_deadline) AS contract_delivery_date
				FROM {{ source('curated_erm', 'po_item_hist') }} pih
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1 
					ON pih.po_hdr_hist_no = phh1.po_hdr_hist_no
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh 
					ON phh.po_no = phh1.po_no
				WHERE pih.is_current = 1
					AND phh.is_current = 1
					AND phh1.is_current = 1
					AND pih.ver = (
						SELECT MAX(pih1.ver)
						FROM {{ source('curated_erm', 'po_item_hist') }} pih1
						WHERE pih.po_item_no = pih1.po_item_no
						AND pih1.ver <= phh.ver
						AND pih1.is_current = 1
					)
					AND EXISTS (
						SELECT 1
						FROM (
							SELECT po_no, MAX(ver) AS ver
							FROM {{ source('curated_erm', 'po_hdr_hist') }} phh2
							WHERE phh2.is_current = 1
							GROUP BY phh2.po_no
						) AS latest_po
						WHERE latest_po.po_no = phh.po_no
						AND latest_po.ver = phh.ver
					)
				GROUP BY phh.po_no, phh.po_id, phh.ver) cdd
		on 	h.po_no = cdd.po_no
		
     left join 
              (
					SELECT 
						ph1.po_no,
						ARRAY_JOIN(COLLECT_LIST(COALESCE(dti.deliv_term_id, dt.deliv_term_id)), ', ') AS incoterm,
						ARRAY_JOIN(COLLECT_LIST(COALESCE(pi.deliv_place, ph1.deliv_place)), '; ') AS delivery_location,
						ARRAY_JOIN(COLLECT_LIST(COALESCE(pi.deliv_place, ph1.deliv_place)), '; ') AS delivery_location1
					FROM {{ source('curated_erm', 'po_hdr') }} ph1
					LEFT JOIN {{ source('curated_erm', 'po_item') }} pi 
						ON ph1.po_no = pi.po_no 
						AND pi.is_current = 1
					LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dti 
						ON pi.deliv_term_no = dti.deliv_term_no 
						AND dti.is_current = 1
					LEFT JOIN {{ source('curated_erm', 'deliv_term') }} dt 
						ON ph1.deliv_term_no = dt.deliv_term_no 
						AND dt.is_current = 1
					WHERE ph1.is_current = 1
					GROUP BY ph1.po_no
				) inc_dl
		on 	h.po_no = inc_dl.po_no			
		
     left join 
              (
			  SELECT  
					phh.proj_no,  
					phh.po_no,  
					phh.po_id,  
					SUM(ri.receive_unit_quan * pih.price) AS received_total  
				FROM {{ source('curated_erm', 'receipt_hdr') }} rh  
				JOIN {{ source('curated_erm', 'receipt_item') }} ri  
					ON rh.receipt_hdr_no = ri.receipt_hdr_no  
				JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh  
					ON rh.po_no = phh.po_no  
				JOIN {{ source('curated_erm', 'po_item_hist') }} pih  
					ON ri.po_item_no = pih.po_item_no  
				LEFT JOIN (  
					SELECT po_item_no, MAX(ver) AS max_ver  
					FROM {{ source('curated_erm', 'po_item_hist') }}  
					WHERE is_current = 1  
					GROUP BY po_item_no  
				) pih_max  
					ON pih.po_item_no = pih_max.po_item_no  
					AND pih.ver = pih_max.max_ver  
				LEFT JOIN (  
					SELECT po_no, MAX(ver) AS max_ver  
					FROM {{ source('curated_erm', 'po_hdr_hist') }}  
					WHERE is_current = 1  
					GROUP BY po_no  
				) phh_max  
					ON phh.po_no = phh_max.po_no  
					AND phh.ver = phh_max.max_ver  
				LEFT JOIN {{ source('curated_erm', 'po_hdr') }} ph  
					ON ph.po_no = phh.po_no  
					AND ph.ver = phh.ver  
					AND ph.proc_int_stat_no <> 3  
					AND ph.is_current = 1  
				WHERE rh.receipt_stat_no = 6  
				AND rh.is_current = 1  
				AND phh.is_current = 1  
				AND pih.is_current = 1  
				AND ri.is_current = 1  
				AND phh_max.max_ver IS NOT NULL  
				AND pih_max.max_ver IS NOT NULL  
				GROUP BY phh.proj_no, phh.po_no, phh.po_id
			) r_tot
		on 	h.po_no = r_tot.po_no			
 

		
     left join 		 
    (
	SELECT  
			phh.proj_no,  
			phh.po_no,  
			phh.po_id,  
			SUM(CASE WHEN pih.price < 0 THEN COALESCE(pih.quan, 0) * ABS(pih.price) ELSE 0 END) AS credit_bal,  
			SUM(CASE WHEN pih.price > 0 THEN COALESCE(pih.quan, 0) * ABS(pih.price) ELSE 0 END) AS debit_bal  
		FROM {{ source('curated_erm', 'po_hdr_hist') }} phh  
		JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1  
			ON phh.po_no = phh1.po_no  
		JOIN {{ source('curated_erm', 'po_item_hist') }} pih  
			ON phh1.po_hdr_hist_no = pih.po_hdr_hist_no  
		WHERE  
			pih.mat_grp_type_no = 3  
			AND phh.is_current = 1  
			AND phh1.is_current = 1  
			AND pih.is_current = 1  
			AND pih.ver = (  
				SELECT MAX(pih2.ver)  
				FROM {{ source('curated_erm', 'po_item_hist') }} pih2  
				WHERE pih2.po_item_no = pih.po_item_no  
				AND pih2.is_current = 1  
				AND pih2.ver <= phh.ver  
			)  
			AND phh.ver = (  
				SELECT MAX(hh2.ver)  
				FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2  
				WHERE hh2.po_no = phh.po_no  
				AND hh2.is_current = 1  
				AND NOT EXISTS (  
					SELECT 1  
					FROM {{ source('curated_erm', 'po_hdr') }} h3  
					WHERE h3.po_no = hh2.po_no  
					AND h3.ver = hh2.ver  
					AND h3.proc_int_stat_no <> 3  
					AND h3.is_current = 1  
				)  
			)  
		GROUP BY phh.proj_no, phh.po_no, phh.po_id
    ) cr
	on h.po_no = cr.po_no			
	
  
     left join 
        (
			SELECT 
				ii.po_no,
				SUM(ii.amount) AS pp_inv_amt
			FROM {{ source('curated_erm', 'po_hdr') }} ph
			JOIN {{ source('curated_erm', 'inv_hdr') }} ih 
				ON ph.po_no = ih.po_no
				AND ih.inv_hdr_stat_no = 9
				AND ih.is_current = 1
			JOIN {{ source('curated_erm', 'inv_item') }} ii 
				ON ih.inv_hdr_no = ii.inv_hdr_no
				AND ii.po_pay_plan_no IS NOT NULL
				AND ii.is_current = 1
			JOIN {{ source('curated_erm', 'po_pay_plan') }} ppp 
				ON ph.po_no = ppp.po_no
				AND ppp.po_pay_plan_no = ii.po_pay_plan_no
				AND ppp.is_current = 1
			WHERE ph.is_current = 1
			GROUP BY ii.po_no
				) ppp
		on 	h.po_no = ppp.po_no


				
left join 				
			(
			SELECT 
				phh.proj_no,
				phh.po_no,
				phh.po_id,
				SUM(ii.amount) AS inv_amt
			FROM {{ source('curated_erm', 'po_hdr_hist') }} phh
			JOIN {{ source('curated_erm', 'po_hdr_hist') }} phh1 
				ON phh.po_no = phh1.po_no
				AND phh1.is_current = 1
			JOIN {{ source('curated_erm', 'po_item_hist') }} pih 
				ON phh1.po_hdr_hist_no = pih.po_hdr_hist_no
				AND pih.is_current = 1
			JOIN {{ source('curated_erm', 'inv_hdr') }} ih 
				ON phh.po_no = ih.po_no
				AND ih.inv_hdr_stat_no = 9
				AND ih.is_current = 1
			JOIN {{ source('curated_erm', 'inv_item') }} ii 
				ON ih.inv_hdr_no = ii.inv_hdr_no
				AND ii.po_item_no = pih.po_item_no
				AND ii.po_pay_plan_no IS NULL
				AND ii.is_current = 1
			WHERE phh.is_current = 1
			AND pih.ver = (
				SELECT MAX(pih2.ver)
				FROM {{ source('curated_erm', 'po_item_hist') }} pih2
				WHERE pih2.po_item_no = pih.po_item_no
				AND pih2.ver <= phh.ver
				AND pih2.is_current = 1
			)
			AND phh.ver = (
				SELECT MAX(hh2.ver)
				FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2
				WHERE hh2.po_no = phh.po_no
				AND hh2.is_current = 1
				AND NOT EXISTS (
					SELECT 1
					FROM {{ source('curated_erm', 'po_hdr') }} h3
					WHERE h3.po_no = hh2.po_no
					AND h3.ver = hh2.ver
					AND h3.proc_int_stat_no <> 3
					AND h3.is_current = 1
				)
			)
			GROUP BY phh.proj_no, phh.po_no, phh.po_id ) ii	
		on 	h.po_no = ii.po_no		
	
     left join 
			(
				SELECT 
					proj_no,
					po_no,
					po_id,
					SUM(inv_amt) AS tot_tax_freight,
					SUM(trans_accpt_taxf) AS trans_accpt_taxf,
					SUM(paid_stat_taxf) AS paid_stat_taxf
				FROM (
					SELECT 
						ph.proj_no,
						ph.po_no,
						ph.po_id,
						SUM(ii.amount) AS inv_amt,
						SUM(CASE WHEN ih.inv_hdr_stat_no = 7 THEN ii.amount ELSE 0 END) AS trans_accpt_taxf,
						SUM(CASE WHEN ih.inv_hdr_stat_no = 9 THEN ii.amount ELSE 0 END) AS paid_stat_taxf
					FROM {{ source('curated_erm', 'inv_item') }} ii
					JOIN {{ source('curated_erm', 'inv_hdr') }} ih ON ih.inv_hdr_no = ii.inv_hdr_no AND ih.is_current = 1
					JOIN {{ source('curated_erm', 'po_hdr') }} ph ON ph.po_no = ih.po_no AND ph.is_current = 1
					WHERE (LOWER(ii.descr) = 'freight' OR ii.po_item_no IS NULL)
					AND ih.inv_hdr_stat_no IN (7, 9)
					AND ii.po_pay_plan_no IS NULL
					AND ii.is_current = 1
					GROUP BY ph.proj_no, ph.po_no, ph.po_id, ih.inv_hdr_stat_no
				) subquery
				GROUP BY proj_no, po_no, po_id
							) tf
		on 	h.po_no = tf.po_no		
		
 
where h.is_current = 1		
and 
hh.ver = (
    SELECT MAX(hh2.ver)
    FROM {{ source('curated_erm', 'po_hdr_hist') }} hh2
    WHERE hh2.po_no = hh.po_no
      AND hh2.is_current = 1
      AND NOT EXISTS (
          SELECT 1
          FROM {{ source('curated_erm', 'po_hdr') }} h3
          WHERE h3.po_no = hh2.po_no
            AND h3.ver = hh2.ver
            AND h3.proc_int_stat_no <> 3
            AND h3.is_current = 1
      )
)

AND NOT EXISTS (
    SELECT 1
    FROM {{ source('curated_erm', 'proj') }} pj
    WHERE pj.proj_trx_po = h.po_no
      AND pj.is_current = 1
)	

)