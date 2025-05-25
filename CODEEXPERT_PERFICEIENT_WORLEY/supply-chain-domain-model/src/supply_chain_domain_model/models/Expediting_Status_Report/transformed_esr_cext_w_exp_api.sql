{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_w_exp_api/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}




SELECT 
po_no,
expedite_hdr_no,
exh_po_no,
expedite_item_no,
expedite_item_id as expedite_occurrence,
supplementary_item_occurrence,
expli_po_item_no,
expli_mat_no,
poli_po_item_no,
poli_po_no,
project,
project_title,
purchase_order_no,
cust_po_no,
revision,
latest_client_revision,
issue_date,
line_item_count,
purchase_order_descr,
suppl_no,
supplier,
buyer_name,
expeditor,
inspector,
last_contact_date,
next_contact_date,
first_forecast_date,
last_forecast_date,
floats_earlros_firstforecast,
earliest_ros_date,
last_ros_date,
floats_lastros_lastforecast,
origin,
order_pos_sub,
shipment_pos_sub,
order_qty,
unit_id,
commopodity_code_tag_number,   
tag_number,
s_item_no,
item_description,
mat_grp,
incoterm_delivery_place,
delivery_place,
site_id,
delivery_designation,
prom_contract_date,
forecast_delivery_date,
delays,
first_shipping_actual,
ros,
forecast_arrival_on_site,
floats,
arrival_on_site_actual,
receipt_status,
expedite_quan,
total_received_to_date,
outstanding_qty,
po_outstanding_qty,
exp_hdr_comments,
proj_client_logo,
logo_file,
po_first_issue_date,
header_comment as header_coment,
sub_project,
mmt_hdr_id as srn_no,
srn_qty,
mmt_approved_date,
mrr_no,
mrr_date,
mrr_qty,
req_no,
gen_att,
delivery_type,
proj_no,
long_descr,
l_comments,
ver as po_max_issue_ver,
etl_load_date,
{{run_date}} as model_created_date,
{{run_date}} as model_updated_date,
{{ generate_load_id(model) }} as model_load_id,


case 
    when mmt_hdr_no is null then null 
    else 0 
end as released_flg,

case 
    when total_received_to_date = order_qty then 0 
    else null 
end as received_flg,

CASE 
    WHEN ORDER_QTY = 0 THEN 'No' 
    ELSE 'Yes' 
END AS qty_chk ,
DATE_FORMAT(next_contact_date, 'dd-MMM-yyyy')  as nxt_contact_date,
CONCAT(
    DATE_FORMAT(po_first_issue_date, 'dd-MMM-yyyy'),
    ' / ',
    DATE_FORMAT(issue_date, 'dd-MMM-yyyy')
) AS po_max_issue_date,

MIN(prom_contract_date) OVER (
    PARTITION BY purchase_order_no 
    ORDER BY prom_contract_date
) AS early_prom_contract_date,

MIN(forecast_delivery_date) OVER (
    PARTITION BY purchase_order_no
) AS early_forecast_del_date,

MIN(ros) OVER (
    PARTITION BY purchase_order_no
) AS early_ros,

MIN(forecast_arrival_on_site) OVER (
    PARTITION BY purchase_order_no
) AS early_forecast_arri_on_site


from
(with vermaster as 
	(SELECT 
				po_no,
				MAX(ver) AS ver,
				ver_cnt,
				proc_int_stat_no
			FROM (
				SELECT 
					po_no,
					ver,
					COUNT(ver) OVER (PARTITION BY po_no) AS ver_cnt,
					CASE 
						WHEN COUNT(ver) OVER (PARTITION BY po_no) = 1 THEN 3
						ELSE proc_int_stat_no
					END AS proc_int_stat_no
				FROM (
					SELECT 
						exh.po_no, 
						po.ver, 
						po.proc_int_stat_no
					FROM {{ source('curated_erm', 'expedite_hdr') }} exh
					JOIN {{ source('curated_erm', 'po_hdr') }} po 
						ON exh.po_no = po.po_no
					WHERE po.proc_int_stat_no = 3
					AND po.is_current = 1
					AND exh.is_current = 1

					UNION ALL

					SELECT 
						exh.po_no, 
						po.ver, 
						po.proc_int_stat_no
					FROM {{ source('curated_erm', 'expedite_hdr') }} exh
					JOIN {{ source('curated_erm', 'po_hdr') }} po 
						ON exh.po_no = po.po_no
					WHERE po.proc_int_stat_no != 3
                    AND po.is_current = 1
					AND exh.is_current = 1
						AND NOT EXISTS (
							SELECT 1 
							FROM {{ source('curated_erm', 'po_hdr_hist') }} phh  
							WHERE phh.po_no = po.po_no 
							AND phh.ver = po.ver
                            AND phh.is_current = 1
						)

					UNION ALL

					SELECT 
						exh.po_no, 
						po.ver, 
						3 AS proc_int_stat_no
					FROM {{ source('curated_erm', 'expedite_hdr') }} exh
					JOIN {{ source('curated_erm', 'po_hdr_hist') }} po 
						ON exh.po_no = po.po_no
                    AND po.is_current = 1
					AND exh.is_current = 1						
					WHERE NOT EXISTS (
						SELECT 1 
						FROM {{ source('curated_erm', 'po_hdr') }} phh  
						WHERE phh.po_no = po.po_no 
						AND phh.ver = po.ver
                        AND phh.is_current = 1						
        )
    )
) 
			WHERE proc_int_stat_no = 3
			GROUP BY po_no, ver_cnt, proc_int_stat_no
	)
				  
,mmtmaster as (
				SELECT 
					mmth.po_no,
					mmth.mmt_hdr_no,
					ARRAY_JOIN(ARRAY_AGG(mmth.mmt_hdr_id), ',') AS mmt_hdr_id,
					mmth.proj_no,
					mmth.actual_ex_work_dispatch_date,
					mmth.forecast_ex_work_dispatch_date,
					expli.expedite_item_no,
					expli.expedite_hdr_no,
					SUM(expli.quan) AS srn_qty,
					ARRAY_JOIN(ARRAY_AGG(mmth.approve_date), ',') AS approved_date
				FROM 
					{{ source('curated_erm', 'mmt_hdr') }} mmth
				JOIN 
					{{ source('curated_erm', 'expedite_hdr') }} exph 
					ON mmth.po_no = exph.po_no
				JOIN 
					{{ source('curated_erm', 'expedite_item') }} expli 
					ON exph.expedite_hdr_no = expli.expedite_hdr_no 
					AND expli.mmt_hdr_no = mmth.mmt_hdr_no
                    AND mmth.is_current = 1
                    AND exph.is_current = 1
                    AND expli.is_current = 1
				GROUP BY 
					mmth.po_no,
					mmth.mmt_hdr_no,
					mmth.proj_no,
					mmth.actual_ex_work_dispatch_date,
					mmth.forecast_ex_work_dispatch_date,
					expli.expedite_item_no,
					expli.expedite_hdr_no
			   )
,receipt_master as
					 (SELECT 
						rech.receipt_hdr_no,
						rech.po_no,
						rech.receipt_hdr_id AS mrr_no,
						rech.receipt_stat_no,
						rech.mmt_hdr_no,
						rech.arrival_date,
						recli.receipt_item_no,
						recli.po_item_no,                        
						recli.receive_unit_quan AS mrr_qty,
						SUM(CAST(recli.receive_unit_quan AS DOUBLE)) OVER (PARTITION BY recli.po_item_no) AS receive_unit_quan,
						SUM(CAST(recli.receive_unit_quan AS DOUBLE)) OVER (PARTITION BY recli.po_item_no, recli.expedite_item_no) AS outs_qty,
						recli.unit_id,
						recli.stock_unit_quan,
						recli.mat_no,
						m.mat_id,
						recli.suppl_quan,
						recli.count_deliv_rem,
						recli.count_deliv_quan,
						recli.count_deliv_usr_id,
						recli.count_deliv_date,
						recli.qa_rem,
						recli.qa_accept_quan,
						recli.qa_non_conformance_quan,
						recli.qa_reject_quan,
						recli.qa_usr_id,
						recli.qa_date,
						recli.issue_item,
						recli.expedite_item_no,
						rech.upd_date AS mrr_date
					FROM 
						{{ source('curated_erm', 'receipt_hdr') }} rech
					JOIN 
						{{ source('curated_erm', 'receipt_item') }} recli
						ON rech.receipt_hdr_no = recli.receipt_hdr_no
					JOIN 
						{{ source('curated_erm', 'mat') }} m
						ON recli.mat_no = m.mat_no
					WHERE 
						recli.expedite_item_no IS NULL
					AND rech.receipt_stat_no = 6
                    AND rech.is_current = 1
                    AND recli.is_current = 1
                    AND m.is_current = 1						
					)
	
,receipt_master1 as 
			(SELECT 
				rech.receipt_hdr_no,
				rech.po_no,
				rech.receipt_hdr_id AS mrr_no,
				rech.receipt_stat_no,
				rech.mmt_hdr_no,
				rech.arrival_date,
				recli.receipt_item_no,
				recli.po_item_no,                         
				recli.receive_unit_quan AS mrr_qty,
				SUM(CAST(recli.receive_unit_quan AS DOUBLE)) OVER (PARTITION BY recli.po_item_no) AS receive_unit_quan,
				SUM(CAST(recli.receive_unit_quan AS DOUBLE)) OVER (PARTITION BY recli.po_item_no, recli.expedite_item_no) AS outs_qty,
				recli.unit_id,
				recli.stock_unit_quan,
				recli.mat_no,
				m.mat_id,
				recli.suppl_quan,
				recli.count_deliv_rem,
				recli.count_deliv_quan,
				recli.count_deliv_usr_id,
				recli.count_deliv_date,
				recli.qa_rem,
				recli.qa_accept_quan,
				recli.qa_non_conformance_quan,
				recli.qa_reject_quan,
				recli.qa_usr_id,
				recli.qa_date,
				recli.issue_item,
				recli.expedite_item_no,
				rech.upd_date AS mrr_date
			FROM 
				{{ source('curated_erm', 'receipt_hdr') }} rech
			JOIN 
				{{ source('curated_erm', 'receipt_item') }} recli
				ON rech.receipt_hdr_no = recli.receipt_hdr_no
			JOIN 
				{{ source('curated_erm', 'mat') }} m
				ON recli.mat_no = m.mat_no
			WHERE 
				recli.expedite_item_no IS NOT NULL
				AND rech.receipt_stat_no = 6
                    AND rech.is_current = 1
                    AND recli.is_current = 1
                    AND m.is_current = 1				
				)
,eil as
		(
			SELECT expedite_item_no,
				   CONCAT(def_date, ' ', def_usr_id, ' ', txt) AS l_comments
			FROM (
				SELECT expedite_item_no, def_date, def_usr_id, txt,
					   ROW_NUMBER() OVER (PARTITION BY expedite_item_no ORDER BY def_date DESC) AS rn
				FROM {{ source('curated_erm', 'expedite_item_log') }}
				WHERE txt NOT LIKE '**ARCH%' AND is_current = 1
			) sub
			WHERE rn = 1
		)

	,main_data as (
				SELECT 
					po_master.po_no,
					exh.expedite_hdr_no,
					exh.po_no AS exh_po_no,
					expli.expedite_item_no,
					expli.expedite_item_id,              
					sub1.batch_id AS supplementary_item_occurrence,		  
					expli.po_item_no AS expli_po_item_no,
					expli.mat_no AS expli_mat_no,
					poli.po_item_no AS poli_po_item_no,
					poli.po_no AS poli_po_no,
					po_master.project,
					po_master.project_title,
					po_master.purchase_order_no,
					pocpv.cust_po_no,                 
					po_master.pi_ver AS ver,
					' ' AS latest_client_revision,                 
					po_master.po_issue_date AS issue_date,
					po_master.po_first_issue_date,
					expdt.line_item_count,
					po_master.purchase_order_descr,
					po_master.suppl_no,
					po_master.supplier,
					COALESCE(po_master.buyer_id, '(empty)') AS buyer_name,
					COALESCE(exh.expedite_usr_id, '(empty)') AS expeditor,
					' ' AS inspector,                 
					exh.last_contact_date AS last_contact_date,
					exh.next_contact_date AS next_contact_date,
					expdt.first_forecast_date,
					expdt.last_forecast_date AS last_forecast_date,
					expdt.floats_earlros_firstforecast,
					expdt.earliest_ros_date AS earliest_ros_date,
					expdt.last_ros_date AS last_ros_date,
					expdt.floats_lastros_lastforecast,              
					COALESCE(cvli.value, '(empty)') AS origin,                 
					COALESCE(sub1.po_item_id, poli.po_item_id) AS order_pos_sub,
					' ' AS shipment_pos_sub,
					poli.quan AS order_qty,                 
					poli.proc_unit_id AS unit_id,
					CASE 
						WHEN po_master.mat_grp = 's' THEN std_mat.commodity_id 
						ELSE NULL 
					END AS commopodity_code_tag_number,                 
					CASE 
						WHEN po_master.mat_grp = 's' THEN NULL 
						ELSE COALESCE(po_master.tag_number, mat.mat_id) 
					END AS tag_number,
					CASE 
						WHEN po_master.mat_grp = 's' THEN mat.mat_id 
						ELSE NULL 
					END AS s_item_no,                 
					COALESCE(std_mat_scheme.descr, po_master.short_descr) AS item_description,
					COALESCE(std_mat_scheme.tech_txt, po_master.long_descr) AS long_descr,
					po_master.mat_grp,      
					COALESCE(dt.deliv_term_id, po_master.delivery_term) AS incoterm_delivery_place,
					COALESCE(expli.deliv_place, poli.deliv_place) AS delivery_place,
					COALESCE(es.site_id, po_master.site_id) AS site_id,
					COALESCE(es.site_id, COALESCE(po_master.site_id, '(empty)')) AS delivery_designation,                 
					poli.agreed_deadline AS prom_contract_date,
					expli.deliv_deadline AS forecast_delivery_date,
					DATE_DIFF(day, poli.agreed_deadline, expli.deliv_deadline) AS delays,                             
					expli.deadline AS ros,                 			
					expli.quan AS expedite_quan,                 
					exh.rem AS exp_hdr_comments,
					proj.client_logo AS proj_client_logo,
					le.logo_file,
					' ' AS po_max_issue_ver,
					' ' AS po_max_issued_date,
					' - ' AS header_comment,
					po_master.po_item_no,
					expli.ship_time,
					COALESCE(ehc.sub_project, 'none') AS sub_project,
					po_master.req_no,
					pccvp.gen_att,
					poli.delivery_type_no,
					po_master.proj_no,
					CAST(po_master.etl_load_date AS DATE) AS etl_load_date
				FROM {{ ref('transformed_esr_cext_w_exp_po_master_api') }} AS po_master
				LEFT OUTER JOIN {{ source('curated_erm', 'expedite_hdr') }} AS exh 
					ON po_master.po_no = exh.po_no AND exh.is_current = 1
				LEFT OUTER JOIN {{ ref('transformed_esr_cext_cr_po_50018_cpv') }} AS pocpv 
					ON po_master.po_no = pocpv.po_no 
					AND pocpv.cpv_type = 'pc'                 
				INNER JOIN {{ source('curated_erm', 'proj') }} AS proj 
					ON po_master.project = proj.proj_id AND proj.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'legal_entity') }} AS le 
					ON proj.legal_entity_no = le.legal_entity_no AND le.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'expedite_item') }} AS expli 
					ON exh.expedite_hdr_no = expli.expedite_hdr_no AND expli.is_current = 1
				LEFT OUTER JOIN {{ ref('transformed_esr_cext_exp_date_summary_api') }} AS expdt 
					ON expdt.po_no = exh.po_no
				INNER JOIN {{ source('curated_erm', 'po_item') }} AS poli 
					ON COALESCE(expli.po_item_no, po_master.po_item_no) = poli.po_item_no 
					AND exh.po_no = poli.po_no 
					AND po_master.po_item_no = poli.po_item_no
					AND poli.is_current = 1					
				LEFT OUTER JOIN {{ source('curated_erm', 'deliv_addr') }} AS eda 
					ON expli.deliv_addr_no = eda.deliv_addr_no  AND eda.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'site') }} AS es 
					ON eda.site_no = es.site_no  AND es.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'deliv_term') }} AS dt 
					ON expli.deliv_term_no = dt.deliv_term_no  AND dt.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'mat') }} AS mat 
					ON expli.mat_no = mat.mat_no  AND mat.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'std_mat') }} AS std_mat 
					ON mat.std_mat_no = std_mat.std_mat_no  AND std_mat.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'std_mat_scheme') }} AS std_mat_scheme 
					ON proj.scheme_no = std_mat_scheme.scheme_no 
					AND std_mat.std_mat_no = std_mat_scheme.std_mat_no
					 AND std_mat_scheme.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'part') }} AS part 
					ON std_mat.part_no = part.part_no  AND part.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'proj_comp') }} AS pc 
					ON mat.proj_comp_no = pc.proj_comp_no  AND pc.is_current = 1
				LEFT OUTER JOIN {{ source('curated_erm', 'proj_comp_cpv') }} AS pccvp 
					ON pc.proj_comp_no = pccvp.proj_comp_no  AND pccvp.is_current = 1
								 left outer join (SELECT 
													pi.po_item_no,
													sub.po_item_id,
													sub.batch_id
												FROM {{ source('curated_erm', 'po_item') }} AS pi
												LEFT JOIN (
													SELECT 
														si.batch_po_item_no,
														si.po_item_no,
														pi.po_item_id,
														si.batch_id
													FROM {{ source('curated_erm', 'po_item') }} AS pi
													JOIN {{ source('curated_erm', 'po_item') }} AS si
														ON pi.po_item_no = si.batch_po_item_no
												) AS sub 
												ON pi.po_item_no = sub.po_item_no) sub1 on poli.po_item_no=sub1.po_item_no
					left outer join  {{ source('curated_erm', 'expedite_hdr_cpv') }}  ehc on exh.expedite_hdr_no=ehc.expedite_hdr_no AND ehc.is_current = 1
					left outer join {{ source('curated_erm', 'cusp_value_list_item') }} cvli on ehc.origin=cvli.value_list_item_no  AND cvli.is_current = 1 	
                 ) 
select main_data.po_no,
       main_data.expedite_hdr_no,
       main_data.exh_po_no,
       main_data.expedite_item_no,
       main_data.expedite_item_id,              
       main_data.supplementary_item_occurrence,		  
       main_data.expli_po_item_no,
       main_data.expli_mat_no,
       main_data.poli_po_item_no,
       main_data.poli_po_no,
       main_data.project,
       main_data.project_title,
       main_data.purchase_order_no,
       main_data.cust_po_no,                 
       vermaster.ver revision ,
       main_data.ver,
       main_data.latest_client_revision,                 
       main_data.issue_date,
       main_data.po_first_issue_date,
       main_data.line_item_count,
       main_data.purchase_order_descr,
       main_data.suppl_no,
       main_data.supplier,
       main_data.buyer_name,
       main_data.expeditor,
       main_data.inspector,                 
       main_data.last_contact_date last_contact_date,
       main_data.next_contact_date next_contact_date,
       main_data.first_forecast_date,
       main_data.last_forecast_date last_forecast_date,
       main_data.floats_earlros_firstforecast,
       main_data.earliest_ros_date earliest_ros_date,
       main_data.last_ros_date last_ros_date,
       main_data.floats_lastros_lastforecast,
       main_data.origin,                 
	   main_data.order_pos_sub,
       main_data.shipment_pos_sub,
       main_data.order_qty,                 
       main_data.unit_id,
	   main_data.commopodity_code_tag_number,
       main_data.s_item_no,
       main_data.item_description,
       main_data.long_descr,
       main_data.mat_grp,                 
       main_data.incoterm_delivery_place, 
       main_data.delivery_place,
       main_data.site_id,
       main_data.delivery_designation,                 
       main_data.prom_contract_date,
       main_data.forecast_delivery_date,
       main_data.delays,                 
       mmtmaster.actual_ex_work_dispatch_date first_shipping_actual,
       main_data.ros,
       main_data.exp_hdr_comments,
       main_data.proj_client_logo,
       main_data.logo_file,
       mmtmaster.mmt_hdr_no,
       mmtmaster.mmt_hdr_id,
       mmtmaster.srn_qty,
       mmtmaster.approved_date mmt_approved_date,
       eil.l_comments,
       main_data.po_max_issue_ver,
       main_data.po_max_issued_date,
       main_data.header_comment,
       main_data.po_item_no,
       main_data.sub_project,
	   coalesce(receipt_master.mrr_no,receipt_master1.mrr_no) mrr_no,
	   coalesce(receipt_master.mrr_date,receipt_master1.mrr_date) mrr_date,
       coalesce(receipt_master.mrr_qty,receipt_master1.mrr_qty) mrr_qty,
	   main_data.req_no,
       main_data.gen_att,
       CASE 
         WHEN main_data.delivery_type_no = 0 THEN 'non-stock item'
         WHEN main_data.delivery_type_no = 1 THEN 'stock item'
          ELSE 'no delivery'
         END AS delivery_type,
       main_data.proj_no,
	   main_data.etl_load_date,
      CASE 
    WHEN mat_grp = 's' THEN NULL
    ELSE COALESCE(main_data.tag_number, receipt_master.mat_id, receipt_master1.mat_id)
END AS tag_number ,
CASE 
    WHEN MMTMASTER.MMT_HDR_NO IS NULL 
    THEN DATE_ADD(CAST(main_data.FORECAST_DELIVERY_DATE AS DATE), CAST(COALESCE(main_data.SHIP_TIME, 0) AS INTEGER))
    ELSE DATE_ADD(
            COALESCE(MMTMASTER.ACTUAL_EX_WORK_DISPATCH_DATE, MMTMASTER.FORECAST_EX_WORK_DISPATCH_DATE),
            CAST(COALESCE(main_data.SHIP_TIME, 0) AS INTEGER)
         )
END AS FORECAST_ARRIVAL_ON_SITE,

CASE 
    WHEN MMTMASTER.MMT_HDR_NO IS NULL 
    THEN DATEDIFF(
            DATE(main_data.ROS), 
           DATE_ADD(CAST(main_data.FORECAST_DELIVERY_DATE AS DATE), CAST(COALESCE(main_data.SHIP_TIME, 0) AS INTEGER))
         )
    ELSE 
    DATEDIFF(
            DATE(main_data.ROS), 
            DATE_ADD(CAST(COALESCE(MMTMASTER.ACTUAL_EX_WORK_DISPATCH_DATE, MMTMASTER.FORECAST_EX_WORK_DISPATCH_DATE) AS DATE), 
                     CAST(COALESCE(main_data.SHIP_TIME, 0) AS INTEGER))     
         )
END AS FLOATS,

COALESCE(receipt_master.arrival_date, receipt_master1.arrival_date) AS arrival_on_site_actual,

CASE 
    WHEN COALESCE(receipt_master.receipt_stat_no, receipt_master1.receipt_stat_no) = 6 
    THEN 'received' 
    ELSE '-' 
END AS receipt_status, 

main_data.expedite_quan,

COALESCE(receipt_master.receive_unit_quan, receipt_master1.receive_unit_quan) AS total_received_to_date,

(main_data.expedite_quan - COALESCE(receipt_master.receive_unit_quan, receipt_master1.receive_unit_quan, 0)) AS outstanding_qty,

(main_data.order_qty - COALESCE(receipt_master.receive_unit_quan, receipt_master1.receive_unit_quan, 0)) AS po_outstanding_qty

FROM main_data
LEFT JOIN  vermaster
    ON main_data.po_no = vermaster.po_no
LEFT JOIN  mmtmaster
    ON main_data.expedite_item_no = mmtmaster.expedite_item_no
    AND main_data.expedite_hdr_no = mmtmaster.expedite_hdr_no
LEFT JOIN  eil
    ON main_data.expedite_item_no = eil.expedite_item_no
LEFT JOIN receipt_master
    ON main_data.po_no = receipt_master.po_no
    AND main_data.po_item_no = receipt_master.po_item_no
LEFT JOIN receipt_master1
    ON main_data.po_no = receipt_master1.po_no
    AND main_data.expedite_item_no = receipt_master1.expedite_item_no 
)
