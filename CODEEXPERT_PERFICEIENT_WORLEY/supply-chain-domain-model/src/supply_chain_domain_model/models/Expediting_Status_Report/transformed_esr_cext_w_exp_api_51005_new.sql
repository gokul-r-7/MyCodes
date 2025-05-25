{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_esr_cext_w_exp_api_51005_new/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select 
	   po_no,
       expedite_hdr_no,
       exh_po_no,
       expedite_item_no,
       expedite_occurrence,
       supplementary_item_occurrence,
       early_prom_contract_date,
       early_forecast_del_date,
       early_ros,
       early_forecast_arri_on_site,
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
	    CASE 
        WHEN use_po_qty IS NULL THEN outstanding_qty
        WHEN use_po_qty = 0 THEN outstanding_qty
        ELSE po_outstanding_qty
		END AS outstanding_qty,
       exp_hdr_comments,
       proj_client_logo,
       logo_file,
       released_flg,
       received_flg,
       long_descr,
       l_comments,
       po_max_issue_ver,
       po_max_issue_date,
       po_first_issue_date,
       qty_chk,
       nxt_contact_date,
       size1,
       size2,
       size3,          
       size4,          
       comnt,
       ' ' hc_comnt,
       sub_project,
       srn_no,
       srn_qty,
       mmt_approved_date,
       mrr_no,
       mrr_date,
       mrr_qty,
       req_no,
       gen_mto_cwp,
       gen_att,
       delivery_type,
       proj_no,
	   etl_load_date,
       {{run_date}} as model_created_date,
       {{run_date}} as model_updated_date,
       {{ generate_load_id(model) }} as model_load_id
from (select mv.po_no,
          mv.expedite_hdr_no,
          mv.exh_po_no,
          mv.expedite_item_no,
          mv.expedite_occurrence,
          mv.supplementary_item_occurrence,
          mv.early_prom_contract_date,
          mv.early_forecast_del_date,
          mv.early_ros,
          mv.early_forecast_arri_on_site,
          mv.expli_po_item_no,
          mv.expli_mat_no,
          mv.poli_po_item_no,
          mv.poli_po_no,
          mv.project,
          mv.project_title,
          mv.purchase_order_no,
          mv.cust_po_no,
          mv.revision,
          mv.latest_client_revision,
          mv.issue_date,
          mv.line_item_count,
          mv.purchase_order_descr,
          mv.suppl_no,
          mv.supplier,
          mv.buyer_name,
          mv.expeditor,
          mv.inspector,
          mv.last_contact_date,
          mv.next_contact_date,
          mv.first_forecast_date,
          mv.last_forecast_date,
          mv.floats_earlros_firstforecast,
          mv.earliest_ros_date,
          mv.last_ros_date,
          mv.floats_lastros_lastforecast,
          mv.origin,
          mv.order_pos_sub,
          mv.shipment_pos_sub,
          mv.order_qty,
          mv.unit_id,
          mv.commopodity_code_tag_number,
          mv.tag_number,
          mv.s_item_no,
          mv.item_description,
          mv.mat_grp,
          mv.incoterm_delivery_place,
          mv.delivery_place,
          mv.site_id,
          mv.delivery_designation,
          mv.prom_contract_date,
          mv.forecast_delivery_date,
          mv.delays,
          mv.first_shipping_actual,
          mv.ros,
          mv.forecast_arrival_on_site,
          mv.floats,
          mv.arrival_on_site_actual,
          mv.receipt_status,
          mv.expedite_quan,
          mv.total_received_to_date,
          mv.outstanding_qty,
          mv.po_outstanding_qty,
          mv.exp_hdr_comments,
          mv.proj_client_logo,
          mv.logo_file,
          mv.released_flg,
          mv.received_flg,
          mv.long_descr,
          mv.l_comments,
          mv.po_max_issue_ver,
          mv.po_max_issue_date,
          mv.po_first_issue_date,
          mv.qty_chk,
          mv.nxt_contact_date,          
	      std_mat_scheme.size_1_value size1,
          std_mat_scheme.size_2_value size2,
          std_mat_scheme.size_3_value size3,
          std_mat_scheme.size_4_value size4,          
          hc.comnt comnt, 
          mv.sub_project,
          pcpv.use_po_qty,
          mv.srn_no,
          mv.srn_qty,
          mv.mmt_approved_date,
          mv.mrr_no,
          mv.mrr_date,
          mrr_qty,
          mv.req_no,
          gcwp.cwp gen_mto_cwp,
          mv.gen_att,
          mv.delivery_type,
          mv.proj_no,
		  cast(mv.etl_load_date as date) as etl_load_date

		  
		FROM {{ ref('transformed_esr_cext_w_exp_api') }} mv 


			LEFT JOIN {{ source('curated_erm', 'proj') }} proj 
				ON mv.project = proj.proj_id  AND proj.is_current = 1 

			LEFT JOIN {{ source('curated_erm', 'mat') }} mat  
				ON mv.expli_mat_no = mat.mat_no	AND mat.is_current = 1

			LEFT JOIN {{ source('curated_erm', 'std_mat') }} sm  
				ON mat.mat_no = sm.std_mat_no AND sm.is_current = 1

			LEFT JOIN {{ source('curated_erm', 'scheme') }} scheme  
				ON proj.scheme_no = scheme.scheme_no AND scheme.is_current = 1

			LEFT JOIN {{ source('curated_erm', 'std_mat_scheme') }} std_mat_scheme  
				ON proj.scheme_no = std_mat_scheme.scheme_no 
				AND sm.std_mat_no = std_mat_scheme.std_mat_no
				AND std_mat_scheme.is_current = 1

			LEFT JOIN {{ source('curated_erm', 'proj_cpv') }} pcpv  
				ON proj.proj_no = pcpv.proj_no  AND pcpv.is_current = 1

			LEFT JOIN {{ ref('transformed_esr_cext_wp_exp_hdr_comments_new') }} hc  
				ON mv.expedite_hdr_no = hc.expedite_hdr_no 

			LEFT JOIN {{ ref('transformed_esr_cext_w_exp_cwp_view_dhub') }} gcwp  
				ON mat.mat_no = gcwp.proj_comp_no
				AND proj.proj_no = gcwp.proj_no

          )
          
		  