{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_po_hdr_details_cext_w_exp_sch_api_nv/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



 select 
		ph.proj_no                                              as projet_no
       ,ph.proj_id                                              as project
       ,ph.sub_project                                          as sub_project
       ,nvl(th.tmr_id,ph.tmr_number)                            as tmr_number
       ,ph.po_no                                                as po_no
       ,ph.purchase_order_no                                    as purchase_order_no
       ,ph.latest_revision                                      as latest_revision
       ,ph.po_first_issue_date                                  as po_first_issue_date
       ,ph.issue_date                                           as issue_date
       ,ph.description                                          as description                     
       ,ph.cust_po_no                                           as customer_po_no
       ,ph.buyer                                                as buyer
       ,ph.supplier                                             as supplier
       ,ph.expediter                                            as expediter
       ,ph.last_contact_date                                    as last_contact_date
       ,ph.next_contact_date                                    as next_contact_date
       ,e_dates.earliest_contract_dlv_date                      as earliest_contract_dlv_date
       ,e_dates.earliest_forecast_dlv_date                      as earliest_forecast_dlv_date
       ,e_dates.earliest_ros                                    as earliest_ros
       ,e_dates.earliest_forecast_arrival_date                  as earliest_forecast_arrival_date
       ,e_dates.contractual_dlv_date                            as contractual_dlv_date  
       ,eh1.comnt                                               as expedite_comment
       ,ish.insp_sch_hdr_id                                     as template_no
       ,ins.insp_sch_hdr_tmpl_no                                as template       
       ,ish.name                                                as template_name
       ,decode(ish.stat,1,'open','closed')                      as status
       ,c.id                                                    as calendar
       ,insp_s.all_items                                        as total_line_items
       ,insp_s.released_items                                   as released_items
       ,tt.trans_term_id                                        as trans_terms
       ,ecpv.expediting_level                                   as expediting_level
       ,ecpv.inspection_level                                   as inspection_level
       ,ish.deadline                                            as ros_date
       ,ish.forecast_date                                       as forecast
	    ,CAST((insp_s.released_items / insp_s.all_items) * 100 as BIGINT) as release_percentage 
       ,decode(ish.insp_type,1,'expediting','inspection')       as insp_type
       ,nvl(insp_s.all_items,0)-nvl(insp_s.released_items,0)    as items_not_released
       ,ish.forecast_updated_at                                 as forecast_update_at
       ,ish.deadline_float                                      as ros_float
       ,ish.responsible_usr_id                                  as responsible
       ,dt.deliv_term_id                                        as incoterm
       ,ish.remark                                              as remark
       ,isi.calc_seq                                            as milestone_seq
       ,isi.descr                                               as milestones
       ,isi.baseline_date                                       as baseline
       ,isi.forecast_start                                      as forecast_start
       ,isi.forecast_complete                                   as forecast_complete
       ,isi.duration                                            as milestone_duration
       ,isi.actual_date                                         as actual_date
       ,isi.deadline_float                                      as deadline_float
       ,isi.duration_before                                     as duration_before_milestone       
       ,isi.baseline_manhours                                   as baseline_manhours
       ,isi.actual_manhours                                     as actual_manhours
       ,decode(isi.target,1,'header','item')                    as milestone_type
       ,ph.client_ref_no                                        as client_reference
	   ,cast(ins.execution_date as date) 						as etl_load_date
	   ,{{run_date}} 											as model_created_date
	   ,{{run_date}} 											as model_updated_date
	   ,{{ generate_load_id(model) }} 							as model_load_id	


       
 from {{ source('curated_erm', 'insp_sch') }}  ins
      join 
	  (
		  select 
		   proj.proj_no
		  ,proj.proj_id          
		  ,po_hdr.po_no
		  ,po_hdr.po_id purchase_order_no
		  ,po_hdr.ver latest_revision
		  ,po_hdr.po_first_issue_date
		  ,po_hdr.issue_date  
		  ,phc.sub_project
		  ,phc.cust_po_no
		  ,po_hdr.merc_hand_usr_id buyer
		  ,suppl.name_1 supplier
		  ,eh.expedite_usr_id expediter
		  ,po_hdr.title description
		  ,po_hdr.trans_term_no
		  ,po_hdr.deliv_term_no
		  ,po_hdr.tmr_id tmr_number
		  ,eh.last_contact_date
		  ,eh.next_contact_date
		  ,eh.expedite_hdr_no
          ,phc.client_ref_no
		  ,po_hdr.execution_date
		from {{ source('curated_erm', 'po_hdr') }} po_hdr
			 join {{ source('curated_erm', 'proj') }} proj on po_hdr.proj_no = proj.proj_no     
			 left join {{ source('curated_erm', 'po_hdr_cpv') }} phc on po_hdr.po_no = phc.po_no
			 left join {{ ref('transformed_erm_suppl') }} suppl on po_hdr.suppl_no = suppl.suppl_no
			 left join {{ source('curated_erm', 'expedite_hdr') }} eh on po_hdr.po_no = eh.po_no
		where po_hdr.proc_int_stat_no = 3
		and po_hdr.is_current  = 1
		and proj.is_current  = 1
		and phc.is_current  = 1
		and eh.is_current  = 1


		union all

		select proj.proj_no
			  ,proj.proj_id    
			  ,ph.po_no
			  ,ph.po_id purchase_order_no
			  ,ph.ver
			  ,ph.po_first_issue_date
			  ,ph.issue_date   
			  ,phc.sub_project
			  ,phc.cust_po_no
			  ,ph.merc_hand_usr_id buyer
			  ,suppl.name_1 supplier
			  ,eh.expedite_usr_id expediter
			  ,ph.title description
			  ,ph.trans_term_no
			  ,ph.deliv_term_no
			  ,ph.tmr_id tmr_number
			  ,eh.last_contact_date
			  ,eh.next_contact_date
			  ,eh.expedite_hdr_no
              ,phc.client_ref_no			  
              ,ph.execution_date
		from {{ source('curated_erm', 'po_hdr_hist') }}  ph
			 join {{ source('curated_erm', 'proj') }} proj on ph.proj_no = proj.proj_no and proj.is_current  = 1
			 left join {{ source('curated_erm', 'po_hdr_hist_cpv') }}  phc on ph.po_hdr_hist_no = phc.po_hdr_hist_no and phc.is_current  = 1
			 left join {{ ref('transformed_erm_suppl') }} suppl on ph.suppl_no = suppl.suppl_no
			 left join {{ source('curated_erm', 'expedite_hdr') }} eh on ph.po_no = eh.po_no and eh.is_current  = 1      
		where not exists (select 1
						  from {{ source('curated_erm', 'po_hdr') }} po_hdr 
						  where po_hdr.po_no = ph.po_no
						  and po_hdr.ver = ph.ver
						  and po_hdr.proc_int_stat_no = 3
						  and po_hdr.is_current  = 1)
		and not exists (select 1
					  from {{ source('curated_erm', 'po_hdr') }} po_hdr 
					  where po_hdr.po_no = ph.po_no
					  and   po_hdr.ver = ph.ver
					  and po_hdr.proc_int_stat_no != 3
					  and po_hdr.is_current  = 1)
		and exists (select 1
					from (select phh.po_no,max(phh.ver) ver
						  from {{ source('curated_erm', 'po_hdr_hist') }}   phh
						  group by phh.po_no) phh
					where ph.po_no = phh.po_no
					and   ph.ver = phh.ver)
        and ph.is_current  = 1			
	  ) ph 
			on ins.po_no = ph.po_no
			
      left join {{ source('curated_erm', 'expedite_hdr') }}   eh  on ph.po_no = eh.po_no 
	                                                                 and eh.is_current  = 1
      left join {{ source('curated_erm', 'insp_sch_hdr') }}   ish on ins.insp_sch_hdr_no = ish.insp_sch_hdr_no 
	                                                                 and ins.proj_no = ish.proj_no 
																	 and ish.is_current  = 1
      left join {{ source('curated_erm', 'insp_sch_item') }}   isi on ish.insp_sch_hdr_no = isi.insp_sch_hdr_no
	                                                                  and isi.is_current  = 1
      left join {{ ref('transformed_po_hdr_cext_w_exp_hdr_insp_status') }} insp_s on eh.expedite_hdr_no = insp_s.expedite_hdr_no
      left join {{ source('curated_erm', 'trans_term') }}   tt on ph.trans_term_no = tt.trans_term_no
	  	                                                          and tt.is_current  = 1
      left join {{ source('curated_erm', 'expedite_hdr_cpv') }}   ecpv on eh.expedite_hdr_no = ecpv.expedite_hdr_no 
	  	                                                                  and ecpv.is_current  = 1	  
      left join {{ source('curated_erm', 'deliv_term') }}   dt on ph.deliv_term_no = dt.deliv_term_no 
	  	  	                                                      and dt.is_current  = 1
      left join {{ source('curated_erm', 'calendar') }}   c on ish.calendar_no = c.calendar_no
	  	  	                                                   and c.is_current  = 1
      left join {{ source('curated_erm', 'tmr_hdr') }}   th on ins.tmr_no = th.tmr_no
	  	  	                                                   and th.is_current  = 1
      left join {{ ref('transformed_po_hdr_cext_w_exp_comts_52003_api') }} eh1 on eh.expedite_hdr_no = eh1.expedite_hdr_no
      left join (select pi.po_no
                        ,min(pi.agreed_deadline) earliest_contract_dlv_date
                        ,min(ei.deliv_deadline)  earliest_forecast_dlv_date
                        ,min(ei.deadline) earliest_ros,
						min(
								CASE 
									WHEN mh.mmt_hdr_no IS NULL THEN 
										TRUNC(DATE_ADD(ei.deliv_deadline, CAST(NVL(ei.ship_time, 0) AS INT)), 'DAY')
									ELSE 
										DATE_ADD(
											NVL(mh.actual_ex_work_dispatch_date, mh.forecast_ex_work_dispatch_date), 
											CAST(NVL(mh.ship_time, 0) AS INT)
										)
								END
							) AS Earliest_Forecast_Arrival_Date
						,max(pi.agreed_deadline) contractual_dlv_date
                  from {{ source('curated_erm', 'po_item') }}   pi 
                       join {{ source('curated_erm', 'po_hdr') }}   ph on pi.po_no = ph.po_no and ph.is_current  = 1
                       left join {{ source('curated_erm', 'expedite_item') }}   ei on pi.po_item_no = ei.po_item_no and ei.is_current  = 1
                       left join {{ source('curated_erm', 'mmt_hdr') }}   mh on ei.mmt_hdr_no = mh.mmt_hdr_no and mh.is_current  = 1
                 where pi.quan != 0
                 and pi.delivery_type_no = 1
				 and pi.is_current  = 1
                 group by pi.po_no) e_dates on ph.po_no = e_dates.po_no


