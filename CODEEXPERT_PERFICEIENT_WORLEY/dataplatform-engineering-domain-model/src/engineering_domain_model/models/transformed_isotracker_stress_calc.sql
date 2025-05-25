{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_isotracker_stress_calc/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
   
    cast(calc_tracking as varchar(500)) as calc_tracking,
    cast(sr_no as varchar(500)) as sr_no,
    cast(item as varchar(500)) as item,
    cast(design_area as varchar(500)) as design_area,
    cast(area_for_tracking as varchar(500)) as area_for_tracking,
    cast(area_for_tracking_edited_by_hou as varchar(500)) as area_for_tracking_edited_by_hou,
    cast(line_id_ref_ldt as varchar(500)) as line_id_refldt,
    cast(pipe_spec_ref_ldt as varchar(500)) as pipe_spec_refldt,
    cast(size_ref_ldt as varchar(500)) as size_refldt,
    cast(flow_sheet_no__ref_ldt as varchar(500)) as flow_sheet_no_refldt,
    cast(designer as varchar(500)) as designer,
    cast(work_share_comments as varchar(500)) as work_share_comments,
    cast(stress_level__ref_ldt as varchar(500)) as stress_level_refldt,
    cast(stress_calc_no__ref_ldt as varchar(500)) as stress_calc_no_refldt,
    cast(module_cm as varchar(500)) as module_cm,
    cast(module_tm as varchar(500)) as module_tm,
    cast(module_bm as varchar(500)) as module_bm,
    cast(module_um as varchar(500)) as module_um,
    cast(module_lm as varchar(500)) as module_lm,
    cast(module_pm as varchar(500)) as module_pm,
    cast(module_sm as varchar(500)) as module_sm,
    cast(module_nm as varchar(500)) as module_nm,
    cast(
        isometric_from_design_date__yyyy_mm_dd as varchar(500)
    ) as pre_isometric_from_designdate_yyyymmdd,
    cast(issued_to_design_date__yyyy_mm_dd as varchar(500)) as pre_issued_to_designdate_yyyymmdd,
    cast(calc__in_analysis_y_n as varchar(500)) as pre_calc_in_analysis,
    cast(in_checking_y_n as varchar(500)) as pre_in_checking,
    cast(completed_y_n as varchar(500)) as pre_completed,
    cast(issued_to_ho_date__yyyy_mm_dd as varchar(500)) as pre_issued_to_ho_date_yyyymmdd,
    cast(received_from_ho_date__yyyy_mm_dd as varchar(500)) as pre_received_from_ho_date_yyyymmdd,
    cast(ho_comments_picked_up_y_n as varchar(500)) as pre_ho_comments_picked_up,
    cast(
        final_issued_to_ho_date__yyyy_mm_dd as varchar(500)
    ) as pre_final_issued_toho_date_yyyymmdd,
    cast(stress_analysis_engineer as varchar(500)) as pre_stress_analysis_engineer,
    cast(gid_checker as varchar(500)) as pre_gid_checker,
    cast(ho_checker as varchar(500)) as pre_ho_checker,
    cast(remarks as varchar(500)) as pre_remarks,
    cast(
        unique_line_id_y_blank_for_ref_only as varchar(500)
    ) as pre_unique_line_id_for_ref_only,
    cast(
        isometric_from_design_date__mm_dd_yyyy as varchar(500)
    ) as reva_isometric_from_design_date_mmddyyyy,
    cast(calc_in_ho_scope_started as varchar(500)) as reva_calc_in_ho_scope_started,
    cast(
        calc_in_analysis_in_gid_scope_started_y as varchar(500)
    ) as reva_calc_in_analysis_in_gid_scope_started,
    cast(issues_w_starting_calc__y as varchar(500)) as reva_issues_w_starting_calc,
    cast(analysis_started_but_now_on_hold_y as varchar(500)) as reva_analysis_started_but_now_on_hold,
    cast(
        lines_not_started_but_isometrics_available_y as varchar(500)
    ) as reva_lines_not_started_but_isometrics_available,
    cast(analysis_completed_yyyy_mm_dd as varchar(500)) as reva_analysis_completed_yyyymmdd,
    cast(in_checking_y as varchar(500)) as reva_in_checking,
    cast(checking_completed_yyyy_mm_dd as varchar(500)) as reva_checking_completed_yyyymmdd,
    cast(issued_to_ho_date__yyyy_mm_dd_1 as varchar(500)) as reva_issued_to_ho_date_yyyymmdd,
    cast(
        received_from_ho_date__yyyy_mm_dd_1 as varchar(500)
    ) as reva_received_from_ho_date_yyyymmdd,
    cast(ho_comments_picked_up_yyyy_mm_dd as varchar(500)) as reva_ho_comments_picked_up_yyyymmdd,
    cast(stress_analysis_engineer_1 as varchar(500)) as reva_stress_analysis_engineer,
    cast(gid_checker_1 as varchar(500)) as reva_gid_checker,
    cast(ho_checker_reviewer as varchar(500)) as reva_ho_checker_reviewer,
    cast(ready_for_client_review_level_1 as varchar(500)) as reva_ready_for_client_review_level_1,
    cast(issued_to_vg_date__yyyy_mm_dd as varchar(500)) as reva_issued_to_vg_date_yyyymmdd,
    cast(approved_from_vg_date__yyyy_mm_dd as varchar(500)) as reva_approved_from_vg_date_yyyymmdd,
    cast(
        final_issued_to_piping_design_date__yyyy_mm_dd as varchar(500)
    ) as reva_final_issued_topiping_design_date_yyyymmdd,
    cast(remarks_1 as varchar(500)) as reva_remarks,
    cast(
        ageing_for_calc_pending_with_ho_for_review as varchar(500)
    ) as reva_ageing_for_calc_pending_with_ho_for_review,
    cast(
        ageing_for_calc_pending_with_gid_for_comment_incorporation as varchar(500)
    ) as reva_ageing_for_calc_pending_with_gid_for_comment,
    cast(
        ageing_for_calc_pending_with_from_vg_approval_to_issue_to_piping_design as varchar(500)
    ) as reva_ageing_calc_pending_vg_approval_piping_design,
    cast(isometric_from_design_date as varchar(500)) as rev1_isometric_from_design_date,
    cast(issued_to_design_date__yy_mm_dd as varchar(500)) as rev1_issued_to_design_date_yymmdd,
    cast(calc__in_analysis_y_n_1 as varchar(500)) as rev1_calc_in_analysis,
    cast(in_checking_y_n_1 as varchar(500)) as rev1_in_checking,
    cast(completed_y_n_1 as varchar(500)) as rev1_completed,
    cast(issued_to_ho_date__yy_mm_dd as varchar(500)) as rev1_issued_to_ho_date_yymmdd,
    cast(received_from_ho_date__yy_mm_dd as varchar(500)) as rev1_received_from_ho_date_yymmdd,
    cast(final_issued_to_ho_date__yy_mm_dd as varchar(500)) as rev1_final_issued_toho_date_yymmdd,
    cast(issued_to_design_date as varchar(500)) as rev1_issued_to_design_date,
    cast(stress_analysis_engineer_2 as varchar(500)) as rev1_stress_analysis_engineer,
    cast(gid_checker_2 as varchar(500)) as rev1_gid_checker,
    cast(ho_checker_1 as varchar(500)) as rev1_ho_checker,
    cast(remarks_2 as varchar(500)) as rev1_remarks,
    cast(
        isometric_from_design_date__yy_mm_dd as varchar(500)
    ) as revb_isometric_from_design_date_yymmdd,
    cast(date_at_excel_cell_bv as varchar(500)) as date_at_excel_cell_bv,
    cast(vessel_number as varchar(500)) as vessel_number,
    cast(vessel_clip_y_n as varchar(500)) as vessel_clip,
    cast(no_of_vessel_clips as varchar(500)) as no_of_vessel_clips,
    cast(nozzle_number as varchar(500)) as nozzle_number,
    qualified_within_vendor_allowable_ks_me_spc_0203_standard_allowable_external_loads_on_equipment_nozzles_specification_api_standards_y_n  as qualified_within_vendor_allowable_standard,
    nozzle_transmittal_prepared_for_loads_exceeding_the_allowables_y_n as nozzle_transmittal_for_loads_exceed_the_allowables,
    cast(nozzle_transmittal_number as varchar(500)) as nozzle_transmittal_number,
    cast(loading_to_csa as varchar(500)) as loading_to_csa,
    cast(s.execution_date as date) as extracted_date,
	source_system_name,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(500)) as project_code,
    '' as line_number,
    cast(
        substring(replace(p.zone, '/', ''), 2, 2) as varchar(500)
    ) as cwa,
    cast(
        substring(replace(p.zone, '/', ''), 2, 4) as varchar(500)
    ) as cwz,
    coalesce(s.area_for_tracking, '') || ' ' || coalesce(substring(replace(p.zone, '/', ''), 2, 2), '') || ' ' || coalesce(substring(replace(p.zone, '/', ''), 2, 4), '') || ' ' || coalesce(s.line_id_ref_ldt, '') || ' ' || coalesce(s.completed_y_n, '') || ' ' || coalesce(s.isometric_from_design_date__yyyy_mm_dd, '') || ' ' || coalesce(s.issued_to_ho_date__yyyy_mm_dd, '') || ' ' || coalesce(s.received_from_ho_date__yyyy_mm_dd, '') || ' ' || coalesce(s.approved_from_vg_date__yyyy_mm_dd, '') || ' ' || coalesce(
        s.final_issued_to_piping_design_date__yyyy_mm_dd,
        ''
    ) || ' ' || coalesce(s.issued_to_vg_date__yyyy_mm_dd, '') as search_key
from
    {{ source('curated_isotracker', 'curated_cp2_stress_calc') }}  s
    left outer join (
        select
            distinct CASE
	                    WHEN pidlineno LIKE '%-%-%-%-%' THEN
                        REGEXP_EXTRACT(pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)-', 1)
	                    WHEN pidlineno LIKE '%-%-%-%' THEN
                        REGEXP_EXTRACT(pidlineno, '^[^-]*-([^-]*-[^-]*)', 1)
	                    WHEN pidlineno LIKE '%-%-%' THEN
                        REGEXP_EXTRACT(pidlineno, '^[^-]*-([^-]*)', 1)
	                    WHEN pidlineno LIKE '%-%' THEN null
            END as unique_line_id,
            execution_date,
            zone
        from
            {{ source('curated_e3d', 'curated_vg_e3d_vglpipes_sheet') }}
        where
            execution_date = (
                select
                    max(execution_date)
                from
                    {{ source('curated_e3d', 'curated_vg_e3d_vglpipes_sheet') }}
            )
    ) p on s.line_id_ref_ldt = p.unique_line_id
    and s.execution_date = p.execution_date           