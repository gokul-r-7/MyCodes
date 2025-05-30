{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spi_drawing/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

select
    cast(dwg_id as decimal(38, 0)) as dwg_id,
    cast(proj_id as decimal(38, 0)) as proj_id,
    cast(site_id as decimal(38, 0)) as site_id,
    cast(chg_num as decimal(38, 0)) as chg_num,
    cast(plant_id as decimal(38, 0)) as plant_id,
    cast(unit_id as decimal(38, 0)) as unit_id,
    cast(user_name as varchar(300)) as user_name,
    cast(dwg_type_id as decimal(38, 0)) as dwg_type_id,
    cast(chg_status as varchar(10)) as chg_status,
    chg_date,
    cast(dwg_num as decimal(38, 0)) as dwg_num,
    cast(curr_rev_num as decimal(38, 0)) as curr_rev_num,
    cast(dwg_title1 as varchar(400)) as dwg_title1,
    cast(dwg_title2 as varchar(400)) as dwg_title2,
    cast(dwg_title3 as varchar(400)) as dwg_title3,
    cast(dwg_name as varchar(500)) as dwg_name,
    cast(ven_prop_by as varchar(200)) as ven_prop_by,
    ven_prop_date,
    cast(ven_dsgn_by as varchar(200)) as ven_dsgn_by,
    ven_dsgn_date,
    cast(ven_drwn_by as varchar(200)) as ven_drwn_by,
    ven_drwn_date,
    cast(ven_ck_by as varchar(200)) as ven_ck_by,
    ven_ck_date,
    cast(ven_appr_by as varchar(200)) as ven_appr_by,
    ven_appr_date,
    cast(cl_ck_by as varchar(200)) as cl_ck_by,
    cl_ck_date,
    cast(cl_engr_by as varchar(200)) as cl_engr_by,
    cl_engr_date,
    cast(cl_appr_by as varchar(200)) as cl_appr_by,
    cl_appr_date,
    cast(area_id as decimal(38, 0)) as area_id,
    cast(bmp_file_name as varchar(200)) as bmp_file_name,
    cast(bmp_file_path as varchar(100)) as bmp_file_path,
    cast(dwg_desc as varchar(100)) as dwg_desc,
    cast(output_dwg_fmt_id as decimal(38, 0)) as output_dwg_fmt_id,
    cast(parent_id as decimal(38, 0)) as parent_id,
    cast(ref_subj_desc as varchar(100)) as ref_subj_desc,
    cast(format_id as decimal(38, 0)) as format_id,
    cast(rev_id as decimal(38, 0)) as rev_id,
    cast(eng_proj_id as decimal(38, 0)) as eng_proj_id,
    cast(eng_ref_id as decimal(38, 0)) as eng_ref_id,
    cast(dwg_key1_id as decimal(38, 0)) as dwg_key1_id,
    cast(dwg_key2_id as decimal(38, 0)) as dwg_key2_id,
    cast(dwg_key3_id as decimal(38, 0)) as dwg_key3_id,
    cast(dwg_style_id as decimal(38, 0)) as dwg_style_id,
    cast(dwg_cond1 as varchar(2000)) as dwg_cond1,
    cast(dwg_cond2 as varchar(255)) as dwg_cond2,
    cast(report_type_id as decimal(38, 0)) as report_type_id,
    cast(merge_release_flg as varchar(10)) as merge_release_flg,
    cast(type_gen as varchar(10)) as type_gen,
    cast(document_type_id as decimal(38, 0)) as document_type_id,
    'VGCP2' as project_code,
	source_system_name,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from
    {{ source('curated_spi', 'curated_drawing') }}
	