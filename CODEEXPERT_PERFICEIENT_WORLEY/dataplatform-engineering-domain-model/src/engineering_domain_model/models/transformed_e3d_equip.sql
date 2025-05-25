{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_equip/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    E.acclass,
    E.acdivision,
    E.acgroup,
    E.area,
    E.areacode,
    E.areano,
    E.asd_autovol,
    E.asd_group,
    E.buil,
    '' as cablename,  --E.cablename,        -- Missing Columns
    '' as cablesourceloc,--E.cablesourceloc,     -- Missing Columns
    '' as cablesourcename,--E.cablesourcename,     -- Missing Columns
    '' as cabletrayname,--E.cabletrayname,     -- Missing Columns
    E.cgflowmed,
    E.cgvolume,
    E.checked,
    E.chkby,
    E.chkdate,
    E.commcode,
    E.comment,
    E.comment0,
    E.ctllocation,
    E.cwarea,
    '' as cwp, --E.cwp,     -- Missing Columns
    E.datumlocation,
    E.dcgpos,
    E.desc,
    E.designpress,
    E.designtemp,
    E.desp,
    E.discipline,
    E.dsco,
    E.dwgno,
    E.dwgnorev,
    E.esptable,
    E.esptag,
    E.ewp,
    E.fireproofing,
    '' as firezone,--E.firezone,     -- Missing Columns
    E.fluidtype,
    E.fstat,
    E.fstatus,
    E.func,
    E.furntype,
    '' as guid,--E.guid,     -- Missing Columns
    E.hold,
    E.holdcomment,
    E.holdflag,
    E.inprtr,
    E.insc,
    E.insulation,
    '' as insutype,--E.insutype,     -- Missing Columns
    E.ispe,
    E.issued,
    E.issuedby,
    E.issueddate,
    E.itemno,
    E.ithk,
    E.lock,
    E.locset,
    E.majorequip,
    '' as material,--E.material,     -- Missing Columns
    E.mdsysf,
    E.mecheck,
    E.modulefabricator,
    E.mps_comment,
    E.mps_date_piping_placeholder_modeling_completed,
    E.mps_date_piping_stress_loads_to_stru,
    E.mps_date_structural_engineering_actual,
    E.mps_date_structural_engineering_complete,
    E.mps_date_structural_engineering_forecast,
    E.mps_date_structural_modeling_complete,
    E.mps_foundation_req,
    E.mps_isometric_number,
    E.mps_line_number,
    E.mps_structural_engineering_released_to_piping,
    E.NAME,
    E.numb,
    E.ocgpos,
    E.ori,
    E.ouprtr,
    E.owner,
    E.pidref,
    E.pidrev,
    E.pidstat,
    E.piecemark,
    E.pmg_design_ae,
    E.pmgdesignai,
    E.ponumber,
    E.pos,
    E.progress,
    E.propst,
    E.ptsp,
    E.purp,
    E.requisition,
    E.rocstatus,
    E.section_name,
    E.serialno,
    E.site,
    E.skey,
    E.spre,
    E.status,
    E.steeltype,
    E.stlr,
    E.strucate,
    E.structural_mps_number,
    E.strutype,
    CASE
        WHEN E.stvval_wor_status_mech_equi NOT LIKE 'ME%' THEN ''
        ELSE E.stvval_wor_status_mech_equi
    END AS stvval,
    '' as subsystem,--E.subsystem,     -- Missing Columns
    E.tagno,
    E.tcgpos,
    E.track_id,
    E.transmittalno,
    E.type,
    '' as type1,--E.type1,     -- Missing Columns
    E.units,
    E.uom,
    E.usrcog,
    E.usrwco,
    E.usrwei,
    E.usrwwe,
    E.uwmtxt,
    E.venddrwgdate,
    E.venddrwgno,
    E.venddrwgrev,
    E.vendname,
    E.wcgcomnt,
    E.wcgdte,
    E.wcgloc,
    E.wcgstat,
    E.workpackno,
    E.worktype,
    E.wtbare_lb,
    E.wtoper_lb,
    E.zone,
    CAST(S.cumpercent AS decimal(38, 0)) AS PROGRESS_PERCENT,
    ( CASE
        WHEN E.stvval_wor_status_mech_equi NOT LIKE 'ME%' THEN ' No Status'
        WHEN E.stvval_wor_status_mech_equi IS NULL THEN ' No Status'
        ELSE E.stvval_wor_status_mech_equi
    END ) AS STATUS_LEVEL,
    Split_part(E.site, '-', 2) AS WBS,-- change
    coalesce(( Substring(E.zone, 2, 4) ), '') --CWPZone
    || ' '
    || coalesce(E.execution_date, '')
    || ' '
    || coalesce(( Substring(E.zone, 2, 2) ), '') --CWA
    || ' '
    || coalesce(E.modulefabricator, '')
    || ' '
    || coalesce(S.cumpercent, ' ')
    || ' '
    || ' '
    || coalesce(( Split_part(E.site, '-', 2) ), '') --WBS
    || ' '
    || coalesce(E.NAME, '')
    || ' '
    || coalesce(E.zone, '')--ZONE
    || ' '
    || coalesce(E.progress, '') 
    || coalesce(E.rocstatus, '')
    || 	CASE
	WHEN E.NAME LIKE '%-%-%-%' THEN
    REGEXP_EXTRACT(E.name, '^[^-]*-[^-]*-([^-]*)', 1)
	WHEN E.NAME LIKE '%-%-%' THEN null
	WHEN E.NAME LIKE '%-%' THEN null
	END  AS Search_Key,
    Substring(E.zone, 2, 2) AS CWA,
    Substring(E.zone, 2, 4) AS CWPZONE,
    E.zone AS CWP1,
    'VGCP2' project_code,
	'VGCP2'|| '_' ||
    Split_part(E.site, '-', 2)  || '_' ||
    Substring(E.zone, 2, 2)|| '_' ||
     Substring(E.zone, 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE,	
	CASE
	WHEN E.NAME LIKE '%-%-%-%' THEN
    REGEXP_EXTRACT(E.name, '^[^-]*-[^-]*-([^-]*)', 1)
	WHEN E.NAME LIKE '%-%-%' THEN null
	WHEN E.NAME LIKE '%-%' THEN null
	END AS ME_FUNC_CODE,
	cast(E.execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
    {{ source('curated_e3d', 'curated_vg_e3d_vglequip_sheet') }} E
    LEFT OUTER JOIN
    {{ source('curated_e3d', 'curated_dim_memstatus') }} S
    ON E.type || RIGHT(E.stvval_wor_status_mech_equi, 1) = S.disc || S.status

	
