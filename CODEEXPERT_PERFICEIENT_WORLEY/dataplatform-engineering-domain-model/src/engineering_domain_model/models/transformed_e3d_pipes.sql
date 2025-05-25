{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_pipes/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    CASE
        WHEN P.NAME LIKE '/%' THEN Substring(P.NAME, 2, Length(P.NAME) - 1)
        ELSE P.NAME
    END AS NAME_WS,
    P.facilitycode,
    P.insulation,
    P.rocstatus,
    P.pipeplanref,
    P.piddesc,
    P.revdesc6,
    P.clientdocrev,
    P.revdata6,
    P.ouprtr,
    P.holdc1,
    P.wcgloc,
    P.fluidtype,
    P.duty,
    P.matlspec,
    P.processhold,
    P.flur,
    P.steeltype,
    P.mtorev,
    P.psisystem,
    P.tspe,
    P.secdel,
    P.holdcomment,
    P.pidlineno,
    P.holdc3,
    P.checked,
    P.nsec,
    P.lock,
    P.design_level,
    P.asd_lmed,
    P.processremark,
    P.pidstat,
    P.chkdate,
    P.secto,
    P.secno,
    P.ispe,
    P.section_name,
    P.secfr,
    P.revdesc9,
    P.pwht,
    P.revdata5,
    P.fluidservicecode,
    P.mtoarea,
    P.requisition,
    P.pmax,
    P.testpress,
    P.designtemp,
    P.designpress,
    P.site,
    P.splp,
    P.deldsg,
    P.systemno,
    P.approv,
    P.cgflowmed,
    P.wcgdte,
    P.material,
    P.frev,
    P.revdata7,
    P.ctllocation,
    P.wcgstat,
    P.below_ground,
    P.buil,
    P.pipsta,
    P.tagno,
    P.liss,
    P.casn,
    P.transmittalno,
    P.erec,
    P.smax,
    P.holdc2,
    P.fabspec,
    P.wpdocrev,
    P.bendmacreference,
    P.mocno,
    P.areacode,
    P.comment,
    P.apprby,
    P.tcgpos,
    P.progress,
    P.revdesc8,
    P.fdra,
    P.pidref,
    P.cleaningcode,
    P.htemp,
    P.track_id,
    P.asd_dpres,
    P.tpress_psi,
    P.revdesc1,
    P.insc,
    P.cgvolume,
    P.care,
    P.stressrelieve,
    P.pres_psi,
    P.holdc4,
    P.proj,
    P.radiography,
    P.frdr,
    P.doctype,
    P.posafc,
    P.mecheck,
    P.temp_degf,
    P.revdesc3,
    P.from as from_name,
    P.workpackno,
    P.dcgpos,
    P.issuedby,
    P.piecemark,
    P.func,
    '' as majortray,--P.majortray,     -- Missing Column
    P.revdata3,
    P.wldc,
    P.revdata8,
    P.paintsystem,
    P.pidrev,
    P.uom,
    P.cnum,
    P.clash,
    P.clientdocno,
    P.drrf,
    P.stresscritical,
    P.drawn,
    P.mtoiss,
    P.commcode,
    P.ocgpos,
    P.ccla,
    P.pedcat,
    P.holdc5,
    P.planu,
    P.cdrg,
    P.strucate,
    P.sdrlcode,
    P.matr,
    P.model,
    P.drawnby,
    P.casr,
    P.rlstor,
    P.cladate,
    P.isohidden,
    P.revdesc5,
    P.jmax,
    P.heatt,
    P.purp,
    P.asd_dtemp,
    P.revdata1,
    P.pspe,
    P.fireproofing,
    P.prmodule,
    P.psidate,
    P.NAME,
    P.units,
    P.subsystem,
    P.hold,
    P.revdesc2,
    P.itemno,
    P.revdesc7,
    P.clientlineno,
    P.disccode,
    P.type,
    P.ccen,
    P.xray,
    P.revdata9,
    P.dsco,
    P.discipline,
    P.skey,
    P.worktype,
    P.critical,
    P.systemcode,
    P.strutype,
    P.testmedium,
    P.fpline,
    P.sloref,
    P.zone,
    P.issued,
    P.chkby,
    '' as traycat,--P.traycat,     -- Missing Column
    P.dwgno,
    P.fstatus,
    P.areano,
    P.safc,
    P.ewp,
    P.jntc,
    P.serialno,
    CASE
        WHEN P.stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN ''
        ELSE P.stvval_wor_status_proc_pipe
    END AS STVVAL_WOR_STATUS_PROC_PIPE,
    P.revdesc4,
    P.fstat,
    '' as insutype,--P.insutype,     -- Missing Column
    '' as cwp,--P.cwp,     -- Missing Column
    P.owner,
    P.revdata2,
    P.modulefabricator,
    '' as guid,--P.guid,     -- Missing Column
    P.fabwgt,
    P.isssta,
    P.mtodrwg,
    P.suffix,
    P.mdsysf,
    P.unitno,
    P.issueddate,
    P.wcgcomnt,
    P.ndegroup,
    P.stresssysno,
    CASE
        WHEN P.bore_in LIKE '%in%' THEN Replace(P.bore_in, 'in', '')
        WHEN P.bore_in LIKE '%Unset%' THEN ''
        ELSE P.bore_in
    END AS BORE_IN,
    P.optemp,
    P.to as to_name,
    P.prdat,
    P._type,
    P.cadarea,
    P.procrem,
    P.issdat,
    P.casdesc,
    P.holdflag,
    P.wmax,
    P.oppress,
    P.farea,
    P.mtoref,
    P.wpdocno,
    P.ptsp,
    P.inprtr,
    P.status,
    P.cwarea,
    P.shop,
    P.revdata4,
    P.dwgnorev,
    P.rev,
    P.desc as description,
    P.lntp,
    P.ithk,
    P.pga,
    Split_part(coalesce(P.site, ''), '-', 2) AS WBS,
    Substring(Replace(P.zone, '/', ''), 2, 2) AS CWA,
    Substring(Replace(P.zone, '/', ''), 2, 4) AS CWPZONE,
    Replace(P.zone, '/', '') AS CWP1,
    CAST(S.cumpercent AS decimal(38, 0)) AS PROGRESS_PERCENT,		
    ( CASE
        WHEN P.stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN ' No Status'
        ELSE P.stvval_wor_status_proc_pipe
    END ) AS STATUS_LEVEL,
    Split_part(coalesce(P.site, ''), '-', 2)
    || ' '
    || coalesce(Substring(Replace(P.zone, '/', ''), 2, 2), '') --CWA  
    || ' '
    || coalesce(Substring(Replace(P.zone, '/', ''), 2, 4), '') --CWPZONE  
    || ' '
    || coalesce(P.pidref, '')
    || ' '
    --|| coalesce(P.unique_line_id, '')     -- Missing Column
    --|| ' '
    || coalesce(S.cumpercent, ' ')
    || ' '
    || coalesce( CASE
        WHEN P.stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN
        ' No Status'
        ELSE P.stvval_wor_status_proc_pipe
    END, ' ')
    || ' '
    || coalesce(Replace(P.zone, '/', ''), '') --CWP  
    || ' '
    || coalesce(P.NAME, '')
    || ' '
    || coalesce(P.execution_date, '')
    || ' '
    || coalesce(P.bore_in, '')
    || ' '
    || coalesce(P.duty, '')
    || ' '
    || coalesce(P.zone, '')
    || ' '
    || coalesce(P.holdflag, '')
    || ' '
    || coalesce(P.clientdocno, '')
    || ' '
    || coalesce(P.ispe, '')
    || ' '
    || coalesce(P.pspe, '')
    || ' '
    || coalesce(P.tspe, '') 
    || coalesce(P.rocstatus, '') 
    || coalesce(P.progress, '') 
    AS SEARCH_KEY1,
    Split_part(P.zone, '-', 2)
    || ' '
    || Substring(Replace(P.zone, '/', ''), 2, 2)
    || ' '
    || P.areacode
    || ' '
    || P.pidref
    || ' '
    --|| P.unique_line_id
    --|| ' '
    || coalesce(S.cumpercent,' ')
    || ' '
    || coalesce( CASE
        WHEN P.stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN
        ' No Status'
        ELSE P.stvval_wor_status_proc_pipe
    END, ' ')
    || coalesce(P.rocstatus, '') 
    || coalesce(P.progress, '') 
     AS SEARCH_KEY2,
CASE
	WHEN p.pidlineno LIKE '%-%-%-%-%' THEN
    REGEXP_EXTRACT(p.pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)-', 1)
	WHEN p.pidlineno LIKE '%-%-%-%' THEN
    REGEXP_EXTRACT(p.pidlineno, '^[^-]*-([^-]*-[^-]*)-', 1)
	WHEN p.pidlineno LIKE '%-%-%' THEN
    REGEXP_EXTRACT(p.pidlineno, '^[^-]*-([^-]*)-', 1)
	WHEN p.pidlineno LIKE '%-%' THEN null
END AS UNIQUE_LINE_ID,
	'VGCP2'|| '_' ||
    Split_part(coalesce(P.site, ''), '-', 2) || '_' ||
    Substring(Replace(P.zone, '/', ''), 2, 2)|| '_' ||
    Substring(Replace(P.zone, '/', ''), 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE,    
    P.source_system_name,
    'VGCP2' project_code,
	cast(P.execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
    {{ source('curated_e3d', 'curated_vg_e3d_vglpipes_sheet') }} P
    LEFT OUTER JOIN {{ source('curated_e3d', 'curated_dim_memstatus') }} S
    ON P.type
    || RIGHT(stvval_wor_status_proc_pipe, 1) = S.disc
    || S.status
    WHERE P.zone NOT LIKE '%TEMP%'
    AND P.zone <> ''

