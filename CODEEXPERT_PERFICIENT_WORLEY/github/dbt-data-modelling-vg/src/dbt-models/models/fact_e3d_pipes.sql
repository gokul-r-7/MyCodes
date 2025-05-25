{{
    config(
        materialized='incremental'
    )
}}


SELECT facilitycode,
       insulation,
       rocstatus,
       pipeplanref,
       piddesc,
       revdesc6,
       clientdocrev,
       revdata6,
       ouprtr,
       holdc1,
       wcgloc,
       fluidtype,
       duty,
       matlspec,
       processhold,
       flur,
       steeltype,
       mtorev,
       psisystem,
       tspe,
       secdel,
       holdcomment,
       pidlineno,
       holdc3,
       checked,
       nsec,
       lock,
       design_level,
       processremark,
       pidstat,
       chkdate,
       secto,
       secno,
       ispe,
       section_name,
       secfr,
       revdesc9,
       pwht,
       revdata5,
       fluidservicecode,
       mtoarea,
       requisition,
       pmax,
       testpress,
       designtemp,
       designpress,
       site,
       splp,
       deldsg,
       systemno,
       approv,
       cgflowmed,
       wcgdte,
       material,
       frev,
       revdata7,
       ctllocation,
       wcgstat,
       below_ground,
       buil,
       pipsta,
       tagno,
       liss,
       casn,
       transmittalno,
       erec,
       smax,
       holdc2,
       fabspec,
       wpdocrev,
       bendmacreference,
       mocno,
       areacode,
       comment,
       apprby,
       tcgpos,
       progress,
       revdesc8,
       fdra,
       pidref,
       cleaningcode,
       htemp,
       track_id,
       asd_dpres,
       tpress_psi,
       revdesc1,
       insc,
       cgvolume,
       care,
       stressrelieve,
       pres_psi,
       holdc4,
       proj,
       radiography,
       frdr,
       doctype,
       posafc,
       mecheck,
       temp_degf,
       revdesc3,
       e3d_pipes.from,
       workpackno,
       dcgpos,
       issuedby,
       piecemark,
       func,
       revdata3,
       wldc,
       revdata8,
       paintsystem,
       pidrev,
       uom,
       cnum,
       clash,
       clientdocno,
       drrf,
       stresscritical,
       drawn,
       mtoiss,
       commcode,
       ocgpos,
       ccla,
       pedcat,
       holdc5,
       planu,
       cdrg,
       strucate,
       sdrlcode,
       matr,
       model,
       drawnby,
       casr,
       rlstor,
       cladate,
       isohidden,
       revdesc5,
       jmax,
       heatt,
       purp,
       asd_dtemp,
       revdata1,
       pspe,
       fireproofing,
       prmodule,
       psidate,
       NAME,
       units,
       subsystem,
       hold,
       revdesc2,
       itemno,
       revdesc7,
       clientlineno,
       disccode,
       type,
       ccen,
       xray,
       revdata9,
       dsco,
       discipline,
       skey,
       worktype,
       critical,
       systemcode,
       strutype,
       testmedium,
       fpline,
       sloref,
       zone,
       issued,
       chkby,
       dwgno,
       fstatus,
       areano,
       safc,
       ewp,
       jntc,
       serialno,
       stvval_wor_status_proc_pipe,
       revdesc4,
       fstat,
       owner,
       revdata2,
       modulefabricator,
       fabwgt,
       isssta,
       mtodrwg,
       suffix,
       mdsysf,
       unitno,
       issueddate,
       wcgcomnt,
       ndegroup,
       stresssysno,
       bore_in,
       optemp,
       e3d_pipes.to,
       prdat,
       _type,
       cadarea,
       procrem,
       issdat,
       casdesc,
       holdflag,
       wmax,
       oppress,
       farea,
       mtoref,
       wpdocno,
       ptsp,
       inprtr,
       e3d_pipes.status,
       cwarea,
       shop,
       revdata4,
       dwgnorev,
       rev,
       e3d_pipes.desc,
       lntp,
       ithk,
       pga,
       CASE
         WHEN pidlineno LIKE '%-%-%-%-%' THEN
         Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)-', 1, 1, 'e')
         WHEN pidlineno LIKE '%-%-%-%' THEN
         Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)', 1, 1, 'e')
         WHEN pidlineno LIKE '%-%-%' THEN
         Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*)', 1, 1, 'e')
         WHEN pidlineno LIKE '%-%' THEN
         Regexp_substr(pidlineno, '^[^-]*-([^ ]*)', 1, 1
         , 'e')
       END                   AS UNIQUE_LINE_ID,
       execution_date,
       'VGCP2'               project_code,
       RIGHT(site, 3)        wbs,
       Substring(zone, 2, 2) cwa,
       Substring(zone, 2, 4) Cwpzone,
       zone                  cwp1,
       memstat.cumpercent    progresspercent,
       Nvl2(stvval_wor_status_proc_pipe, stvval_wor_status_proc_pipe,
       'No  Status')
                             statuslevel,
       COALESCE(RIGHT(site, 3), '')
       || ' '
       || COALESCE(cwarea, '')
       || ' '
       || COALESCE(areacode, '')
       || ' '
       || COALESCE(pidref, '')
       || ' '
       || COALESCE(CASE
                     WHEN pidlineno LIKE '%-%-%-%-%' THEN
                     Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)-', 1,
                     1, 'e')
                     WHEN pidlineno LIKE '%-%-%-%' THEN
                     Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*-[^-]*)', 1, 1
                     , 'e')
                     WHEN pidlineno LIKE '%-%-%' THEN
                     Regexp_substr(pidlineno, '^[^-]*-([^-]*-[^-]*)', 1, 1, 'e')
                     WHEN pidlineno LIKE '%-%' THEN
                     Regexp_substr(pidlineno, '^[^-]*-([^ ]*)', 1, 1
                     , 'e')
                   END, '')
       ||' '||coalesce(memstat.CUMPERCENT,'')
       || ' '
       || COALESCE(CASE
                     WHEN stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN
                     ' No Status'
                     ELSE stvval_wor_status_proc_pipe
                   END, '')
       || ' '
       || COALESCE(workpackno, '')
       || ' '
       || COALESCE(NAME, '')
       || ' '
       || COALESCE(execution_date, '')
       || ' '
       || COALESCE(bore_in, '')
       || ' '
       || COALESCE(duty, '')
       || ' '
       || COALESCE(zone, '')
       || ' '
       || COALESCE(holdflag, '')
       || ' '
       || COALESCE(clientdocno, '')
       || ' '
       || COALESCE(ispe, '')
       || ' '
       || COALESCE(pspe, '')
       || ' '
       || COALESCE(tspe, '') Search_key
FROM   dev.vg_e3d.curated_vg_e3d_vglpipes_sheet e3d_pipes

       LEFT JOIN (SELECT DISTINCT disc,
                                  status,
                                  cumpercent
                  FROM   "dev"."vg_e3d"."curated_dim_memstatus") memstat
              ON memstat.disc
                 || memstat.status = e3d_pipes.type || RIGHT(e3d_pipes.stvval_wor_status_proc_pipe, 1) 


{%if is_incremental() %}
  where execution_date >= (select coalesce(max(execution_date),'1900-01-01') from {{ this }} )
{% endif %}