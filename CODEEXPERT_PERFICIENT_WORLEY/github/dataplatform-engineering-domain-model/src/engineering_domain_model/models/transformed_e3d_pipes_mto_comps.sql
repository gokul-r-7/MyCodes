{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_pipes_mto_comps/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


SELECT  
    CASE
        WHEN M.pipe LIKE '/%' THEN Concat('', Substring(Upper(M.pipe), 2))
        ELSE M.pipe
    END AS PIPE_WS,
    M.WBS,
    M.SITE,
    M.ZONE,
    M.PIPE,
    M.BRANCH,
    M.TYPE,
    M.REF,
    M.TAG as TAG_NAME,
    M.SPREF,
    M.ISPEC,
    M.NB1,
    M.NB2,
    M.NB3,
    M.NB4,
    M.LENGTH,
    M.DESCRIPTION,
    M.MATERIAL,
    M.IDCODE,
    M.CMCODE,
    M.LINENO,
    M.DUTY,
    M.WGT,
    M.WGT_LBS_W_INSU,
    M.EWP,
    M.CWA,
    M.CWP,
    M.CWPZONE,
    M.ISO,
    M.DRAWING_NO,
    M.P_ID_LINE_NO,
    M.P_ID_REFERENCE,
    Split_part(M.site, '-', 2) AS WBS1,
    Substring(Replace(M.zone, '/', ''), 2, 2) AS CWA1,
    Substring(Replace(M.zone, '/', ''), 2, 4) AS CWPZONE1,
    Replace(M.zone, '/', '') AS CWP1,
    ( CASE
        WHEN rocstatus NOT LIKE 'ME%' THEN ' No Status'
        WHEN rocstatus IS NULL THEN ' No Status'
        ELSE rocstatus
    END ) AS STATUS_LEVEL,
    Split_part(spref, '/', 2) AS PIPING_SPECIFICATION,
    coalesce(Split_part(M.site, '-', 2), '')
    || ' '
    || coalesce(Substring(Replace(M.zone, '/', ''), 2, 2), '')
    || ' '
    || coalesce(Substring(Replace(M.zone, '/', ''), 2, 4), '')
    || ' '
    || coalesce(Replace(M.zone, '/', ''), '')
    || ' '
    || coalesce(M.iso, '')
    || ' '
    || coalesce(M.material, '')
    || ' '
    || coalesce(M.nb1, '')
    || ' '
    || coalesce(M.spref, '')
    || ' '
    || coalesce(M.pipe, '')
    || ' '
    || coalesce(M.tag, '')
    || ' '
    || coalesce(M.type, '')
    || ' '
    || coalesce(M.length, '')
    || ' '
    || coalesce(M.wgt, '')
    || ' '
    || coalesce(M.description, '')
    || ' '
    || coalesce(M.ewp, '')
    --||' '||LINE  
    || ' '
    || coalesce(M.execution_date, '')
    || ' '
    || ( CASE
        WHEN stvval_wor_status_proc_pipe NOT LIKE 'ME%' THEN ' No Status'
        WHEN stvval_wor_status_proc_pipe IS NULL THEN ' No Status'
        ELSE stvval_wor_status_proc_pipe
    END )
    || ( CASE
        WHEN rocstatus NOT LIKE 'ME%' THEN ' No Status'
        WHEN rocstatus IS NULL THEN ' No Status'
        ELSE rocstatus
    END )  
    AS SEARCH_KEY,
	'VGCP2'|| '_' ||
    Split_part(M.site, '-', 2) || '_' ||
    Substring(Replace(M.zone, '/', ''), 2, 2) || '_' ||
    Substring(Replace(M.zone, '/', ''), 2, 4) as PROJECT_CODE_WBS_CWA_CWPZONE,
    'VGCP2' project_code,
    cast(M.execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM 
    {{ source('curated_e3d', 'curated_pipecompwbsrep') }} M
    LEFT OUTER JOIN {{ source('curated_e3d', 'curated_vg_e3d_vglpipes_sheet') }} P
    ON CASE
    WHEN Substring(Upper(M.pipe), 1, 1) = '/' THEN
    Concat('', Substring(Upper(M.pipe), 2
    ))
    ELSE Upper(M.pipe)
    END
    || '_'
    || M.execution_date = Upper((
    CASE
    WHEN P.NAME LIKE '/%' THEN
    Substring(P.NAME, 2,
    Length(P.NAME) - 1)
    ELSE P.NAME
    END ))
    || '_'
    || P.execution_date
    WHERE 
    ( nullif(M.nb1, '') = ''
    OR M.nb1 LIKE '%in%' )
    AND ( nullif(M.length, '') = ''
    OR M.length LIKE '%in%' )
    AND ( wgt LIKE '%kg'
    OR ( wgt = '0.0000' ) )
    AND M.zone NOT LIKE '%TEMP%'

