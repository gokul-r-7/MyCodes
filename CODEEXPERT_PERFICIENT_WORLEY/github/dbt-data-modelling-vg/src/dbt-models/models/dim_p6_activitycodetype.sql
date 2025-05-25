{{ config(materialized='table') }}

SELECT DISTINCT
    createdate,
    createuser,
    epsobjectid,
    isbaseline,
    issecurecode,
    istemplate,
    lastupdatedate,
    lastupdateuser,
    length,
    name,
    objectid,
    projectobjectid,
    scope,
    sequencenumber,
    execution_date
--    is_current
 FROM
 (   
    SELECT DISTINCT
       *,
       ROW_NUMBER() OVER (PARTITION BY objectid order by execution_date DESC) as rownum
    FROM 
    {{ source('curated_vg_p6', 'curated_project_activitycodetype') }}
--    WHERE is_current = 1
) t
WHERE rownum = 1