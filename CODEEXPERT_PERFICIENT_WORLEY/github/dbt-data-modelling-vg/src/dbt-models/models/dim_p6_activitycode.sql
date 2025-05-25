{{ config(materialized='table') }}


SELECT DISTINCT
    codeconcatname,
    codetypename,
    codetypeobjectid,
    codetypescope,
    codevalue,
    color,
    createdate,
    createuser,
    description,
    lastupdatedate,
    lastupdateuser,
    objectid,
    parentobjectid,
    projectobjectid,
    sequencenumber,
    execution_date
--    is_current
FROM
(
    SELECT DISTINCT
        *,
        ROW_NUMBER() OVER (PARTITION BY objectid order by execution_date DESC) as rownum
    FROM 
        {{ source('curated_vg_p6', 'curated_project_activitycode') }}
    -- WHERE is_current = 1
) t
WHERE rownum = 1