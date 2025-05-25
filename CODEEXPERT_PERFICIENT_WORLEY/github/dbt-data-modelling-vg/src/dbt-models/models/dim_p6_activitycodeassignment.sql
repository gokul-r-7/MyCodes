{{ config(materialized='table') }}

SELECT DISTINCT
    activityid,
    activityobjectid,
    project_id,
    projectobjectid,
    activityname,
    activitycodetypeid,
    activitycodeid,
    execution_date
--    is_current
FROM
( 
    SELECT DISTINCT
        *,
        ROW_NUMBER() OVER (PARTITION BY activityobjectid,activitycodeid,activitycodetypeid order by execution_date DESC) as rownum
    FROM 
    {{ source('curated_vg_p6', 'curated_activitycodeassignment') }}

    WHERE -- is_current = 1 and 
    projectobjectid = projectid
) t
WHERE rownum = 1