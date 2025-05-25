{{ config(materialized='table') }}


SELECT DISTINCT
    autocomputeactuals,
    calculatecostfromunits,
    calendarname,
    calendarobjectid,
    createdate,
    createuser,
    currencyid,
    currencyname,
    currencyobjectid,
    defaultunitspertime,
    effectivedate,
    guid,
    val_id,
    isactive,
    isovertimeallowed,
    lastupdatedate,
    lastupdateuser,
    latitude,
    locationobjectid,
    longitude,
    maxunitspertime,
    name,
    objectid,
    overtimefactor,
    parentobjectid,
    priceperunit,
    primaryroleid,
    primaryrolename,
    primaryroleobjectid,
    resourcenotes,
    resourcetype,
    sequencenumber,
    shiftobjectid,
    timesheetapprovalmanager,
    timesheetapprovalmanagerobjectid,
    title,
    unitofmeasureabbreviation,
    unitofmeasurename,
    unitofmeasureobjectid,
    usetimesheets,
    username,
    userobjectid,
    execution_date
--    is_current
FROM 
(
    SELECT DISTINCT
        *,
        ROW_NUMBER() OVER (PARTITION BY objectid order by execution_date DESC) as rownum
    FROM 
    {{ source('curated_vg_p6', 'curated_resource') }}
--    WHERE is_current = 1
) t
WHERE rownum = 1