{{ config(materialized='table') }}


SELECT DISTINCT
    startdate,
    enddate,
    periodtype,
    period_startdate,
    period_enddate,
    objectid,
    remainingunits,
    actualcost,
    actualovertimeunits,
    actualregularcost,
    actualregularunits,
    actualunits,
    atcompletioncost,
    atcompletionunits,
    plannedcost,
    plannedunits,
    remainingcost,
    remaininglatecost,
    staffedremainingcost,
    staffedremaininglatecost,
    staffedremaininglateunits,
    staffedremainingunits,
    unstaffedremainingcost,
    unstaffedremaininglatecost,
    unstaffedremaininglateunits,
    unstaffedremainingunits,
    cumulativeactualcost,
    cumulativeactualovertimecost,
    cumulativeactualovertimeunits,
    cumulativeactualregularcost,
    cumulativeactualregularunits,
    cumulativeactualunits,
    cumulativeatcompletioncost,
    cumulativeatcompletionunits,
    cumulativeplannedcost,
    cumulativeplannedunits,
    cumulativeremainingcost,
    cumulativeremaininglatecost,
    cumulativestaffedremainingcost,
    cumulativestaffedremaininglatecost,
    cumulativestaffedremaininglateunits,
    cumulativestaffedremainingunits,
    cumulativeunstaffedremainingcost,
    cumulativeunstaffedremaininglatecost,
    cumulativeunstaffedremaininglateunits,
    cumulativeunstaffedremainingunits,
    remaininglateunits,
    cumulativeremaininglateunits,
    cumulativeremainingunits,
    execution_date
    --is_current
 FROM
 (   
    SELECT DISTINCT
       *,
       ROW_NUMBER() OVER (PARTITION BY objectid,period_startdate,period_enddate order by execution_date DESC) as rownum
    FROM
    {{ source('curated_vg_p6', 'curated_resourcespread') }}
    --WHERE is_current = 1
) t
WHERE rownum = 1