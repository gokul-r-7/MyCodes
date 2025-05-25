{{ config(materialized='table') }}

select
    p.id as project_id,
    p.name as projectname,
    p.objectid as projectobjectid,
    p.datadate as projectdatadate,
    p.parentepsname as projectparentepsname,
    a.activityid as activityid ,
    a.objectid as activityobjectid,
    a.activityname,
    a.type as activity_type,
    a.status as activity_status,
    a.wbsname as activity_wbsname,
    a.wbspath as activity_wbspath,
    a.wbsnamepath as activity_wbsnamepath,
    a.wbscode as activity_wbscode,
    a.actualstartdate as activity_actualstartdate,
    a.actualfinishdate as activity_actualfinishdate,
    a.plannedstartdate as activity_plannedstartdate,
    a.plannedfinishdate as activity_plannedfinishdate,
    a.baselinestartdate as activity_baselinestartdate,
    a.baselinefinishdate as activity_baselinefinishdate,
    a.remaininglatestartdate as activity_remaininglatestartdate,
    a.remaininglatefinishdate as activity_remaininglatefinishdate ,
    a.remainingearlystartdate as activity_remainingearlystartdate,
    a.remainingearlyfinishdate as activity_remainingearlyfinishdate,
    a.earlystartdate as activity_earlystartdate,
    a.earlyfinishdate as activity_earlyfinishdate,
    a.latestartdate as activity_latestartdate,
    a.latefinishdate as activity_latefinishdate_activty,
    a.schedulevariance as activity_schedulevariance,
    a.floatpath as activity_floatpath,
    a.floatpathorder as activity_floatpathorder,
    a.freefloat as activity_freefloat,
    a.totalfloat as activity_totalfloat,
    a.plannedlaborunits as activity_plannedlaborunits,
    a.baselineplannedlaborunits as activity_baselineplannedlaborunits,
    a.remainingduration as  activity_remainingduration,
    a.startdate as activity_startdate  ,
    a.finishdate as activity_finishdate,
    a.baselineduration as activity_baselineduration,
    a.plannedduration as activity_plannedduration,
    a.finishdatevariance as activity_finishdatevariance,
    a.datadate as activity_datadate,
    ac.codeconcatname as activitycodeconcatname,
    ac.description as activitycodedescription,
    ac.codetypescope as activitycodetypescope,
    ac.codetypename as activitycodetypename,
    ac.codevalue as activitycodevalue,
    ra.resourceobjectid,
    rs.val_id as resource_id,
    rs.name as resourcename,
    rs.resourcetype,
    right(rs.val_id,charindex('-',reverse(rs.val_id))-1) as costtypeid,
    ra.objectid as resourceassignmentobjectid,
    ras.periodtype,
    ras.startdate,
    ras.enddate,
    ras.period_startdate,
    ras.period_enddate,
    ddt.date_key,
    ddt.monthyear,
    -- ras.cumulativeplannedunits - lag(ras.cumulativeplannedunits, 1) over (partition by
    -- ras.objectid order by period_enddate) as plannedunits,  
    -- ras.cumulativeremainingunits - lag(ras.cumulativeremainingunits, 1) over (partition by      
    -- ras.objectid order by period_enddate) as remainingunits,  
    -- ras.cumulativeremaininglateunits - lag(ras.cumulativeremaininglateunits, 1) over (partition by
    -- ras.objectid order by period_enddate) as remaininglateunits,    
    ras.plannedunits,
    ras.remainingunits,
    ras.remaininglateunits,
    ras.actualunits,
    ras.cumulativeactualunits,
    ras.cumulativeplannedunits,
    ras.cumulativeremainingunits,
    ras.cumulativeremaininglateunits
from
            {{ref('dim_p6_activity')}} a
inner join  {{ref('dim_p6_projects')}} p on p.objectid = a.projectobjectid
left join   {{ref('dim_p6_resourceassignment')}} ra on a.objectid = ra.activityobjectid
left join   {{ref('fact_p6_resourceassignmentspread')}} ras on ra.objectid = ras.objectid
left join   {{ref('dim_p6_resource')}} rs on rs.objectid = ra.resourceobjectid
left join   {{ref('dim_p6_activitycodeassignment')}} aca on aca.activityobjectid =  a.objectid
left join   {{ref('dim_p6_activitycode')}}  ac on aca.activitycodeid = ac.objectid
full join   {{ref('dim_p6_date')}} ddt on ddt.date_key=ras.period_enddate and 
ddt.date_key <= (select max(period_enddate)  from {{ref('fact_p6_resourceassignmentspread')}})
 
