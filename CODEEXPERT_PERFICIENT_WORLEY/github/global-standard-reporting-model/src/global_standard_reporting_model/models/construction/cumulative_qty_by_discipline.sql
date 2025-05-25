{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

select 
getdate() as execution_date,
iwp.iwpsid,
iwp.cwpsid,
iwp.projectid as project_id,
iwp.updated_date as iwp_updated_date,
iwp.status as iwp_status,
iwp.disciplineid as iwp_disciplineid,
iwp.iwpsdescription as iwp_description,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
iwp.budgetedhours as budgetedhours,
iwp.remaininghours as remaininghours,
iwp.estimatedhours as estimatedhours,
iwp.earnedhours as earnedhours,
iwp.plannedstartdate as iwp_manual_plannedstartdate,
iwp.plannedfinishdate as iwp_manual_plannedfinishdate,
iwp.actualstartdate as  iwp_actualstartdate,
iwp.actualfinishdate as iwp_actualfinishdate,

disc.disciplinedescription as disc_disciplinedescription,
cwp.cwpsid as cwp_cwpsid,
cwp.cwpsdescription as cwp_cwpsdescription,
cwp.entityid as cwp_entityid,
cwp.entitytypeid as cwp_entitytypeid,
cwa.cwasname as cwa_constructionworkarea,
cwp.contract as cwp_contract,
CONCAT(cwp_contract,CONCAT(' ',disc_disciplinedescription)) AS scope_discipline,

work_package.entityname as wp_entityname,
work_package.taskuom as wp_taskuom,
work_package.totalunits_sum as wp_totalunits,
work_package.percentcomplete_avg as wp_percentcomplete,
wp_totalunits * (wp_percentcomplete/100) as wp_earned_qty,

dimdate.date_key,
dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) as weekend

from   
{{ source('domain_integrated_model','o3_iwps')}} iwp
inner join {{ source('domain_integrated_model', 'o3_cwps') }} cwp on iwp.cwpsid=cwp.cwpsid

left join
( select entityname, taskuom, sum(totalunits) as totalunits_sum, avg(percentcomplete) as percentcomplete_avg 
from {{ source('domain_integrated_model', 'o3_work_package_execution_tasks') }} GROUP BY entityname, taskuom ) work_package 
on iwp.iwpsid = work_package.entityname

left join  {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join  {{ source('global_standard_model', 'dim_date') }} dimdate on iwp.plannedstartdate = dimdate.date_key
where iwp.projectid = 41

