{{
     config(

         materialized = "table",
         tags=["construction"]
         )
}}

select 
iwp.updated_date as iwp_updated_date,
iwp.projectid as project_id,
iwp.iwpsid as iwp_iwpsid,
iwp.cwpsid as iwp_cwpsid,
cwp.cwpsid as cwp_cwpsid,
cwpsdescription as cwp_cwpsdescription,
cwp.entityid as cwp_entityid,
cwp.entitytypeid as cwp_entitytypeid,
disc.disciplinedescription as disc_disciplinedescription,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
cwa.cwasname as cwa_constructionworkarea,
cwp.contract as cwp_contract,
CONCAT(cwp_contract,CONCAT(' ',disc_disciplinedescription)) AS scope_discipline,

iwp.status as iwp_status,
iwp.estimatedhours as estimatedhours,
iwp.remaininghours as remaininghours,
iwp.criticalpath as criticalpath,
iwp.plannedstartdate as iwp_manual_plannedstartdate,
iwp.plannedfinishdate as iwp_manual_plannedfinishdate,
iwp.actualstartdate as  iwp_actualstartdate,
iwp.actualfinishdate as iwp_actualfinishdate,    
dimdate.date_key,
TO_CHAR(dimdate.date_key, 'Day') as weekday_name,
CASE when  EXTRACT(DOW FROM date (dimdate.date_key)) = 0 then  dimdate.date_key
else dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key)) as INT) end as weekend,

CASE  
when weekend = 
CASE when EXTRACT(DOW FROM date(GETDATE())) = 0  then  CAST(GETDATE() as date) 
else CAST(GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE)  end  then 'Current Week'
when weekend = 
CASE when EXTRACT(DOW FROM date(GETDATE())) = 0  then  CAST(GETDATE() as date) -7
else CAST(GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) -7 end  then 'Previous Week' 
end as report_week,    

work_package.entityname as wp_entityname,
work_package.taskuom as wp_taskuom,
work_package.totalunits_sum as wp_totalunits_sum,
work_package.percentcomplete_avg as wp_percentcomplete_avg,
wp_totalunits_sum * (wp_percentcomplete_avg/100) as wp_earned_qty

from
{{ source('domain_integrated_model','o3_iwps')}} iwp
inner join {{ source('domain_integrated_model', 'o3_cwps') }} cwp on  iwp.cwpsid=cwp.cwpsid

left join
( select entityname, taskuom, sum(totalunits) as totalunits_sum, avg(percentcomplete) as percentcomplete_avg 
from {{ source('domain_integrated_model', 'o3_work_package_execution_tasks') }} GROUP BY entityname, taskuom ) work_package 
on iwp.iwpsid = work_package.entityname

left join  {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join  {{ source('global_standard_model', 'dim_date') }} dimdate on iwp.plannedstartdate = dimdate.date_key
where iwp.projectid = 41