{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

select 
project_id,
cwpsid,
iwpsid,
iwp_discipline_description,
scope_discipline,
plannedstartdate,
plannedfinishdate,
datecreated,
Earned_hours,
estimatedhours,
weekly_estimated_hours,
day_name,
daily_estimated_hours,
percent_earnedhours,
cwa_constructionworkarea,
weekend,
week,
iwp_updated_date,
execution_date,
row_number() over (partition by iwpsid,weekend order by datecreated) as row_number,
case when row_number=1 then weekly_estimated_hours else 0.00 end Weekly_est_hours_identity
from 

(
select  
iwp.projectid as project_id,
cwp.cwpsid,
iwp.iwpsid,
disc.disciplinedescription as iwp_discipline_description,
CONCAT(cwp.contract,CONCAT(' ',iwp_discipline_description)) AS scope_discipline,
iwp.plannedstartdate,
iwp.plannedfinishdate,
change_history.datecreated,
change_history.Earned_hours,
iwp.estimatedhours,
Round(iwp.estimatedhours/7,2)  as weekly_estimated_hours,
to_char(dimdate.date_key, 'Day') as  day_name,
Round(weekly_estimated_hours/7,2) as daily_estimated_hours,
Case when daily_estimated_hours is null or daily_estimated_hours <=0 then 0.00  else Round((change_history.Earned_hours/daily_estimated_hours)*100,2) end percent_earnedhours,
cwa.cwasname as cwa_constructionworkarea,
Case when  EXTRACT(DOW FROM date (dimdate.date_key) ) = 0 then  dimdate.date_key
else dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT)  end as weekend,

Case  when 
weekend = 
Case when EXTRACT(DOW FROM date (GETDATE()) ) = 0  then  Cast (GETDATE() as date) 
else CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE)  end  then 'Current Week'
When weekend = 
 Case when EXTRACT(DOW FROM date (GETDATE()) ) = 0  then  Cast (GETDATE() as date) -7
 else CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) -7 end  then 'Previous Week' end  as week,
iwp.updated_date as iwp_updated_date,
getdate() as execution_date

from {{ source('domain_integrated_model', 'o3_cwps') }} cwp
inner join  {{ source('domain_integrated_model', 'o3_iwps') }} iwp on cwp.cwpsid=iwp.cwpsid
inner join
(
select 
projectid,entityname,cast(datecreated as date) datecreated,cast(max(newvalue) as numeric(9,2)) newvalue,
lag(  cast(max(newvalue) as numeric(9,2) )) over (partition by entityname order by Cast(datecreated as date) asc) previous_Val,
case when previous_Val is null then  cast(max(newvalue) as numeric(9,2)) else  cast ( max(newvalue) as numeric(9,2)) - previous_Val  end as Earned_hours
from 
{{ source('domain_integrated_model', 'o3_change_history') }}
where fieldname= 'Earned Hours'  and  cast(newvalue as decimal(9,2))>0 and Cast(datecreated as date) is not null
group by projectid,entityname,cast(datecreated as date) order by entityname ,Cast(datecreated as date)
)change_history 
on iwp.iwpsid=change_history.entityname
left join {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('global_standard_model', 'dim_date') }} dimdate on change_history.datecreated = dimdate.date_key
)comp_hours where project_id=41