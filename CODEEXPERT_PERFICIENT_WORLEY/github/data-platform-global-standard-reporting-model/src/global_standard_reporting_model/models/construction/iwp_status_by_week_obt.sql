{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

with iwps_details as
(  
select 
iwp.projectid,
iwp.iwpsid,
iwp.cwpsid,
iwp.plannedstartdate,
iwp.plannedfinishdate,
dt_date.date_key,
 Case when  EXTRACT(DOW FROM date (  cast( dt_date.date_key as date)) ) = 0 then  cast(dt_date.date_key as date)
                                      else cast(dt_date.date_key as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(dt_date.date_key as date)) ) as INT)  end as weekend, 
iwp.actualstartdate,
iwp.actualfinishdate,
iwp.PercentComplete,
iwp.disciplineid,
iwp.DISCIPLINEDESCRIPTION,
iwp.constructionworkareaid,
iwp.contract

 from
(SELECT
iwp.projectid  ,
iwp.iwpsid,
iwp.cwpsid,
---iwp.plannedstartdate,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-01' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-04-01'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-05-01'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-05-15'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-01'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-16'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-07-01'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-07-10'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-01'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-09-28'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-10-16'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-01-11' else iwp.plannedstartdate  end as plannedstartdate,
---iwp.plannedfinishdate
---dt_date.date_key,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-30' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-05-15'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-05-31'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-06-20'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-15'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-30'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-07-31'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-08-15'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-28'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-10-15'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-11-15'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-12-31' else iwp.plannedfinishdate end as plannedfinishdate,
----iwp.actualstartdate,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-01' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-04-01'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-05-01'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-05-15'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-01'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-16'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-07-01'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-07-10'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-01'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-09-28'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-10-16'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-01-11' else iwp.actualstartdate end as actualstartdate,
---iwp.actualfinishdate,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-30' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-05-15'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-05-31'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-06-20'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-15'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-30'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-07-31'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-08-15'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-28'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-10-15'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-11-15'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-12-31' else iwp.actualfinishdate end as actualfinishdate,
---iwp.PercentComplete,
case when iwp.iwpsid = 'EUU00UGC08-EW003' then 10
when  iwp.iwpsid = 'EUU00UGC08-EW004' then 20
when  iwp.iwpsid ='EUU00UGC16-EW004' then 30
when  iwp.iwpsid ='EUU00UGC08-EW006' then 40
when  iwp.iwpsid ='EUU00UGC16-EW010' then 45
when  iwp.iwpsid ='EUU00UGC16-EW009' then 50
when  iwp.iwpsid ='EUU00UGC16-EW001' then 56
when  iwp.iwpsid ='EUU00UGC08-EW005' then 15
when  iwp.iwpsid ='EUU00UGC16-EW008' then 21
when  iwp.iwpsid ='EUU00UGC16-EW003' then 24
when  iwp.iwpsid ='EUU00UGC16-EW002' then 33
when  iwp.iwpsid ='EUU00UGC16-EW005' then 77 else iwp.PercentComplete end as PercentComplete,
iwp.disciplineid,
iwp.DISCIPLINEDESCRIPTION,
iwp.constructionworkareaid,
iwp.contract
FROM  {{ source('domain_integrated_model', 'transformed_iwps') }}  iwp) iwp
inner join {{ source("global_standard_model","dim_date_new1" ) }}  dt_date on dt_date.date_key between iwp.plannedstartdate and iwp.plannedfinishdate 
WHERE exists (select * from {{ source('domain_integrated_model', 'transformed_cwps') }} cwp where iwp.projectid=cwp.projectid and iwp.cwpsid=cwp.cwpsid)
),iwps AS(
select projectid  ,
iwpsid,
cwpsid,
max(plannedstartdate) plannedstartdate,
max(plannedfinishdate) plannedfinishdate ,
weekend, 
max(actualstartdate) actualstartdate ,
max(actualfinishdate) actualfinishdate,
max(PercentComplete) as PercentComplete ,
max(disciplineid) as disciplineid ,
max(DISCIPLINEDESCRIPTION) as DISCIPLINEDESCRIPTION,
max(constructionworkareaid) as  constructionworkareaid,
max(contract) as   contract
from iwps_details
group by projectid,iwpsid,
cwpsid,weekend
),
change_history AS(
select 
projectid,entityname as iwpsid,fieldname,
row_number() over (partition by entityname order by  entityname ) row_id,

case when entityname='EUU00UGC08-EW003' then 
    case when row_id =1 then '2025-04-15' 
         when row_id =2 then '2025-04-16' 
         when row_id =3 then '2025-04-17' 
         when row_id =4 then '2025-04-18' end

        when  entityname='EUU00UGC16-EW004' then
    case when row_id =1 then '2025-04-12'  
         when row_id =2 then '2025-04-26' 
         when row_id =3 then '2025-04-27'  
         when row_id =4 then '2025-05-01'  
         when row_id =5 then '2025-05-02'  
         when row_id =6 then '2025-05-03'  END

     when  entityname='EUU00UGC08-EW004' then
    case when row_id =1 then '2025-05-10'  
         when row_id =2 then '2025-05-17' 
         when row_id =3 then '2025-05-18'  
         when row_id =4 then '2025-05-31'  END 
    END 
  as datecreated_derived,

case when entityname='EUU00UGC08-EW003' then 
    case when row_id =1 then 10 
         when row_id =2 then 20 
         when row_id =3 then 30 
         when row_id =4 then 40 end
    
    when  entityname='EUU00UGC16-EW004'  then
    case when row_id =1 then 100 
         when row_id =2 then 110
         when row_id =3 then 120 
         when row_id =4 then 130
         when row_id =5 then 140 
         when row_id =6 then 150
         end 
   when  entityname='EUU00UGC08-EW004' then
         case when row_id =1 then 200
         when row_id =2 then 210
         when row_id =3 then 220
         when row_id =4 then 230 end 
   end newvalue,

   Case when  EXTRACT(DOW FROM date (  cast( datecreated_derived as date)) ) = 0 then  cast(datecreated_derived as date)
                                      else cast(datecreated_derived as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(datecreated_derived as date)) ) as INT)  end as weekend 
 from  {{ source('domain_integrated_model', 'transformed_change_history') }} ch
where entityname in ('EUU00UGC08-EW003' ,'EUU00UGC08-EW004','EUU00UGC16-EW004')
and ch.fieldname= '% Complete'  ---and  cast(ch.newvalue as decimal(9,2))>0 and Cast(ch.datecreated as date) is not null
order by iwpsid
),changhistory_max as 
(
select * from change_history a  
where cast(a.datecreated_derived as datetime)=(select max(cast(b.datecreated_derived as datetime)) from change_history b where a.projectid=b.projectid 
and a.iwpsid=b.iwpsid and a.weekend=b.weekend)
),fts_timesheet AS
(
select projectid,iwpsid,weekendingdate,sum(spenthours)+sum(overtimehours) spenthours from  {{ source('domain_integrated_model', 'transformed_fts_timesheet') }}
where iwpsid in ('EUU00UGC08-EW003' ,'EUU00UGC08-EW004','EUU00UGC16-EW004')
group by projectid,iwpsid,weekendingdate
union all
select '41','EUU00UGC08-EW003' ,'2025-04-06',20
union all
select '41','EUU00UGC08-EW003' ,'2025-04-13',30
union all
select '41','EUU00UGC08-EW004' ,'2025-05-11',20
union all
select '41','EUU00UGC08-EW004' ,'2025-05-25',40
union all
select '41','EUU00UGC08-EW004' ,'2025-06-01',20
union all
select '41','EUU00UGC16-EW004', '2025-04-20',30
union all
select '41','EUU00UGC16-EW004','2025-04-27',20
union all
select '41','EUU00UGC16-EW004','2025-05-04',40
union all
select '41','EUU00UGC16-EW004','2025-05-11',20
)  
-- ,merge_ch_fts AS
-- (
-- select 
-- case when ch.projectid is null then fts.projectid else ch.projectid end  as projectid,
-- case when ch.entityname is null then fts.iwpsid else ch.entityname end as iwpsid , 
-- case when ch.weekend is null  then cast(fts.weekendingdate as date) else ch.weekend end as weekend,
-- ch.newvalue  as earnedhours,fts.spenthours  from changhistory_max ch
-- full join fts_timesheet fts ON ch.projectid=fts.projectid and ch.ENTITYNAME=fts.iwpsid and ch.weekend=fts.weekendingdate
-- order by iwpsid,weekend
-- )
,chs_start_weekend AS
(
select projectid,iwpsid as iwpsid,cast(min(weekend) as date) as min_weekend from changhistory_max
group by projectid,iwpsid
),fts_start_weekend AS
(
select projectid,iwpsid,cast(min(weekendingdate) as date) as min_weekend from fts_timesheet
group by projectid,iwpsid
)
select 
project_id,
iwpsid,
cwpsid,
disciplinedescription,
cwasdescription,
scope_discipline,
plannedstartdate,
plannedstartdate_weekend,
plannedfinishdate,
plannedfinishdate_weekend,
actualstartdate,
actualstartdate_weekend,
actualfinishdate,
actualfinishdate_weekend,
PercentComplete,
weekend,
earnedhours,
spenthours,
earned_minweekend,
spent_minweekend,
case when actualstartdate_weekend is not null and actualstartdate_weekend<=earned_minweekend and actualstartdate_weekend<=spent_minweekend then actualstartdate_weekend
     when earned_minweekend is not null  and earned_minweekend<=actualstartdate_weekend and earned_minweekend<=spent_minweekend then earned_minweekend
     when spent_minweekend is not null  and spent_minweekend<=actualstartdate_weekend and spent_minweekend<=earned_minweekend then spent_minweekend
     else actualstartdate_weekend end min_startweekend,
Case when iwps.weekend = min_startweekend then 1
     when iwps.weekend = actualfinishdate_weekend then 2
     when earnedhours>0 and spenthours>0 then 3
     when (earnedhours is null)  and (spenthours is null) then 4
     when spenthours>0 and  (earnedhours is null) then 5
     when earnedhours>0 and (spenthours is null) then 6
     end  as category
 from 
(
select 
cast(iwp.projectid as varchar(100)) as project_id,
iwp.iwpsid,
iwp.cwpsid,
disc.disciplinedescription,
cwa.cwasdescription,
CONCAT(iwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
cast (iwp.plannedstartdate as date) as plannedstartdate ,
Case when  EXTRACT(DOW FROM date (  cast( iwp.plannedstartdate as date)) ) = 0 then  cast(iwp.plannedstartdate as date)
                                      else cast(iwp.plannedstartdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedstartdate as date)) ) as INT)  end as plannedstartdate_weekend,
cast(iwp.plannedfinishdate as date) as plannedfinishdate,
Case when  EXTRACT(DOW FROM date (  cast( iwp.plannedfinishdate as date)) ) = 0 then  cast(iwp.plannedfinishdate as date)
                                      else cast(iwp.plannedfinishdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedfinishdate as date)) ) as INT)  end as plannedfinishdate_weekend,
cast(iwp.actualstartdate as date) as actualstartdate,
Case when  EXTRACT(DOW FROM date (  cast( iwp.actualstartdate as date)) ) = 0 then  cast(iwp.actualstartdate as date)
                                      else cast(iwp.actualstartdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.actualstartdate as date)) ) as INT)  end as actualstartdate_weekend,
cast(iwp.actualfinishdate as date) as actualfinishdate ,
Case when  EXTRACT(DOW FROM date (  cast( iwp.actualfinishdate as date)) ) = 0 then  cast(iwp.actualfinishdate as date)
                                      else cast(iwp.actualfinishdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.actualfinishdate as date)) ) as INT)  end as actualfinishdate_weekend,
cast(iwp.PercentComplete as float) as PercentComplete ,
cast(iwp.weekend as date) as weekend,
cast(ch.newvalue as float) as earnedhours,
cast(ftss.spenthours as float) as spenthours,
ch_startweekend.min_weekend as earned_minweekend,
fts_startweekend.min_weekend as spent_minweekend
-- case when actualstartdate_weekend is not null and actualstartdate_weekend<=earned_minweekend and actualstartdate_weekend<=spent_minweekend then actualstartdate_weekend
--      when earned_minweekend is not null  and earned_minweekend<=actualstartdate_weekend and earned_minweekend<=spent_minweekend then earned_minweekend
--      when spent_minweekend is not null  and spent_minweekend<=actualstartdate_weekend and spent_minweekend<=earned_minweekend then spent_minweekend
--      else actualstartdate_weekend end min_startweekend
from iwps iwp
-- left join merge_ch_fts mfc on iwp.projectid=mfc.projectid and iwp.iwpsid=mfc.iwpsid
left join changhistory_max ch on iwp.projectid=ch.projectid and iwp.iwpsid=ch.iwpsid and iwp.weekend=ch.weekend
left join fts_timesheet ftss on iwp.projectid=ftss.projectid and iwp.iwpsid=ftss.iwpsid and iwp.weekend=ftss.weekendingdate
left join {{ source('domain_integrated_model', 'transformed_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join {{ source('domain_integrated_model', 'transformed_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join chs_start_weekend ch_startweekend on iwp.projectid=ch_startweekend.projectid and iwp.iwpsid=ch_startweekend.iwpsid
left join fts_start_weekend fts_startweekend on iwp.projectid=fts_startweekend.projectid and iwp.iwpsid= fts_startweekend.iwpsid
-- where iwp.iwpsid in ('EUU00UGC08-EW003','EUU00UGC08-EW004','EUU00UGC16-EW004')
order by iwp.iwpsid,iwp.weekend
) iwps

