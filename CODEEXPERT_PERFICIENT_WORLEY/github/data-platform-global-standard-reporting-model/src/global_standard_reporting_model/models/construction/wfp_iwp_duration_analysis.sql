{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}


with iwps as
(  
SELECT
iwp.projectid  ,
iwp.iwpsid,
iwp.cwpsid,
----iwp.Status
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then 'Open' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then 'Open'
when  iwp.iwpsid ='EUU00UGC16-EW004' then 'Open'
when  iwp.iwpsid ='EUU00UGC08-EW006' then 'Open'
when  iwp.iwpsid ='EUU00UGC16-EW010' then 'Open'
when  iwp.iwpsid ='EUU00UGC16-EW009' then 'Open'
when  iwp.iwpsid ='EUU00UGC16-EW001' then 'Closed'
when  iwp.iwpsid ='EUU00UGC08-EW005' then 'Closed'
when  iwp.iwpsid ='EUU00UGC16-EW008' then 'Closed'
when  iwp.iwpsid ='EUU00UGC16-EW003' then 'Closed'
when  iwp.iwpsid ='EUU00UGC16-EW002' then 'Closed'
when  iwp.iwpsid ='EUU00UGC16-EW005' then 'Closed' else iwp.status end as status,

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
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-11-01' else iwp.plannedstartdate end as plannedstartdate,
---iwp.plannedfinishdate
---dt_date.date_key,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-20' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-05-31'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-06-05'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-06-10'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-10'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-23'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-09-30'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-08-14'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-10'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-10-10'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-12-25'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-12-31' else iwp.plannedfinishdate end as plannedfinishdate,
----iwp.actualstartdate,
Case when  iwp.iwpsid = 'EUU00UGC08-EW003' then '2025-04-30' 
when  iwp.iwpsid = 'EUU00UGC08-EW004' then '2025-05-31'
when  iwp.iwpsid ='EUU00UGC16-EW004' then '2025-06-05'
when  iwp.iwpsid ='EUU00UGC08-EW006' then '2025-06-20'
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-01'
when  iwp.iwpsid ='EUU00UGC16-EW009' then '2025-06-30'
when  iwp.iwpsid ='EUU00UGC16-EW001' then '2025-07-31'
when  iwp.iwpsid ='EUU00UGC08-EW005' then '2025-08-20'
when  iwp.iwpsid ='EUU00UGC16-EW008' then '2025-09-10'
when  iwp.iwpsid ='EUU00UGC16-EW003' then '2025-10-15'
when  iwp.iwpsid ='EUU00UGC16-EW002' then '2025-10-25'
when  iwp.iwpsid ='EUU00UGC16-EW005' then '2025-12-31' else iwp.actualstartdate end as actualstartdate,
---iwp.actualfinishdate,
Case 
when  iwp.iwpsid ='EUU00UGC16-EW010' then '2025-06-15'
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
--- iwp.estimatedhours
case when iwp.iwpsid='EUU00UGC08-EW003' then 90
when iwp.iwpsid='EUU00UGC16-EW009' then 40
when iwp.iwpsid='EUU00UGC08-EW004' then 50
when iwp.iwpsid='EUU00UGC16-EW004' then 100
when iwp.iwpsid='EUU00UGC08-EW006' then 100
when iwp.iwpsid='EUU00UGC16-EW010' then 150
when iwp.iwpsid='EUU00UGC16-EW002' then 200 
when iwp.iwpsid='EUU00UGC08-EW005' then 300
when iwp.iwpsid='EUU00UGC16-EW008' then 400
when iwp.iwpsid='EUU00UGC16-EW001' then 500
when iwp.iwpsid='EUU00UGC16-EW005' then 600
when iwp.iwpsid='EUU00UGC16-EW003' then 700 else iwp.estimatedhours  end estimatedhours,
---iwp.earnedhours,
case when iwp.iwpsid = 'EUU00UGC16-EW004' then 100
  when iwp.iwpsid = 'EUU00UGC08-EW003' then 10
when iwp.iwpsid = 'EUU00UGC16-EW009' then 10
when iwp.iwpsid = 'EUU00UGC08-EW004' then 10
when  iwp.iwpsid = 'EUU00UGC08-EW006' then 90 
when  iwp.iwpsid = 'EUU00UGC16-EW002' then 11
when  iwp.iwpsid = 'EUU00UGC08-EW005' then 12
when  iwp.iwpsid = 'EUU00UGC16-EW008' then 13
when  iwp.iwpsid ='EUU00UGC16-EW003' then 14
when  iwp.iwpsid ='EUU00UGC16-EW001' then 15
when  iwp.iwpsid ='EUU00UGC16-EW005' then 16
else iwp.earnedhours end as earnedhours,
iwp.disciplineid,
iwp.DISCIPLINEDESCRIPTION,
iwp.constructionworkareaid,
iwp.contract
-- Case when  EXTRACT(DOW FROM date (  cast( iwp.plannedstartdate as date)) ) = 0 then  cast(iwp.plannedstartdate as date)
--                                       else cast(iwp.plannedstartdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedstartdate as date)) ) as INT)  end as plannedstartdate_Weekend,
-- Case when  EXTRACT(DOW FROM date (  cast( plannedfinishdate as date)) ) = 0 then  cast(iwp.plannedfinishdate as date)
--                                       else cast(plannedfinishdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedfinishdate as date)) ) as INT)  end as plannedfinishdate_Weekend,
-- DATEDIFF(days ,plannedstartdate_Weekend,plannedfinishdate_Weekend)/7 as IWP_Planned_Week_Diff
FROM {{ source('domain_integrated_model', 'transformed_iwps') }} iwp
WHERE exists (select * from   {{ source('domain_integrated_model', 'transformed_cwps') }} cwp where iwp.projectid=cwp.projectid and iwp.cwpsid=cwp.cwpsid)
),
change_history AS(
select 
projectid,entityname as iwpsid,fieldname,
row_number() over (partition by entityname order by  entityname ) row_id,

case when entityname='EUU00UGC08-EW003' then 
    case when row_id =1 then '2025-04-01' 
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
 from    {{ source('domain_integrated_model', 'transformed_change_history') }} ch
where entityname in ('EUU00UGC08-EW003' ,'EUU00UGC08-EW004','EUU00UGC16-EW004')
and  cast(ch.newvalue as decimal(9,2))>0 and Cast(ch.datecreated as date) is not null
order by iwpsid
),changhistory_grouped as 
(
select projectid,iwpsid,max(weekend) as weekend ,sum(newvalue)as ch_earned_hours from change_history a  
where cast(a.datecreated_derived as datetime)=(select max(cast(b.datecreated_derived as datetime)) from change_history b where a.projectid=b.projectid 
and a.iwpsid=b.iwpsid and a.weekend=b.weekend)
group by projectid,iwpsid
),fts_timesheet AS
(
     
select projectid,iwpsid,max(weekendingdate) as weekendingdate,sum(spenthours)+sum(overtimehours) spenthours from   {{ source('domain_integrated_model', 'transformed_fts_timesheet') }}
where iwpsid in ('EUU00UGC08-EW003' ,'EUU00UGC08-EW004','EUU00UGC16-EW004') ---remove
group by projectid,iwpsid
union all ---remove 
select '41','EUU00UGC08-EW003' ,'2025-04-06',100
union all
select '41','EUU00UGC16-EW009' ,'2025-05-11',50
union all
select '41','EUU00UGC08-EW004' ,'2025-05-25',60
union all
select '41','EUU00UGC16-EW004' ,'2025-06-01',100
union all
select '41','EUU00UGC08-EW006', '2025-04-20',110
union all
select '41','EUU00UGC16-EW010','2025-04-27',150
union all
select '41','EUU00UGC16-EW002','2025-05-04',210
union all
select '41','EUU00UGC08-EW005','2025-05-11',310
union all
select '41' ,'EUU00UGC16-EW008','2025-05-11',390
union all
select '41' ,'EUU00UGC16-EW001','2025-05-11', 500
union all
select '41' ,'EUU00UGC16-EW003', '2025-05-11', 800
union all
select '41' ,'EUU00UGC16-EW005', '2025-05-11', 999

)
select 
cast(iwp.projectid as varchar(100)) as project_id,
iwp.iwpsid,
iwp.cwpsid,
iwp.status,
cast(iwp.plannedstartdate as date) as plannedstartdate,
cast(iwp.plannedfinishdate as date) as plannedfinishdate ,
cast(iwp.actualstartdate as date) as actualstartdate ,
cast(iwp.actualfinishdate as date) as actualfinishdate ,
cast(iwp.PercentComplete as numeric(38,2)) as PercentComplete ,
cast(iwp.estimatedhours as numeric(38,2)) as  estimatedhours,
cast(iwp.earnedhours as numeric(38,2))  as iwp_earnedhours,
cast(fts.spenthours as numeric(38,2)) as spenthours,
disc.disciplinedescription,
cwa.cwasname,
cwa.cwasdescription,
CONCAT(iwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
cast(fts.weekendingdate as date) as weekendingdate, 
cast(ch.ch_earned_hours as numeric(38,2)) as ch_earnedhours,
cast(ch.weekend as date) as ch_weekend,


cast(Case when  EXTRACT(DOW FROM date (  cast( iwp.plannedstartdate as date)) ) = 0 then  cast(iwp.plannedstartdate as date)
                                      else cast(iwp.plannedstartdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedstartdate as date)) ) as INT)  end   as date) as plannedstartdate_Weekend,
cast(Case when  EXTRACT(DOW FROM date (  cast( iwp.plannedfinishdate as date)) ) = 0 then  cast(iwp.plannedfinishdate as date)
                                      else cast(iwp.plannedfinishdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.plannedfinishdate as date)) ) as INT)  end as date) as plannedfinishdate_Weekend,
cast(Case when  EXTRACT(DOW FROM date (  cast( iwp.actualstartdate as date)) ) = 0 then  cast(iwp.actualstartdate as date)
                                      else cast(iwp.actualstartdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.actualstartdate as date)) ) as INT)  end as date) as actualstartdate_Weekend,
cast(Case when  EXTRACT(DOW FROM date (  cast( iwp.actualfinishdate as date)) ) = 0 then  cast(iwp.actualfinishdate as date)
                                      else cast(iwp.actualfinishdate as date) + 7 -1 * cast(DATE_PART(dayofweek, date (cast(iwp.actualfinishdate as date)) ) as INT)  end as date) as actualfinishdate_Weekend,

DATEDIFF(days ,plannedstartdate_Weekend,plannedfinishdate_Weekend)/7 as IWP_Planned_Week_Diff,

DATEDIFF(days ,actualstartdate_Weekend,actualfinishdate_Weekend)/7 as IWP_actual_Week_Diff,

case when plannedfinishdate_Weekend = actualfinishdate_Weekend then 1 else 0 end as closed_in_same_week,

Case 
      when spenthours>iwp.estimatedhours and (iwp_earnedhours is not null and iwp_earnedhours > 0) then 'Over Estimate Duration'
      when spenthours<=iwp.estimatedhours and (iwp_earnedhours is not null and iwp_earnedhours > 0) then 'Within Estimate Duration'
      when iwp.status ='Open' and (iwp_earnedhours is null or iwp_earnedhours = 0) and spenthours > 0 then 'Spent No Earned'
      when iwp.status ='Closed' and  closed_in_same_week = 1 then 'Week Expected to finish' End  as Category_1,

Case 
      when iwp.status ='Closed' and  Category_1='Over Estimate Duration'  and  closed_in_same_week = 0
       and iwp_planned_week_diff > 6  then '> 6 weeks Over Estimated'
       when iwp.status ='Closed' and  Category_1='Over Estimate Duration'  and  closed_in_same_week = 0
       and iwp_planned_week_diff < 3  then '< 3 weeks Over Estimated'
       when iwp.status ='Closed' and  Category_1='Over Estimate Duration'  and  closed_in_same_week = 0
       and iwp_planned_week_diff > 3 and iwp_planned_week_diff < 6  then '3 to 6 weeks Over Estimated'

       when iwp.status ='Closed' and  Category_1='Within Estimate Duration'  and  closed_in_same_week = 0
       and iwp_planned_week_diff > 3  then '> 3 Weeks within Estimate'
      
       when iwp.status ='Closed' and  Category_1='Within Estimate Duration'  and  closed_in_same_week = 0
       and iwp_planned_week_diff < 3  then '< 3 Weeks within Estimate'

       when iwp.status ='Closed' and  closed_in_same_week = 1 then 'Week Expected to finish'
      End  as Category_2


from iwps iwp
left join fts_timesheet fts on iwp.projectid=fts.projectid and iwp.iwpsid=fts.iwpsid
left join changhistory_grouped ch on  iwp.projectid=ch.projectid and iwp.iwpsid=ch.iwpsid
left join {{ source('domain_integrated_model', 'transformed_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join {{ source('domain_integrated_model', 'transformed_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid

-- where iwp.iwpsid in ( 'EUU00UGC08-EW003',
-- 'EUU00UGC08-EW004',
-- 'EUU00UGC16-EW004',
-- 'EUU00UGC08-EW006',
-- 'EUU00UGC16-EW010',
-- 'EUU00UGC16-EW009',
-- 'EUU00UGC16-EW001',
-- 'EUU00UGC08-EW005',
-- 'EUU00UGC16-EW008',
-- 'EUU00UGC16-EW003',
-- 'EUU00UGC16-EW002',
-- 'EUU00UGC16-EW005'
-- ) 
