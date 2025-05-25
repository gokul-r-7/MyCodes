{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}


select 
 cwp.projectid as project_id,
 cwp.cwpsid as cwpsid,
 iwp.iwpsid as iwpsid,
 disc.disciplinedescription,
 CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
 dimdate.date_key,
 iwp.status,
 iwp.openconstraintcount,
CASE
    when iwp.iwpsid is not null and  ( CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) >
         dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT)  and 
         iwp.status <> 'Issued'  ) then 'Late'
     
     when  iwp.iwpsid is not null and 
       CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) =
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) then 'Week 0'
    when iwp.iwpsid is not null and 
       CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 7 =
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) then 'Week 1'
    when iwp.iwpsid is not null and 
       CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 14 =
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) then 'Week 2'
    when iwp.iwpsid is not null and 
       CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 21 =
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) then 'Week 3'
    when iwp.iwpsid is not null and 
       CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 28 =
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) then 'Week 4'
   
    when  iwp.iwpsid is not null and   (
            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT)  and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 7 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 14 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 
           
            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 21 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 28 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 
             iwp.status = 'In Development' )  then 'In Development' 
  
     when iwp.iwpsid is not null and   (
            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT)  and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 7 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 14 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 
           
            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 21 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 

            CAST( GETDATE() + 7 - 1 *  cast(DATE_PART( dayofweek , date (GETDATE()) ) as INT) AS DATE) + 28 <>
            dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) and 
             iwp.status not in ( 'In development','Closed','Completed','Sign-off' ) and  iwp.openconstraintcount= 0 )   then 'Backlog'

    when iwp.iwpsid is not null and iwp.status = 'Completed' then 'Completed'

    when iwp.iwpsid is not null and  iwp.status='Issued to Field' then 'Issued to Field'
    else 'NA' end as Lookahead,
     
    iwp.BUDGETEDHOURS  ,
    iwp.estimatedhours,
    iwp.EARNEDHOURS,
    iwp.REMAININGHOURS,
    iwp.PERCENTCOMPLETE ,
    iwp.iwpsdescription,
    iwp.PLANNERUSER,
    iwp.plannedstartdate,
    iwp.plannedfinishdate,
    iwp.actualstartdate,
    iwp.actualfinishdate,

    cwa.cwasname as cwa_constructionworkarea,
  
    Case when  iwp.status  in ( 'In development','Open','In Progress' ) and 
    (iwp.actualstartdate is not null and iwp.actualstartdate<>'')  and (iwp.actualfinishdate is null or iwp.actualfinishdate='') 
    then DATEDIFF(day,cast(GETDATE() as date),cast(iwp.actualstartdate  as date))  end  as Open_Duration,
    iwp.PLANNEDDURATION,
    Case when  (iwp.actualstartdate is not null and iwp.actualstartdate<>'') and (iwp.actualfinishdate is not null and iwp.actualfinishdate<>'')
    then iwp.ActualDuration end as Actual_Duration,

    Case when Open_Duration>iwp.PLANNEDDURATION then Open_Duration-iwp.PLANNEDDURATION end  as Delay,

    Delay/iwp.PLANNEDDURATION as Delay_Percent, 
    ct.constrainttype, cons.constraintid,cons._constraint,
    Case when cons.hardconstraint='true' and (ct.constrainttype is not null and ct.constrainttype != '') then 1
    else NULL end as constrainttype_count,
    Case when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) <=0 then 'past'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 1 and 30 then '30'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 31 and 60 then '60'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 61 and 90 then '60'
         end  as Period_obt

         
from   {{ source('domain_integrated_model', 'o3_cwps') }} cwp
inner join {{ source('domain_integrated_model', 'o3_iwps') }} iwp on cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left join {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join  {{ source('global_standard_model', 'dim_date') }} dimdate on iwp.plannedstartdate = dimdate.date_key
left join  {{ source('domain_integrated_model', 'o3_constraints') }} cons on iwp.projectid=cons.projectid and iwp.iwpsid=cons.entityname
left join  {{ source('domain_integrated_model', 'o3_constraint_type') }} ct on cons.constrainttypeid=ct.constrainttypeid
