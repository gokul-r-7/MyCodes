{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}


select 
cast(iwp.projectid as varchar(100)) as project_id,
cwp.cwpsid,
cwpsdescription,
cwp.entityid as cwp_entityid,
cwp.entitytypeid as cwp_entitytypeid,
iwp.iwpsid,
iwp.iwpsdescription,
disc.disciplinedescription as iwp_discipline_discription,
cast(iwp.estimatedhours as numeric(38,2)) as estimatedhours,
cast(iwp.remaininghours as numeric(38,2)) as remaininghours,
iwp.openconstraintcount as openconstraintcount,
iwp.criticalpath as criticalpath,
cast(iwp.plannedstartdate as date) as iwp_manual_plannedstartdate,
cast(iwp.plannedfinishdate as date) as iwp_manual_plannedfinishdate,
cast(iwp.actualstartdate as date) as  iwp_actualstartdate,
cast(iwp.actualfinishdate  as date)as iwp_actualfinishdate,
crewsize,
cwp.contract as cwp_contract,
CONCAT(cwp_contract,CONCAT(' ',iwp_discipline_discription)) AS scope_discipline,

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
    else 'NA'
    end 
    as Lookahead ,
cast( dimdate.date_key as date) as date_key ,
cast(dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) as date) as weekend,
iwp.status as iwp_status,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
cwa.cwasname as cwa_constructionworkarea,
Case when 
iwp.iwpsid  IS NOT NULL then  
case when 
(select count(1) from  {{ source('domain_integrated_model', 'transformed_constraints') }} a 
 where a.entitytypeid= 4 and status='Open' and  iwp.iwpsid=a.entityname) > 0 then 1 else 0 end
end  as HasOpenConstraint,
(select count(1) from {{ source('domain_integrated_model', 'transformed_constraints') }} a 
 where a.entitytypeid= 4 and  iwp.iwpsid=a.entityname)  as contraintcnt,
case when 
(select count(1) from  {{ source('domain_integrated_model', 'transformed_constraints') }} a 
 where a.entitytypeid= 4 and a.constrainttype='Material' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as MaterialContraint,
case when 
(select count(1) from {{ source('domain_integrated_model', 'transformed_constraints') }} a 
 where a.entitytypeid= 4 and a.constrainttype='Drawing' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as DrawingContraint,
iwp.disciplineid as iwp_disciplineid ,
disc.disciplinedescription as disc_disciplinedescription,
cast(iwp.model_updated_date as date) as iwp_updated_date,
cast(getdate() as date) as execution_date
from   {{ source('domain_integrated_model', 'transformed_cwps') }} cwp
inner join  {{ source('domain_integrated_model', 'transformed_iwps') }} iwp on cwp.cwpsid=iwp.cwpsid
left join  {{ source('domain_integrated_model', 'transformed_cwas') }}  cwa on  iwp.constructionworkareaid=cwa.cwasid
left join {{ source('domain_integrated_model', 'transformed_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join    {{ source("global_standard_model","dim_date_new1" ) }} dimdate on iwp.plannedstartdate = dimdate.date_key
where iwp.projectid=41
