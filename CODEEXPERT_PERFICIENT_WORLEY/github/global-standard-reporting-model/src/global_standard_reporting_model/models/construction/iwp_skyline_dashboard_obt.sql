{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

select  * ,
case when (iwpsid is null and date_key is null) or (date_key is null) then null else
Row_number() over (Partition by date_key order by iwpsid) end as Row_ID,

case when (iwpsid is null and finish_date_key is null) or (finish_date_key is null) then null else
Row_number() over (Partition by finish_date_key order by iwpsid) end as finish_Row_ID 


from 
(

select 
iwp.projectid as project_id,
cwp.cwpsid,
cwpsdescription,
cwp.entityid as cwp_entityid,
cwp.entitytypeid as cwp_entitytypeid,
iwp.iwpsid,
iwp.iwpsdescription,
disc.disciplinedescription as iwp_discipline_discription,
iwp.estimatedhours as estimatedhours,
iwp.remaininghours as remaininghours,
iwp.openconstraintcount as openconstraintcount,
iwp.criticalpath as criticalpath,
iwp.plannedstartdate as iwp_manual_plannedstartdate,
iwp.plannedfinishdate as iwp_manual_plannedfinishdate,
iwp.actualstartdate as  iwp_actualstartdate,
iwp.actualfinishdate as iwp_actualfinishdate,
iwp.crewsize as crewsize, 
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
             iwp.status not in ( 'In development','Closed','Completed','Sign-off' ) and  iwp.openconstraintcount= 0 )   then 'backlog'

    when iwp.iwpsid is not null and iwp.status = 'Completed' then 'Completed'

    when iwp.iwpsid is not null and  iwp.status='Issued to Field' then 'Issued to Field'
    else 'NA'
    end 
    as Lookahead ,

dimdate.date_key,
dimdate_finish.date_key as finish_date_key,
dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) as weekend,
dimdate_finish.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate_finish.date_key) ) as INT) as finish_weekend,

iwp.status as iwp_status,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
cwa.cwasname as cwa_constructionworkarea,
Case when 
iwp.iwpsid  NOTNULL then  
case when 
(select count(1) from  {{ source('domain_integrated_model', 'o3_constraints') }} a 
 where a.entitytypeid= 4 and status='Open' and  iwp.iwpsid=a.entityname) > 0 then 1 else 0 end
end  as HasOpenConstraint,
(select count(1) from  {{ source('domain_integrated_model', 'o3_constraints') }} a 
 where a.entitytypeid= 4 and  iwp.iwpsid=a.entityname)  as contraintcnt,
case when 
(select count(1) from  {{ source('domain_integrated_model', 'o3_constraints') }} a 
 inner join {{ source('domain_integrated_model', 'o3_constraint_type') }} b on a.constrainttypeid=b.constrainttypeid
 where a.entitytypeid= 4 and b.constrainttype='Material' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as MaterialContraint,
case when 
(select count(1) from {{ source('domain_integrated_model', 'o3_constraints') }} a 
 inner join {{ source('domain_integrated_model', 'o3_constraint_type') }} b on a.constrainttypeid=b.constrainttypeid
 where a.entitytypeid= 4 and b.constrainttype='Drawing' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as DrawingContraint,
iwp.disciplineid as iwp_disciplineid ,
disc.disciplinedescription as disc_disciplinedescription,
iwp.updated_date as iwp_updated_date,
getdate() as execution_date

from   {{ source('domain_integrated_model', 'o3_cwps') }} cwp
inner join 
{{ source('domain_integrated_model', 'o3_iwps') }} iwp on cwp.cwpsid=iwp.cwpsid

left join  {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join  {{ source('global_standard_model', 'dim_date') }} dimdate on iwp.plannedstartdate = dimdate.date_key
left join  {{ source('global_standard_model', 'dim_date') }} dimdate_finish on iwp.plannedfinishdate = dimdate_finish.date_key
) Z where project_id = 41