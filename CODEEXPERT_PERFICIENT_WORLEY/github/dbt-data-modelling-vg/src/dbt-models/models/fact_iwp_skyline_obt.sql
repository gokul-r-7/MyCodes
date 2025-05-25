{{ config(materialized='table') }}


select 
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
iwp.plannedstartdate as iwp_plannedstartdate,
iwp.plannedfinishdate as iwp_plannedfinishdate,
iwp.actualstartdate as  iwp_actualstartdate,
iwp.actualfinishdate as iwp_actualfinishdate,
iwp.crewsize as crewsize,
iwp.iwp_manual_plannedstartdate,  --- Manually added
iwp.iwp_manual_plannedfinishdate, --- Manually added
dimdate.date_key,
dimdate.date_key + 7 -1 * cast(DATE_PART(dayofweek, date (dimdate.date_key) ) as INT) as weekend,
iwp.status as iwp_status,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
cwa.constructionworkarea as cwa_constructionworkarea,
(select count(1) from  {{ref('fact_o3_constraints')}} a 
 where a.entitytypeid= 4 and  iwp.iwpsid=a.entityname)  as contraintcnt,
case when 
(select count(1) from  {{ref('fact_o3_constraints')}} a 
 inner join {{ref('dim_constraint_type')}} b on a.constrainttypeid=b.constrainttypeid
 where a.entitytypeid= 4 and b.constrainttype='Material' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as MaterialContraint,
case when 
(select count(1) from  {{ref('fact_o3_constraints')}} a 
 inner join {{ref('dim_constraint_type')}}  b on a.constrainttypeid=b.constrainttypeid
 where a.entitytypeid= 4 and b.constrainttype='Drawing' and iwp.iwpsid=a.entityname) >0 then 'Y' else 'N' end as DrawingContraint,
--CONSTRAINTTYPE
iwp.disciplineid as iwp_disciplineid ,
disc.disciplinedescription as disc_disciplinedescription
 
from {{ref('fact_o3_cwps')}} cwp
---inner join vg_analytics. fact_o3_IWPS iwp on cwp.cwpsid=iwp.cwpsid
inner join 
(select *, case when ID between 1 and 10 then '2023-01-01' 
               when ID between 11 and 20 then '2023-02-05' 
               when ID between 21 and 30 then '2023-02-15'   
               when ID between 31 and 40 then '2023-03-03'       
               when ID between 41 and 50 then '2024-01-01' 
               when ID between 51 and 60 then '2024-01-16' 
               when ID between 61 and 70 then '2024-02-09'    
               when ID between 71 and 90 then '2024-03-15'     
               when ID between 91 and 95 then '2024-04-10'  end  as iwp_manual_plannedstartdate ,
 
               case when ID between 1 and 10 then '2023-01-09' 
               when ID between 11 and 20 then '2023-02-15' 
               when ID between 21 and 30 then '2023-02-25'   
               when ID between 31 and 40 then '2023-03-10'       
               when ID between 41 and 50 then '2024-01-15' 
               when ID between 51 and 60 then '2024-01-26' 
               when ID between 61 and 70 then '2024-02-20'    
               when ID between 71 and 90 then '2024-03-21'     
               when ID between 91 and 95 then '2024-04-15'  end  as iwp_manual_plannedfinishdate    
from 
(
select  row_number() over (Order by iwpsid ) ID,*  from   {{ref('fact_o3_iwps')}} a 
where exists (select * from  {{ref('fact_o3_cwps')}} b where a.cwpsid=b.cwpsid)
) iwp) iwp on cwp.cwpsid=iwp.cwpsid
 
full join {{ref('dim_construction_work_area')}} cwa on  iwp.constructionworkareaid=cwa.constructionworkareaid
full join {{ref('dim_discipline')}}  disc on  iwp.disciplineid=disc.disciplineid
full join {{ref('dim_p6_date')}}  dimdate on iwp.iwp_manual_plannedstartdate = dimdate.date_key