{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

select 
iwp.iwpsid,
iwp.projectid as project_id,
iwp.status as iwp_status,
iwp.iwpsdescription as iwp_description,
iwp.disciplineid as iwp_disciplineid,
disc.disciplinedescription as disc_disciplinedescription,
iwp.updated_date as iwp_updated_date,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
cwa.cwasname as cwa_constructionworkarea,
iwp.budgetedhours as budgetedhours,
iwp.remaininghours as remaininghours,
iwp.estimatedhours as estimatedhours,
iwp.earnedhours as earnedhours,
iwp.actualfinishdate as actualfinishdate,
cast(iwp.plannedstartdate as DATE) as iwp_plannedstartdate,
cast(iwp.plannedfinishdate as DATE) as iwp_plannedfinishdate,

iwp.contract as cwp_contract,
CONCAT(cwp_contract,CONCAT(' ',disc_disciplinedescription)) AS scope_discipline,

case 
when iwp.iwpsid is not null then datediff(days,iwp_plannedstartdate,iwp_plannedfinishdate)
else null
end as datedifference,

getdate() as execution_date

from   
(select  * from {{ source('domain_integrated_model', 'o3_iwps') }} a 
where exists (select * from  {{ source('domain_integrated_model', 'o3_cwps') }} b where a.cwpsid=b.cwpsid)) iwp
left join  {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
where iwp.projectid = 41
