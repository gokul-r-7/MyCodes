{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

select 
 cast(cwp.projectid as varchar(100)) as project_id,
 cwp.cwpsid as cwpsid,
 iwp.iwpsid as iwpsid,
 disc.disciplinedescription,
 CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
 cast(iwp.plannedstartdate as date) as date_key,
 iwp.status,
 iwp.openconstraintcount,
'' as Lookahead,
     
    cast(iwp.BUDGETEDHOURS as numeric) ,
    cast(iwp.estimatedhours as numeric),
    cast(iwp.EARNEDHOURS as numeric),
    cast(iwp.REMAININGHOURS as numeric),
    cast(iwp.PERCENTCOMPLETE as numeric),
    iwp.iwpsdescription,
    iwp.PLANNERUSER,
    cast(iwp.plannedstartdate as date) as plannedstartdate ,
    cast(iwp.plannedfinishdate as date) as plannedfinishdate,
    cast(iwp.actualstartdate as date) as actualstartdate ,
    cast(iwp.actualfinishdate as date) as actualfinishdate,

    cwa.cwasname as cwa_constructionworkarea,
  
    Case when  iwp.status  in ( 'In development','Open','In Progress' ) and 
    (iwp.actualstartdate is not null)  and (iwp.actualfinishdate is null) 
    then DATEDIFF(day,cast(GETDATE() as date),cast(iwp.actualstartdate  as date))  end  as Open_Duration,
    iwp.PLANNEDDURATION,
    Case when  (iwp.actualstartdate is not null) and (iwp.actualfinishdate is not null)
    then iwp.ActualDuration end as Actual_Duration,

    Case when Open_Duration>iwp.PLANNEDDURATION then Open_Duration-iwp.PLANNEDDURATION end  as Delay,

    Delay/iwp.PLANNEDDURATION as Delay_Percent, 
    ct.constrainttype, cons.constraintid,cons._constraint,
    Case when cons.hardconstraint=true and (ct.constrainttype is not null and ct.constrainttype != '') then 1
    else NULL end as constrainttype_count,
    Case when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) <=0 then 'past'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 1 and 30 then '30'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 31 and 60 then '60'
         when DATEDIFF(day, cast(getdate()  as date),cast(iwp.plannedstartdate as date)) between 61 and 90 then '60'
         end  as Period_obt

from   {{ source('domain_integrated_model', 'transformed_cwps') }} cwp
inner join {{ source('domain_integrated_model', 'transformed_iwps') }} iwp on cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left join {{ source('domain_integrated_model', 'transformed_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid
left join  {{ source('domain_integrated_model', 'transformed_discipline') }}  disc on  iwp.disciplineid=disc.disciplineid
left join  {{ source('domain_integrated_model', 'transformed_constraints') }} cons on iwp.projectid=cons.projectid and iwp.iwpsid=cons.entityname
left join  {{ source('domain_integrated_model', 'transformed_constraint_type') }} ct on cons.constrainttypeid=ct.constrainttypeid
