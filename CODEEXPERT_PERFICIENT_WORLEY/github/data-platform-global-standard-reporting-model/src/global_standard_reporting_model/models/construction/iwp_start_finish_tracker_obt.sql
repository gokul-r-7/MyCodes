{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

select 
cast(cwp.projectid as varchar(100)) as project_id,
cwp.cwpsid  as cwp,
iwp.iwpsid  as iwp, 
disc.disciplinedescription,
CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
cwa.cwasdescription,
cast(case   when  iwp.iwpsid ='1LU00UGC07-CN07F' then '2025-03-13'
            when  iwp.iwpsid ='1LU00UGC07-CN02G' then '2025-03-14'
            when  iwp.iwpsid ='1KU00UGC08-CN010' then '2025-03-15'	
            when  iwp.iwpsid ='1LZ08ZGC03-CN080' then '2025-03-17'	
            when  iwp.iwpsid ='1PZ07ZGC03-CN190' then '2025-03-18'	
            when  iwp.iwpsid ='1LZ08ZGC03-CN140' then '2025-03-24'	
            when  iwp.iwpsid ='1BZ01ZGS01-SS008' then '2025-03-25'	
            when  iwp.iwpsid ='2EM19HPP01-PI001' then '2025-04-07'	
            when  iwp.iwpsid ='1MZ04ZGM02-ME043' then '2025-04-14'
            when  iwp.iwpsid ='1BZ01ZGS01-SS045' then '2025-04-21'
 else iwp.plannedstartdate  end  as date)  iwp_plannedstartdate,

 cast(case      when iwp.iwpsid = '1LU00UGC07-CN07F' then '2025-03-15'
                when iwp.iwpsid ='1LU00UGC07-CN02G'	then '2025-03-20'
                when iwp.iwpsid = '1KU00UGC08-CN010'then '2025-03-21'	
                when iwp.iwpsid ='1LZ08ZGC03-CN080'	then '2025-03-16'	
                when iwp.iwpsid = '1PZ07ZGC03-CN190'then '2025-03-27'	
                when iwp.iwpsid ='1LZ08ZGC03-CN140'	then '2025-04-01'	
                when iwp.iwpsid = '1BZ01ZGS01-SS008'then '2025-03-27'	
                when iwp.iwpsid = '1SZ02ZGC02-CN002'then '2025-04-14'	
                when iwp.iwpsid = '1MZ04ZGM02-ME043'then '2025-04-27'
                when iwp.iwpsid ='1BZ01ZGS01-SS045' then '2025-04-28'
else iwp.actualstartdate  end  as date)  iwp_actualstartdate,
 
cast(case when  iwp.iwpsid = '1LU00UGC07-CN07F'  then '2025-04-14'
                    when iwp.iwpsid ='1LU00UGC07-CN02G'	then '2025-04-20'
                    when iwp.iwpsid ='1KU00UGC08-CN010'	then '2025-05-15'	
                    when iwp.iwpsid ='1LZ08ZGC03-CN080'	then '2025-05-16'	
                    when iwp.iwpsid = '1PZ07ZGC03-CN190' then '2025-05-17'	
                    when iwp.iwpsid = '1LZ08ZGC03-CN140' then '2025-05-27'	
                    when iwp.iwpsid = '1BZ01ZGS01-SS008' then '2025-06-05'	
                    when iwp.iwpsid = '1SZ02ZGC02-CN002' then '2025-06-07'	
                    when iwp.iwpsid ='1MZ04ZGM02-ME043' then '2025-06-14'
                    when iwp.iwpsid ='1BZ01ZGS01-SS045' then '2025-06-21'
else iwp.plannedfinishdate  end  as date) as iwp_plannedfinishdate,

cast(case when  iwp.iwpsid = '1LU00UGC07-CN07F'  then '2025-04-13'
                    when  iwp.iwpsid =  '1LU00UGC07-CN02G'	then '2025-04-20'
                    when   iwp.iwpsid = '1KU00UGC08-CN010'	then '2025-05-15'	
                    when  iwp.iwpsid =   '1LZ08ZGC03-CN080'	then '2025-05-20'	
                    when    iwp.iwpsid =  '1PZ07ZGC03-CN190'	then '2025-05-16'	
                    when     iwp.iwpsid = '1LZ08ZGC03-CN140'	then '2025-06-27'	
                    when    iwp.iwpsid =  '1BZ01ZGS01-SS008'	then '2025-06-04'	
                    when     iwp.iwpsid = '1SZ02ZGC02-CN002' then '2025-06-07'	
                    when    iwp.iwpsid = '1MZ04ZGM02-ME043' then '2025-07-14'
                    when   iwp.iwpsid =  '1BZ01ZGS01-SS045' then '2025-06-27'
else iwp.actualfinishdate  end  as date) as iwp_actualfinishdate
from  {{ source('domain_integrated_model', 'transformed_cwps') }} CWP
left join  {{ source('domain_integrated_model', 'transformed_iwps') }} IWP on  cwp.projectid=iwp.projectid and  cwp.cwpsid=iwp.cwpsid
left join  {{ source('domain_integrated_model', 'transformed_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join {{ source('domain_integrated_model', 'transformed_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid