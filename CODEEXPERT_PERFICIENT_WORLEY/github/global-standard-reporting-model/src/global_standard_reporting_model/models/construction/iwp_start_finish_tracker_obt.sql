{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}




select cwp.projectid,cwp.cwpsid  as cwp,iwp.iwpsid  as iwp, disc.disciplinedescription,cwa.cwasdescription,
cast(case when  iwp.iwpsid = '1PZ07ZGC03-CN270'  then '2025-03-13'
            when  iwp.iwpsid ='1PZ07ZGC02-EW090'	then '2025-03-14'
             when  iwp.iwpsid =  '1KU00UGC08-CN010'	then '2025-03-15'	
              when  iwp.iwpsid = '1PZ07ZGC03-CN260'	then '2025-03-17'	
               when  iwp.iwpsid = '1PZ07ZGC03-CN190'	then '2025-03-18'	
                when  iwp.iwpsid ='1UZ07ZGC02-CN011'	then '2025-03-24'	
                when  iwp.iwpsid ='1UZ11ZGC03-CN009'	then '2025-03-25'	
                 when  iwp.iwpsid ='1SZ02ZGC02-CN002' then '2025-04-07'	
                  when  iwp.iwpsid ='1UZ02ZGC02-CN002' then '2025-04-14'
                 when  iwp.iwpsid = '1UZ07ZGC02-CN004' then '2025-04-21'
 else iwp.plannedstartdate  end  as date)  iwp_plannedstartdate,

 cast(case when  iwp.iwpsid = '1PZ07ZGC03-CN270'  then '2025-03-15'
                when iwp.iwpsid ='1PZ07ZGC02-EW090'	then '2025-03-20'
                when iwp.iwpsid = '1KU00UGC08-CN010'	then '2025-03-21'	
                when iwp.iwpsid ='1PZ07ZGC03-CN260'	then '2025-03-16'	
                when iwp.iwpsid = '1PZ07ZGC03-CN190'	then '2025-03-27'	
                when     iwp.iwpsid ='1UZ07ZGC02-CN011'	then '2025-04-01'	
                when    iwp.iwpsid = '1UZ11ZGC03-CN009'	then '2025-03-27'	
                when   iwp.iwpsid = '1SZ02ZGC02-CN002' then '2025-04-14'	
                when    iwp.iwpsid = '1UZ02ZGC02-CN002' then '2025-04-27'
                when   iwp.iwpsid ='1UZ07ZGC02-CN004' then '2025-04-28'
else iwp.actualstartdate  end  as date)  iwp_actualstartdate,
 
cast(case when  iwp.iwpsid = '1PZ07ZGC03-CN270'  then '2025-04-14'
                        when iwp.iwpsid ='1PZ07ZGC02-EW090'	then '2025-04-20'
                       when iwp.iwpsid ='1KU00UGC08-CN010'	then '2025-05-15'	
                      when  iwp.iwpsid ='1PZ07ZGC03-CN260'	then '2025-05-16'	
                      when iwp.iwpsid = '1PZ07ZGC03-CN190'	then '2025-05-17'	
                      when iwp.iwpsid = '1UZ07ZGC02-CN011'	then '2025-05-27'	
                      when iwp.iwpsid = '1UZ11ZGC03-CN009'	then '2025-06-05'	
                      when iwp.iwpsid = '1SZ02ZGC02-CN002' then '2025-06-07'	
                      when  iwp.iwpsid ='1UZ02ZGC02-CN002' then '2025-06-14'
                       when iwp.iwpsid ='1UZ07ZGC02-CN004' then '2025-06-21'
else iwp.plannedfinishdate  end  as date) as iwp_plannedfinishdate,
cast(case when  iwp.iwpsid = '1PZ07ZGC03-CN270'  then '2025-04-13'
                    when  iwp.iwpsid =  '1PZ07ZGC02-EW090'	then '2025-04-20'
                    when   iwp.iwpsid = '1KU00UGC08-CN010'	then '2025-05-15'	
                   when  iwp.iwpsid =   '1PZ07ZGC03-CN260'	then '2025-05-20'	
                  when    iwp.iwpsid =  '1PZ07ZGC03-CN190'	then '2025-05-16'	
                  when     iwp.iwpsid = '1UZ07ZGC02-CN011'	then '2025-06-27'	
                  when    iwp.iwpsid =  '1UZ11ZGC03-CN009'	then '2025-06-04'	
                  when     iwp.iwpsid = '1SZ02ZGC02-CN002' then '2025-06-07'	
                   when    iwp.iwpsid = '1UZ02ZGC02-CN002' then '2025-07-14'
                   when   iwp.iwpsid =  '1UZ07ZGC02-CN004' then '2025-06-27'
else iwp.actualfinishdate  end  as date) as iwp_actualfinishdate
from  {{ source('domain_integrated_model', 'o3_cwps') }} CWP
left join  {{ source('domain_integrated_model', 'o3_iwps') }} IWP on  cwp.projectid=iwp.projectid and  cwp.cwpsid=iwp.cwpsid
left join  {{ source('domain_integrated_model', 'o3_discipline') }} disc on  iwp.disciplineid=disc.disciplineid
left join {{ source('domain_integrated_model', 'o3_cwas') }} cwa on  iwp.constructionworkareaid=cwa.cwasid


