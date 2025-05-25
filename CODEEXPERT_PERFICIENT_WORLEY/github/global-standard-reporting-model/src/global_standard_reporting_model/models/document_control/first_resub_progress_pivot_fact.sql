{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}


Select 
weekend_date,document_sid
,'planned' as attribute
,planned_count as Total_count
,submittal_status
,SUM(planned_count) over ( partition by submittal_status order by weekend_date rows unbounded preceding) as cumulative
From
(
    SELECT  value as weekend_date  
    ,document_sid  
    ,count (distinct document_sid) as planned_count
     ,submittal_status 
     from 
     (   
         select document_sid
         , snapshot_date
         , submittal_status
         , 'planned' as attribute
         , planned as value 
         from(   
             Select document_sid    
             ,planned_submission_date_weekend as planned    
             ,Case When submittal_status = 'First Submittal' then date_submitted_weekend          
                   When submittal_status = 'Total Resubmitted'then date_submitted_weekend              
                   else null end as actual    
                   ,snapshot_date    
                   ,submittal_status   
                   From {{ ref('supplier_submittal_fact') }}
                   where submittal_status in ('First Submittal','Total Resubmitted') --and document_sid = 'aconex|1353331688148364949'
                   )
                   )
                   group by value,submittal_status,document_sid
                   )

                   Union ALL

                  Select 
                      weekend_date,document_sid
                     ,'actual' as attribute
                     ,actual_count as Total_count
                     ,submittal_status
                     ,SUM(actual_count) over ( partition by submittal_status order by weekend_date rows unbounded preceding) as cumulative
                    From
                     (
                            SELECT  value as weekend_date  
                            ,document_sid  
                            ,count (distinct document_sid) as actual_count 
                           ,submittal_status 
                            from 
                           (   
                              select document_sid
                              , snapshot_date
                              , submittal_status
                              , 'actual' as attribute
                              , actual as value 
                              from(   
                                    Select document_sid        
                                     ,Case When submittal_status = 'First Submittal' then date_submitted_weekend          
                                     When submittal_status = 'Total Resubmitted'then date_submitted_weekend              
                                     else null end as actual    
                                     ,snapshot_date    
                                     ,submittal_status   
                                     From {{ ref('supplier_submittal_fact') }}
                                     where submittal_status in ('First Submittal','Total Resubmitted') --and document_sid = 'aconex|1353331688148364949'
                                            )
                                             )
                                             group by value,submittal_status,document_sid
                                              )      
