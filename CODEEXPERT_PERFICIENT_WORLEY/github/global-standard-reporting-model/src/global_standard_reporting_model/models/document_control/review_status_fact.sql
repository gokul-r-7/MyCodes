{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}


WITH WF_action as
(
     SELECT *
    FROM {{ source('document_control_domain_integrated_model', 'workflow_action') }}
     Where workflow_status = 'In Progress'
    
)
,
EWP as
(
    Select  version_sid, package_name as ewp
    FROM     {{ source('document_control_domain_integrated_model', 'package_to_document') }}
    where package_type = 'Engineering Work Package'
)   
,
CWP as
(
    Select  version_sid, package_name as cwp
    FROM     {{ source('document_control_domain_integrated_model', 'package_to_document') }}
    where package_type = 'Construction Work Package'
)   
,
WBS as
(
    Select  version_sid, attribute_value as wbs
    FROM     {{ source('document_control_domain_integrated_model', 'custom_attribute') }}
    where normalized_attribute = 'Work Brakedown Structure'
)    
,Master as
(
  Select   document_number
  ,document_sid
     ,type_code 
     ,title
     ,asset_name
    ,category
    ,discipline_code
    ,discipline_name
    ,dim_project_rls_key  
    ,dim_project_name       
    FROM {{ source('document_control_domain_integrated_model', 'master') }}
)  
, Revision as
( 
    Select document_sid
      ,status_code
     ,status_name
      ,sub_project
      ,phase 
      ,revision_date
      ,revision_code
      ,current
      ,version_sid
      ,file_type
    FROM {{ source('document_control_domain_integrated_model', 'revision') }}
   where file_type = 'Primary' and current = 'true' 
   --and status_code not in ('SUP','VOD','Cancelled','No Longer In Use', 'Rejected', 'Working Document')
   and status_code in ('IFR','IFA')
)
 ,Consolidator as
(
    SELECT workflow_number
            ,MAX(action_status) as StepStatus
            ,Max(action_name) as StepName
           ,Max(assignee_name) as Consolidator
           ,Max(Cast(original_due_date as date)) as Planned         
           ,Max(Cast (current_due_date as date)) as Forecast 
           ,Max(Cast (date_completed as date)) as Actual
          ,Max(Cast (dim_snapshot_date as date)) as snapshot_date
          ,max(review_day_late) as wf_daysdue
    FROM {{ source('document_control_domain_integrated_model', 'workflow_action') }}
    WHERE action_name like  'Consolidator' 
    Group By workflow_number
)
,Reviewer as
(
     SELECT Distinct workflow_number
            ,MAX(action_status) as StepStatus
            ,action_name as StepName
           ,assignee_name as Reviewer
           ,assignee_organization as reviewer_organization_name
           ,Max(Cast(original_due_date as date)) as Planned         
           ,Max(Cast (current_due_date as date)) as Forecast 
           ,Max(Cast (date_completed as date)) as Actual
          ,Max(Cast (dim_snapshot_date as date)) as snapshot_date
          ,max(review_day_late) as a_daysdue
    FROM {{ source('document_control_domain_integrated_model', 'workflow_action') }}
    Group By workflow_number,action_name ,assignee_name ,assignee_organization
)
SELECT DISTINCT
     Master.document_number
    ,Master.type_code 
    ,Master.category
    ,WF_action.reason_for_issue
    ,WF_action.document_sid
    ,Revision.status_code
    ,Revision.status_name
    ,WF_action.Workflow_number as WorkflowID
    ,WF_action.workflow_title
    ,WF_action.workflow_status AS WF_Status
    ,Consolidator.Consolidator
    ,Consolidator.Planned AS WF_Date_Planned
    ,Consolidator.Forecast AS WF_Date_Forecast
    ,Consolidator.Actual AS WF_Date_Actual
    ,Reviewer.StepStatus
    ,Consolidator.wf_daysdue
    ,Revision.current as current_flag
    ,Master.discipline_code
    ,Master.discipline_name
    ,Master.title 
    ,WF_action.revision_code
    ,Revision.revision_date
    ,Master.asset_name as asset
    ,Revision.sub_project
    ,Revision.phase 
    ,EWP.ewp as ewp
       ,CWP.cwp as cwp
       ,WBS.wbs as wbs
    ,case when Master.category = 'Drawing' then 'Yes' else 'No' end as is_drawing
    ,Case when Reviewer.a_daysdue <= '0' then 'In Progress' 
          When Reviewer.a_daysdue > '0' and Reviewer.a_daysdue <= '5' then '0-5 Days Late'
          When Reviewer.a_daysdue > '5' and Reviewer.a_daysdue <= '15' then '5-15 Days Late'
          When Reviewer.a_daysdue > '15' and Reviewer.a_daysdue <= '25' then '15-25 Days Late'
        When Reviewer.a_daysdue > '25' then '25+ Days Late'
    else null end as workflow_kpi_status
    ,Case when Reviewer.a_daysdue <= '0' then 1 
          When Reviewer.a_daysdue > '0' and Reviewer.a_daysdue <= '5' then 2
          When Reviewer.a_daysdue > '5' and Reviewer.a_daysdue <= '15' then 3
          When Reviewer.a_daysdue > '15' and Reviewer.a_daysdue <= '25' then 4
        When Reviewer.a_daysdue > '25' then 5
    else null end as workflow_kpi_status_order
    ,Case when Reviewer.a_daysdue <= '0' then 'Not Overdue' 
        when Reviewer.a_daysdue > '0' then 'Overdue' else null end as actionee_kpi_status
    ,Reviewer.Reviewer as Actionee
    ,Reviewer.reviewer_organization_name as organization_name
    ,Reviewer.Planned AS A_Date_Planned
    ,Reviewer.Forecast AS A_Date_Forecast
    ,Reviewer.Actual AS A_Date_Actual
    ,Reviewer. a_daysdue 
    ,WF_action.dbt_ingestion_date AS snapshot_date
    ,Master.dim_project_name + ' - '  + 'Engineering Document Register' as report_title
    ,Master.dim_project_rls_key as project_id 
    ,Master.dim_project_name as meta_project_rls_name
FROM WF_action
Left Join Consolidator on Wf_Action.workflow_number = Consolidator.workflow_number
Left Join Reviewer on Wf_Action.workflow_number = Reviewer.workflow_number
Left join  Revision on revision.document_sid = WF_action.document_sid and revision.revision_code = WF_action.revision_code
Left Join Master on Master.document_sid = WF_action.document_sid
Left Join EWP on EWP.version_sid = Revision.version_sid
Left Join CWP on CWP.version_sid = Revision.version_sid
Left Join WBS on WBS.version_sid = Revision.version_sid