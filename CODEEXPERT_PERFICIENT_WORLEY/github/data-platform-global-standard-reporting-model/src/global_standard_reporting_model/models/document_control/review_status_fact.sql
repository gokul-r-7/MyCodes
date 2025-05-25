{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}


WITH WF_action as
(
    SELECT *
    FROM {{ source('document_control_domain_integrated_model', 'transformed_workflow_action') }}
    --Where workflow_status = 'In Progress'
    Where workflow_status in  ('In Progress','Completed')

    
)
,
EWP as
(
    Select  version_sid, package_name as ewp,dim_project_rls_key
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_package_to_document') }}
    where package_type = 'Engineering Work Package'
)   
,
CWP as
(
    Select  version_sid, package_name as cwp,dim_project_rls_key
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_package_to_document') }}
    where package_type = 'Construction Work Package'
)   
,
WBS as
(
    Select  version_sid, attribute_value as wbs,dim_project_rls_key
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_custom_attribute') }}
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
    FROM {{ source('document_control_domain_integrated_model', 'transformed_master') }}
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
      ,confidential
      ,current
      ,version_sid
      ,file_type,
      dim_project_rls_key
    FROM {{ source('document_control_domain_integrated_model', 'transformed_revision') }}
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
          ,dim_project_rls_key
    FROM {{ source('document_control_domain_integrated_model', 'transformed_workflow_action') }}
    WHERE action_name like  'Consolidator' 
    Group By workflow_number,dim_project_rls_key
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
          ,dim_project_rls_key
    FROM {{ source('document_control_domain_integrated_model', 'transformed_workflow_action') }}
    Group By workflow_number,action_name ,assignee_name ,assignee_organization,dim_project_rls_key
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
    ,cast(Revision.revision_date as timestamp) as revision_date
    ,Revision.confidential
    ,Master.asset_name as asset
    ,Revision.sub_project
    ,Revision.phase 
    ,EWP.ewp as ewp
       ,CWP.cwp as cwp
       ,WBS.wbs as wbs
    ,case when Master.category = 'Drawing' then 'Yes' else 'No' end as is_drawing
    ,Case when cast(Reviewer.a_daysdue as INT) <= 0 then 'In Progress' 
          When cast(Reviewer.a_daysdue as INT) > 0 and cast(Reviewer.a_daysdue as INT) <= 5 then '0-5 Days Late'
          When cast(Reviewer.a_daysdue as INT) > 5 and cast(Reviewer.a_daysdue as INT) <= 15 then '6-15 Days Late'
          When cast(Reviewer.a_daysdue as INT) > 15 and cast(Reviewer.a_daysdue as INT) <= 25 then '16-25 Days Late'
        When cast(Reviewer.a_daysdue as INT) > 25 then '25+ Days Late'
    else null end as workflow_kpi_status
    ,Case when cast(Reviewer.a_daysdue as INT) <= 0 then 1 
          When cast(Reviewer.a_daysdue as INT) > 0 and cast(Reviewer.a_daysdue as INT) <= 5 then 2
          When cast(Reviewer.a_daysdue as INT) > 5 and cast(Reviewer.a_daysdue as INT) <= 15 then 3
          When cast(Reviewer.a_daysdue as INT) > 15 and cast(Reviewer.a_daysdue as INT) <= 25 then 4
        When cast(Reviewer.a_daysdue as INT) > 25 then 5
    else null end as workflow_kpi_status_order
    ,Case when cast(Reviewer.a_daysdue as INT) <= 0 then 'Not Overdue' 
        when cast(Reviewer.a_daysdue as INT) > 0 then 'Overdue' else null end as actionee_kpi_status
    ,Reviewer.Reviewer as Actionee
    ,Reviewer.reviewer_organization_name as organization_name
    ,Reviewer.Planned AS A_Date_Planned
    ,Reviewer.Forecast AS A_Date_Forecast
    ,Reviewer.Actual AS A_Date_Actual
    ,Reviewer. a_daysdue 
    ,cast(WF_action.dbt_ingestion_date as timestamp) AS snapshot_date
    ,Master.dim_project_name + ' - '  + 'Engineering Document Register' as report_title
    ,cast(Master.dim_project_rls_key as varchar(100)) as project_id 
    ,Master.dim_project_name as meta_project_rls_name
FROM WF_action
Left Join Consolidator on Wf_Action.workflow_number = Consolidator.workflow_number and WF_action.dim_project_rls_key=Consolidator.dim_project_rls_key
Left Join Reviewer on Wf_Action.workflow_number = Reviewer.workflow_number and WF_action.dim_project_rls_key=Reviewer.dim_project_rls_key
Left join  Revision on revision.document_sid = WF_action.document_sid and revision.revision_code = WF_action.revision_code and WF_action.dim_project_rls_key=Revision.dim_project_rls_key
Left Join Master on Master.document_sid = WF_action.document_sid and WF_action.dim_project_rls_key=Master.dim_project_rls_key
Left Join EWP on EWP.version_sid = Revision.version_sid and WF_action.dim_project_rls_key=EWP.dim_project_rls_key
Left Join CWP on CWP.version_sid = Revision.version_sid and WF_action.dim_project_rls_key=CWP.dim_project_rls_key
Left Join WBS on WBS.version_sid = Revision.version_sid and WF_action.dim_project_rls_key=WBS.dim_project_rls_key