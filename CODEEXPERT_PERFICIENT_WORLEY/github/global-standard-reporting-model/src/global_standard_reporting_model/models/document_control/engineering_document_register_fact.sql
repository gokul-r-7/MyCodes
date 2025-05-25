{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}


WITH Master as
(
    Select  *
    FROM     {{ source('document_control_domain_integrated_model', 'master') }}
   Where category != 'Supplier Data' or type_code not like '%SDC%'
)

, Revision as
( 
    Select *  
    FROM  {{ source('document_control_domain_integrated_model', 'revision') }}
   where file_type = 'Primary' and current = 'true' 
   and status_code not in ('SUP','VOD','Cancelled','No Longer In Use', 'Rejected', 'Working Document')
)
,

Resubmission AS
(
    Select MAX(countofrevision) as no_of_resubmission,status_code,document_sid
    FROM (
        SELECT
             document_sid, status_code,
            (
                dense_rank() over (partition by document_sid,status_code  order by revision_code asc) 
                + dense_rank() over (partition by document_sid,status_code  order by revision_code desc)
                - 1
            ) as countofrevision 
        FROM    {{ source('document_control_domain_integrated_model', 'revision') }}
        WHERE file_type = 'Primary' -- and current = 'true' 
            AND status_code not in ('SUP','VOD','Cancelled','No Longer In Use', 'Rejected', 'Working Document')
        )
        where status_code IN ('IFR','IFA','IFC')
        group by document_sid,status_code
)

, Transmittal as
(
   Select Distinct
    MIN(sent_date) OVER (
    PARTITION BY document_sid, status_code, revision_code
     ) as Min_Transmittal_Date
    ,transmittal_number
    ,document_sid
    ,revision_code as revison_code
    ,status_code as status_code
    FROM(
        Select revision.document_sid,status_code,revision_code,sent_date,transmittal_number 
        FROM    {{ source('document_control_domain_integrated_model', 'revision') }} as revision
    Left Join  {{ source('document_control_domain_integrated_model', 'transmittal_to_document') }} as transmittal_doc on Revision.version_sid = Transmittal_Doc.version_sid
    Left Join  {{ source('document_control_domain_integrated_model', 'transmittal') }} on Transmittal_doc.transmittal_sid = Transmittal.transmittal_sid
             where   distribution_type = 'TO' and (recipient_organization not like 'Worley%' OR Transmittal.recipient_organization not like 'worley%')
    )
),
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
SELECT Distinct
    Master.document_sid
    ,Master.document_number
    ,Master.category
    ,cast(Revision.revision_date as date) as revision_date
    ,Revision.issued_date_weekend as issued_date --for now use this date for date_modified
    ,Revision.revision_code
    ,Master.title
    ,Master.type_code 
    ,Master.type_name
    ,Revision.current as current_flag
     ,Revision.status_code as status_code
     ,Revision.status_name as status_name    
    ,CASE 
            WHEN Revision.status_code LIKE 'IFR%' THEN 'Pending Review(IFR)'
            WHEN Revision.status_code LIKE 'IFA%' THEN 'Pending Review(IFA)'
            ELSE NULL END AS IFR_Status         
    ,Revision.version_number
    ,Revision.originating_organization
    ,Master.discipline_code
    ,Master.discipline_name
    ,Master.asset_name as asset
    ,Revision.sub_project
    ,Revision.phase 
    ,EWP.ewp as ewp
       ,CWP.cwp as cwp
       ,WBS.wbs as wbs
    ,case when Master.category = 'Drawing' then 'Yes' else 'No' end as is_drawing
    ,Transmittal.Min_Transmittal_Date as transmittal_date
    ,Transmittal.transmittal_number as transmittal_number
    ,Case When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '1' then 'IFC'
            When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '2' then  'IFC (Revised 1)'
           When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '3' then  'IFC (Revised 2)'
           else  'IFC (Revised > 2)' end as revised_status
    --,Case   when Resubmission.idc is null or Resubmission.idc = 0 then 0 else Resubmission.idc - 1 end as idc_no_of_resubmission
    ,Case when Resubmission.no_of_resubmission is null or Resubmission.no_of_resubmission = 0 then 0 else Resubmission.no_of_resubmission - 1 end as no_of_resubmission
    ,Resubmission.status_code as resubmission_filter
    ,Case when Revision.status_code = 'Reserved' 
          OR Revision.status_code = 'IIR - Issued for Internal Review' 
    OR Revision.status_code = 'Commented Internally' 
     then 'In Progress'
    when (Revision.status_code like 'Issued%' OR Revision.status_name like 'Issued%')
    and transmittal.Min_Transmittal_Date is not null
     then 'Issued'
     else 'Others' 
  end as issued_vs_reserved
    ,dim_project_name + ' - ' + 'Engineering Document Register' as report_title
    --,'Refreshed On: ' + Max(revision.dbt_ingestion_date) as date_stamp_label
    ,Master.dim_project_rls_key as project_id  
    ,dim_project_name as meta_project_rls_name
    ,Revision.dbt_ingestion_date  AS snapshot_date

    FROM
    Revision

    Left join  Master on Master.document_sid = Revision.document_sid 
    Left Join Resubmission on Revision.document_sid = Resubmission.document_sid
    Left Join Transmittal on Transmittal.document_sid = Revision.document_sid and Transmittal.revison_code = Revision.revision_code and Transmittal.status_code = Revision.Status_code
    Left Join EWP on EWP.version_sid = Revision.version_sid
    Left Join CWP on CWP.version_sid = Revision.version_sid
    Left Join WBS on WBS.version_sid = Revision.version_sid
    Where Master.document_sid is not null