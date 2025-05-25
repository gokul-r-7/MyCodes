{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}


WITH Master as
(
    Select  *
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_master') }}
   Where category != 'Supplier Data' or type_code not like '%SDC%'
)

, Revision as
( 
    Select *  
    FROM  {{ source('document_control_domain_integrated_model', 'transformed_revision') }}
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
        FROM    {{ source('document_control_domain_integrated_model', 'transformed_revision') }}
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
     ,MIN(transmittal_number) OVER (
    PARTITION BY document_sid, status_code, revision_code
     ) as transmittal_number
    --,transmittal_number
    ,document_sid
    ,revision_code as revison_code
    ,status_code as status_code
    FROM(
        Select revision.document_sid,status_code,revision_code,sent_date,transmittal_number 
        FROM    {{ source('document_control_domain_integrated_model', 'transformed_revision') }} as revision
    Left Join  {{ source('document_control_domain_integrated_model', 'transformed_transmittal_to_document') }} as transmittal_doc on Revision.version_sid = Transmittal_Doc.version_sid
    Left Join  {{ source('document_control_domain_integrated_model', 'transformed_transmittal') }} as Transmittal on Transmittal_doc.transmittal_sid = Transmittal.transmittal_sid
             where   distribution_type = 'TO' and (recipient_organization not like 'Worley%' OR Transmittal.recipient_organization not like 'worley%')
    )
),
EWP as
(
    Select  version_sid, package_name as ewp
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_package_to_document') }}
    where package_type = 'Engineering Work Package'
)   
,
CWP as
(
    Select  version_sid, package_name as cwp
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_package_to_document') }}
    where package_type = 'Construction Work Package'
)   
,
WBS as
(
    Select  version_sid, attribute_value as wbs
    FROM     {{ source('document_control_domain_integrated_model', 'transformed_custom_attribute') }}
    where normalized_attribute = 'Work Brakedown Structure'
)    
SELECT Distinct
    Master.document_sid
    ,Master.document_number
    ,Master.category
    ,cast(Revision.revision_date::timestamp as date) as revision_date
    ,cast(Revision.issued_date_weekend::timestamp as timestamp) as issued_date --for now use this date for date_modified
    ,DATE_PART(week,  Revision.issued_date_weekend::timestamp) as issued_date_week
    ,to_char(issued_date_weekend::timestamp,'Mon') + '-' + DATE_PART(year,  Revision.issued_date_weekend::timestamp) as issued_date_month_year
    ,Revision.revision_code
    ,Revision.confidential
    ,Master.title
    ,Master.type_code 
    ,Master.type_name
    ,Revision.current as current_flag
     ,Revision.status_code as status_code
     --,Revision.status_name as status_name
    ,case when Revision.status_name is NULL or Revision.status_name=''  then Revision.status_code else Revision.status_name end as status_name    
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
    ,cast(Transmittal.Min_Transmittal_Date as timestamp) as transmittal_date
    ,Transmittal.transmittal_number as transmittal_number
    ,Case When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '1' then 'IFC'
            When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '2' then  'IFC (Revised 1)'
           When Resubmission.Status_code = 'IFC' and Resubmission.no_of_resubmission = '3' then  'IFC (Revised 2)'
           else  'IFC (Revised > 2)' end as revised_status
    --,Case   when Resubmission.idc is null or Resubmission.idc = 0 then 0 else Resubmission.idc - 1 end as idc_no_of_resubmission
    ,Case when Resubmission.no_of_resubmission is null or Resubmission.no_of_resubmission = 0 then 0 else Resubmission.no_of_resubmission - 1 end as no_of_resubmission
    ,Resubmission.status_code as resubmission_filter
    , Case when Revision.status_code = 'Reserved' 
          OR Revision.status_code = 'IIR'--'IIR - Issued for Internal Review' 
    OR Revision.status_code = 'Commented Internally'
    or (Revision.status_code like 'Issued%' OR Revision.status_name like 'Issued%')
    and transmittal.Min_Transmittal_Date is null
     then 'In Progress'
    when (Revision.status_code like 'Issued%' OR Revision.status_name like 'Issued%')
    and transmittal.Min_Transmittal_Date is not null
     then 'Issued'
     else 'Others' 
  end as issued_vs_reserved
    ,dim_project_name + ' - ' + 'Engineering Document Register' as report_title
    --,'Refreshed On: ' + Max(revision.dbt_ingestion_date) as date_stamp_label
    ,cast(Master.dim_project_rls_key as varchar(100)) as project_id  
    ,dim_project_name as meta_project_rls_name
    ,cast(Revision.dbt_ingestion_date as timestamp)  AS snapshot_date
    ,Master.native_application as source 

    FROM
    Revision

    Left join  Master on Master.document_sid = Revision.document_sid 
    Left Join Resubmission on Revision.document_sid = Resubmission.document_sid
    Left Join Transmittal on Transmittal.document_sid = Revision.document_sid and Transmittal.revison_code = Revision.revision_code and Transmittal.status_code = Revision.Status_code
    Left Join EWP on EWP.version_sid = Revision.version_sid
    Left Join CWP on CWP.version_sid = Revision.version_sid
    Left Join WBS on WBS.version_sid = Revision.version_sid
    Where Master.document_sid is not null
