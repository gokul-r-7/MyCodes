{{
     config(
         materialized = "table",
         tags=["project_control"]
         )

}}

select
    hdr.project_internal_id,
    hdr.project_no,
    hdr.project_title,
    hdr.project_no || ' - ' || hdr.project_title as project_display_name,
	pl.portfolio_project_grouping,
    cast(hdr.project_start_date as date) as project_start_date,
    cast(hdr.project_forecast_end_date as date) as project_forecast_end_date,
    hdr.project_manager,
    hdr.project_controller,
    hdr.project_sponsor,
    hdr.approval_status_id,
    case
        when hdr.approval_status_id = 'APR' then 'Approved'
        when hdr.approval_status_id = 'ARC' then 'Archived'
        when hdr.approval_status_id = 'ASR' then 'Approved Snap / Report Only'
        when hdr.approval_status_id = 'IDN' then 'Identified'
        when hdr.approval_status_id = 'CES' then 'Closed External Systems'
        when hdr.approval_status_id = 'CLS' then 'Closed'
    else null end as project_approval_status_desc,
    case
        when hdr.billing_type_name is not null then hdr.billing_type_name
    else 'Unidentified' end as billing_type_name,
    case
        when hdr.billing_type_name = 'Lump Sum' then 'Yes'
    else 'No' end as is_lump_sum,
    hdr.legacy_wbs,
    hdr.ownerorganizationid as sec_org_id,
    org.path,
    org.region,
    org.sub_region,
    org.location_l1,
    org.location_l2,
    org.location_l3,
    org.location_l4,
    org.location_l5,
    org.location_l6,
    hdr.project_size_classification,
    case
        when hdr.project_size_classification is not null then trim(split_part(hdr.project_size_classification,'-',1)) 
    else 'Unidentified' end as project_size,
    hdr.project_risk_classification,
    case
        when hdr.project_risk_classification = '01' or hdr.project_risk_classification = '1' then 'Low'
        when hdr.project_risk_classification = '02' or hdr.project_risk_classification = '2' then 'Medium'
        when hdr.project_risk_classification = '03' or hdr.project_risk_classification = '3' then 'High'
    else 'Unidentified' end as project_risk,
    hdr.project_type,
    trim(split_part(hdr.project_type,'-',1)) as project_type_code,
    case
        when hdr.project_type is not null then trim(split_part(hdr.project_type,'-',2))
    else 'Unidentified' end as project_type_name,
    hdr.project_currency,
    trim(split_part(hdr.project_currency,'-',1)) as project_currency_code,
    trim(split_part(hdr.project_currency,'-',2)) as project_currency_name,
    business_line,
    trim(split_part(hdr.business_line,'-',1)) as business_line_code,
    case
        when hdr.business_line is not null then trim(split_part(hdr.business_line,'-',2))
    else 'Unidentified' end as business_line_name,
    sector,
    trim(split_part(hdr.sector,'-',1)) as sector_code,
    case
        when hdr.sector is not null then trim(split_part(hdr.sector,'-',2))
    else 'Unidentified' end as sector_name,
    hdr.service_type,
    trim(split_part(hdr.service_type,'-',1)) as service_type_code,
    case
        when hdr.service_type is not null then trim(split_part(hdr.service_type,'-',2))
    else 'Unidentified' end as service_type_name,
    hdr.customer_name,
    hdr.customer_type,
    trim(split_part(hdr.customer_type,'-',1)) as customer_type_code,
    case
        when hdr.customer_type is not null then trim(split_part(hdr.customer_type,'-',2))
    else 'Unidentified' end as customer_type_name,
    hdr.home_office_location,
    trim(split_part(hdr.home_office_location,'-',1)) as home_office_location_code,
    case
        when hdr.home_office_location is not null then trim(split_part(hdr.home_office_location,'-',2))
    else 'Unidentified' end as home_office_location_name,
    cast(hdr.project_funding as float) as project_funding,
    case
        when hdr.project_funding is null or hdr.project_funding = 0 then 'No'
    else 'Yes' end as has_funding,
    hdr.major_project,
    case
        when hdr.major_project = 'true' then 'Yes'
    else 'No' end as is_major_project,
    case
        when wh.gid_hours > 0 then 'Yes'
    else 'No' end as has_gid_hours
from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_header') }} hdr
left join {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_org_hierarchy') }} org
    on org.org_id = hdr.ownerorganizationid
left join {{ source('Project_control_domain_integrated_model', 'transformed_project_list') }} pl
    on pl.project_internal_id = hdr.project_internal_id
left join 
    (
        select
        project_id,
        project_internal_id,
        count(*) as gid_hours
    from
        {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_work_hours') }} wh
    where
        hours_category = 'B'
    group by
        project_id,
        project_internal_id
    ) wh
    on hdr.project_internal_id = wh.project_internal_id