
{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}

with wbs as (
    select
        version_sid,
        attribute_value,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_custom_attribute') }}
    where
        normalized_attribute = 'Work Brakedown Structure'
        and meta_project_rls_key = '1207979025'
        and meta_journal_current = 1
),
ewp as (
    select
        version_sid,
        package_name,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_package_to_document') }}
    where
        package_type = 'EwpNumber'
        and meta_project_rls_key = '1207979025'
),
cwp as (
    select
        version_sid,
        package_name,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_package_to_document') }}
    where
        package_type = 'CwpNumber'
        and meta_project_rls_key = '1207979025'
),
cwpzone as (
    select
        version_sid,
        package_name,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_package_to_document') }}
    where
        package_type = 'CwpZone'
        and meta_project_rls_key = '1207979025'
),
vgarea as (
    select
        version_sid,
        package_name,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_package_to_document') }}
    where
        package_type = 'VgArea'
        and meta_project_rls_key = '1207979025'
),
epnumber as (
    select
        version_sid,
        package_name,
        split_part(version_sid, '|', 2) as documentid
    from
        {{ ref('sf_package_to_document') }}
    where
        package_type = 'EpNumber'
        and meta_project_rls_key = '1207979025'
)
select
    'aconex' || '|' || ds.documentid as version_sid,
    ds.documentid,
    documentnumber as documentno,
    revision,
    title,
    documenttype as "type",
    documentstatus as status,
    discipline as areacode,
    vdrcode as discipline,
    category as documentidentifier,
    selectlist1 as unitcode,
    selectlist2 as fileclassification,
    selectlist3 as originator,
    selectlist4 as subprojectnumbername,
    selectlist5 as phase,
    selectlist6 as vgdocumenttype,
    selectlist7 as subunit_train_jetty_tankid,
    selectlist8 as purchaseordernumber,
    selectlist9 as fdcategory_section_subsection,
    selectlist10 as securityclassification,
    confidential as confidential,
    filename as "file",
    printsize as printsize,
    reference as documentauthor,
    cast(datecreated as timestamp) as revisiondate,
    cast(toclientdate as timestamp) as datetoclient,
    cast(datemodified as timestamp) as datemodified,
    NULLIF(milestonedate, '')::timestamp as milestonedate,
    cast(plannedsubmissiondate as timestamp) plannedsubmissiondate,
    date1 as ofecontractdate,
    NULLIF(date2, '')::timestamp as datereceivedfromclient,
    author as originatingorganisation,
    reviewstatus,
    reviewsource,
    comments,
    comments2 as supersededby,
    tagnumber as vgtransmittalnumber,
    contractnumber as clientreviewstatus,
    vendordocumentnumber as supplierdocumentnumber,
    vendorrevision as supplierdocumentrevision,
    contractordocumentnumber as alternatedocumentnumber,
    '' as worleydocumentrevision,
    asbuiltrequired,
    check2 as forhandoverorturnover,
    projectfield3 as appliestoitem,
    check1 as integritycritical,
    versionnumber as "version",
    wbs.attribute_value as wbscode,
    cwpzone.package_name as cwpzone,
    trackingid,
    vgarea.package_name as vgarea,
    epnumber.package_name as epnumber,
    '' as eptype,
    ewp.package_name as ewpno,
    cwp.package_name as cwpnumber,
    'admin' as created_by,
    to_timestamp(execution_date,'YYYY-MM-DD HH24:MI:SS') :: timestamp as execution_date,
    cast(eff_end_date as date) as eff_end_date,
    current as current_flag,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    {{ source('curated_aconex', 'curated_docregister_standard') }} ds
    left join cwp on ds.documentid = cwp.documentid
    left join cwpzone on ds.documentid = cwpzone.documentid
    left join wbs on ds.documentid = wbs.documentid
    left join ewp on ewp.documentid = ds.documentid
    left join vgarea on vgarea.documentid = ds.documentid
    left join epnumber on epnumber.documentid = ds.documentid
where
    ds.projectid = '1207979025'
    and ds.is_current = '1'
