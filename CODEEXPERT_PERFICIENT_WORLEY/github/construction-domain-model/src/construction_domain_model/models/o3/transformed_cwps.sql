{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_cwps/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
    ) 
}}
  
select 
    {{ dbt_utils.generate_surrogate_key(['projectid','name']) }} as cwpspk,
    a.scheduleactivityid as activityid,
    CAST(a.id AS INTEGER) AS entityid,
    CAST(a.entitytypeid AS INTEGER) AS entitytypeid,
    CAST(a.constructionworkareaid AS INTEGER) AS cwasid,
    CAST(a.disciplineid AS INTEGER) AS disciplineid,
    CAST(a.constructiontypeid AS INTEGER) AS constructiontypeid,
    CAST(a.wbsid AS INTEGER) AS wbsid,
    CAST(a.projectid AS INTEGER) AS projectid,
    a.constructiontype,
    a.wbs,
    CAST(a.plannedduration AS INTEGER) AS plannedduration,
    CAST(a.actualduration AS INTEGER) AS actualduration,
    CAST(a.openconstraintcount AS INTEGER) AS openconstraintcount,
    CAST(a.iwptotalestimatedhours AS INTEGER) AS iwptotalestimatedhours,
    CAST(a.totalconstraintcount AS INTEGER) AS totalconstraintcount,
    CAST(a.closedconstraintcount AS INTEGER) AS closedconstraintcount,
    CAST(a.baselinestartdate as TIMESTAMP) as baselinestartdate,
    CAST(a.baselinefinishdate as TIMESTAMP) as baselinefinishdate,
    a.projectinfo,
    a.name,
    a.constructionworkarea,
    a.cwadescription,
    a.zone,
    a.area,
    a.discipline,
    a.disciplinedescription,
    CAST(a.issuedatevariance AS INTEGER) AS issuedatevariance,  
    cast(a.plannedstartdate as timestamp) as plannedstartdate,
    CAST(a.plannedstartyear AS INTEGER) AS plannedstartyear,  
    CAST(a.plannedstartmonth AS INTEGER) AS plannedstartmonth,  
    CAST(a.plannedstartweek AS INTEGER) AS plannedstartweek, 
    CAST(a.plannedstartdayofyear AS INTEGER) AS plannedstartdayofyear,  
    CAST(a.plannedstartweekofyear AS INTEGER) AS plannedstartweekofyear,  
    CAST(a.actualstartdate as TIMESTAMP) as actualstartdate,
    CAST(a.actualstartyear AS INTEGER) AS actualstartyear,  
    CAST(a.actualstartmonth AS INTEGER) AS actualstartmonth,  
    CAST(a.actualstartweekofyear AS INTEGER) AS actualstartweekofyear,  
    CAST(a.actualstartdayofyear AS INTEGER) AS actualstartdayofyear, 
    CAST(a.startdatevariance AS INTEGER) AS startdatevariance, 
    cast(a.plannedfinishdate as timestamp) as plannedfinishdate,
    cast(a.actualfinishdate as timestamp) as actualfinishdate,
    CAST(a.finishdatevariance AS INTEGER) AS finishdatevariance,  
    CAST(a.actualfinishyear AS INTEGER) AS actualfinishyear,  
    CAST(a.actualfinishmonth AS INTEGER) AS actualfinishmonth,  
    CAST(a.actualfinishweekofyear AS INTEGER) AS actualfinishweekofyear,  
    CAST(a.actualfinishdayofyear AS INTEGER) AS actualfinishdayofyear, 
    CAST(a.statusid AS INTEGER) AS statusid,
    a.status,
    CAST(a.statusdate AS TIMESTAMP) AS statusdate,
    a.cwpstatus,
    a.cwpstatuscolor,
    a.name  AS cwpsid,
    a.description as cwpsdescription,
    a.longdescription as cwpslongdescription,
    a.unitid as unitid,
    a.unit,
    CAST(a.areaid as INTEGER) as areaid,
    CAST(a.zoneid AS INTEGER) AS zoneid,
    CAST(b.RegionID as INTEGER) RegionID,
    a.purposeid  as purposeid,
    a.purpose,
    CAST(a.contractid AS INTEGER) AS contractid,
    a.contract,
    CAST(a.planneruserid AS INTEGER) AS planneruserid,
    a.planneruser,
    a.planneruseremail,
    a.significantrequirements,
    a.scopeexclusions,
    a.externallink,
    a.revision as revision,
    CAST(revisiondate AS TIMESTAMP) AS revisiondate,
    a.scopeid as scopeid,
    a.scope,
    a.deliveryteamid as deliveryteamid,
    a.deliveryteam,
    CAST(a.drawingcount AS INTEGER) AS drawingcount,
    CAST(a.budgetedhours AS FLOAT) AS budgetedhours,
    CAST(a.estimatedhours AS FLOAT) AS estimatedhours,
    a.scheduleactivityid as scheduleactivityid,
    CAST(plannedissuedate AS TIMESTAMP) AS plannedissuedate,
    CAST(a.actualissuedate as TIMESTAMP) AS actualissuedate,
    CAST(a.assemblystartdate AS TIMESTAMP) AS assemblystartdate,
    CAST(a.assemblyfinishdate AS TIMESTAMP) AS assemblyfinishdate,
    a.releasedtouserid as releasedtouserid,
    a.releasedtouser,
    CAST(a.plannedreleasedate AS TIMESTAMP) AS plannedreleasedate,
    CAST(a.forecastreleasedate AS TIMESTAMP) AS forecastreleasedate,
    CAST(a.datedelivered AS TIMESTAMP) AS datedelivered,
    CAST(a.earnedhours AS FLOAT) AS earnedhours,
    CAST(a.actualhours as FLOAT) as actualhours,
    CAST(a.forecastedhours AS FLOAT) AS forecastedhours,
    CAST(a.percentcomplete AS FLOAT) AS percentcomplete,
    CAST(a.percentdocumented AS FLOAT) AS percentdocumented,
    CAST(a.percentdocumentedinclusive AS FLOAT) AS percentdocumentedinclusive,
    CAST(a.percentconstraintfree AS FLOAT) AS percentconstraintfree,
    CAST(a.percentdelayfree AS FLOAT) AS percentdelayfree,
    CAST(a.percentapproved AS FLOAT) AS percentapproved,
    CAST(a.percentdeveloped AS FLOAT) AS percentdeveloped,
    a.designareacode as designareacode,
    a.designarea,
    a.volumetypecode as volumetypecode,
    a.volumetype,
    a.modulenumber as modulenumber,
    a.modulehandlingcode,
    a.modulehandlingdescription,
    CAST(a.cwpsequencenumber as INTEGER) cwpsequencenumber,
    a.materialdestination,
    CAST(a.datecreated AS TIMESTAMP) AS datecreated,
    CAST(a.createdbyuserid AS INTEGER) AS createdbyuserid,
    a.createdbyuser,
    CAST(a.datemodified AS TIMESTAMP) AS datemodified,
    CAST(a.modifiedbyuserid AS INTEGER) AS modifiedbyuserid,
    a.modifiedbyuser,
    CAST(a.isdeleted AS BOOLEAN) AS isdeleted,
    CAST(a.percentageplanned AS FLOAT) AS percentageplanned,
    CAST(a.sourcedeleted AS BOOLEAN) AS sourcedeleted,
    a.dateconstraintfree as dateconstraintfree,
    CAST(a.keyquantity AS FLOAT) AS keyquantity,
    a.unitofmeasureid as unitofmeasureid,
    a.uom,
    CAST(a.releasedquantity AS FLOAT) AS releasedquantity,
    CAST(a.percentreleased AS FLOAT) AS percentreleased,
    CAST(a.technicalauthorshiptransferdate AS TIMESTAMP) AS technicalauthorshiptransferdate,
    CAST(a.datedatacomplete AS TIMESTAMP) AS datedatacomplete,
    CAST(a.datecloseoutdatacomplete AS TIMESTAMP) AS datecloseoutdatacomplete,
    a.datacompletenessstatus,
    CAST(estimatedcost AS FLOAT) AS estimatedcost,
    CAST(a.forecastedcost AS FLOAT) AS forecastedcost,
    CAST(a.actualcost AS FLOAT) AS actualcost,
    CAST(a.planneddevelopmentstartdate AS TIMESTAMP) AS planneddevelopmentstartdate,
    CAST(a.actualdevelopmentstartdate AS TIMESTAMP) AS actualdevelopmentstartdate,
    CAST(a.plannedreadyforreviewdate AS TIMESTAMP) AS plannedreadyforreviewdate,
    CAST(a.readyforreviewdate AS TIMESTAMP) AS readyforreviewdate,
    CAST(a.plannedapproveddate AS TIMESTAMP) AS plannedapproveddate,
    CAST(a.approveddate as TIMESTAMP) as approveddate,
    CAST(a.draftreviewdate AS TIMESTAMP) AS draftreviewdate,
    CAST(a.plannedcloseoutdate AS TIMESTAMP) AS plannedcloseoutdate,
    CAST(a.actualcloseoutdate AS TIMESTAMP) as actualcloseoutdate,
    CAST(a.overviewmodelshotattachmentid AS INTEGER) AS overviewmodelshotattachmentid,
    a.contractgroupid as contractgroupid,
    a.contractgroup as contractgroup,
    CAST(a.criticalpath AS BOOLEAN) AS criticalpath,
    a.criticalpathcategoryid as criticalpathcategoryid,
    a.criticalpathcategory as criticalpathcategory,
    CAST(a.cwpreleasecount AS INTEGER) AS cwpreleasecount,
    CAST(a.overdueconstraintcount AS INTEGER) AS overdueconstraintcount,
    CAST(a.attachmentcount AS INTEGER) as attachmentcount,
    a.unitcategoryid as unitcategoryid,
    a.unitcategory as unitcategory,
    a.labels as labels,
    CAST(a.stateid AS INTEGER) AS stateid,
    CAST(a.forecaststartdate AS TIMESTAMP) AS forecaststartdate,
    CAST(a.forecastfinishdate AS TIMESTAMP) AS forecastfinishdate,
    CAST(a.externallinkverified AS BOOLEAN) AS externallinkverified,
    a.externallinkverifiedbyuser as externallinkverifiedbyuser,
    a.externallinkverifiedbyuserid as externallinkverifiedbyuserid,
    CAST(a.externallinkverifieddate AS TIMESTAMP) AS externallinkverifieddate,
    CAST(a.dateapprovedforconstruction AS TIMESTAMP) AS dateapprovedforconstruction,
    CAST(a.plannedapprovedforconstructiondate AS TIMESTAMP) AS plannedapprovedforconstructiondate,
    a.forgemodelshotstate as forgemodelshotstate,
    CAST(a.invalidmodeldata AS BOOLEAN) AS invalidmodeldata,
    CAST(a.calendarstartdate AS TIMESTAMP) as calendarstartdate,
    CAST(a.calendarenddate AS TIMESTAMP) as calendarenddate,
    a.statuscolor as statuscolor,
    a.masterentityid as masterentityid,
    a.releasedbyuserid as releasedbyuserid,
    a.releasedbyuser as releasedbyuser,
    CAST(a.datereleased AS TIMESTAMP) AS datereleased,
    CAST(a.hasreleasedversion AS BOOLEAN) AS hasreleasedversion,
    CAST(a.hasworkingversion AS BOOLEAN) AS hasworkingversion,
    CAST(a.leadengineeruserid AS INTEGER) AS leadengineeruserid,
    CAST(a.constructionrepresentativeuserid AS INTEGER) AS constructionrepresentativeuserid,
    a.leadengineer as leadengineer,
    a.leadengineeremail as leadengineeremail,
    a.constructionrepresentative as constructionrepresentative,
    a.constructionrepresentativeemail as constructionrepresentativeemail,
    CAST(a.isworkingversion AS BOOLEAN) AS isworkingversion,
    a.lastreleasedversionentityid as lastreleasedversionentityid,
    CAST(a.budgetedearnedhours AS FLOAT) as budgetedearnedhours,
    CAST(a.modelshot AS BOOLEAN) AS modelshot,
    a.forecastduration as forecastduration,
    CAST(a.projectphaseid AS INTEGER) AS projectphaseid,
    a.projectphase as projectphase,
    CAST(a.drawingfilecount AS INTEGER) AS drawingfilecount,
    a.lessonslearnednotes as lessonslearnednotes,
    a.safetynotes as safetynotes,
    a.interfacenotes as interfacenotes,
    a.constructabilitynotes as constructabilitynotes,
    a.qualitynotes as qualitynotes,
    a.vendornotes as vendornotes,
    a.benchmarknotes  AS benchmarknotes,
    CAST(a.unreleaseddrawingcount AS INTEGER) AS unreleaseddrawingcount,
    CAST(a.plotplancount AS INTEGER) AS plotplancount,
    CAST(a.drawingsupdatedafterlatestreleasecount AS INTEGER) AS drawingsupdatedafterlatestreleasecount,
    a.preferredplotplanid as preferredplotplanid,
    a.preferredplotplan as preferredplotplan,
    a.contractors as contractors,
    CAST(a.scheduledatadate  AS TIMESTAMP) as scheduledatadate,
    CAST(a.earlystartdate AS TIMESTAMP) AS earlystartdate,
    CAST(a.latestartdate AS TIMESTAMP) AS latestartdate,
    CAST(a.earlyfinishdate AS TIMESTAMP) AS earlyfinishdate,
    CAST(a.latefinishdate AS TIMESTAMP) AS latefinishdate,
    a.functionalareaid as functionalareaid,
    a.functionalarea as functionalarea,
    a.componentkeyquantityuom as componentkeyquantityuom,
    a.componentkeyquantityunitofmeasureid as componentkeyquantityunitofmeasureid,
    a.componentkeyquantityunitfield as componentkeyquantityunitfield,
    a.componentkeyquantityunitfieldid as componentkeyquantityunitfieldid,
    a.is_current,
    CAST(a.componentkeyquantity AS FLOAT) AS componentkeyquantity,
    cast(a.execution_date as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_o3', 'curated_cwps') }} a
left join {{ ref('transformed_region') }} b ON a.zoneid = b.zoneid and a.areaid = b.areaid
where a.is_current = 1
{%- if execution_date_arg != "" %}
    and a.execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(a.execution_date as DATE) > 
            (select MAX(cast(execution_date as DATE)) as max_execution_date
            from {{ this }})
    {%- endif %}
{%- endif %}