{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'oracle_p6/transformed_p6_project_udfvalue/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["p6", "v2"]
        )
}}

SELECT
activityid as activity_key,
activityid as activity_val_id,
activityowneruserid as activityowneruserid,
actuallaborunits as actuallaborunits,
cbscode as cbscode,
cbsid as cbsid,
createdate as createdate,
createuser as createuser,
double as double,
externalearlystartdate as externalearlystartdate,
externallatefinishdate as externallatefinishdate,
feedback as feedback,
finishdate as finishdate,
floatpath as floatpath,
floatpathorder as floatpathorder,
foreignobjectid as foreignobjectid,
hasfuturebucketdata as hasfuturebucketdata,
"integer",
isbaseline as isbaseline,
istemplate as istemplate,
isudftypecalculated as isudftypecalculated,
isudftypeconditional as isudftypeconditional,
lastupdatedate as lastupdatedate,
lastupdateuser as lastupdateuser,
locationname as locationname,
locationobjectid as locationobjectid,
maximumduration as maximumduration,
minimumduration as minimumduration,
mostlikelyduration as mostlikelyduration,
notestoresources as notestoresources,
owneridarray as owneridarray,
ownernamesarray as ownernamesarray,
postrespcriticalityindex as postrespcriticalityindex,
postresponsepessimisticfinish as postresponsepessimisticfinish,
postresponsepessimisticstart as postresponsepessimisticstart,
prerespcriticalityindex as prerespcriticalityindex,
preresponsepessimisticfinish as preresponsepessimisticfinish,
preresponsepessimisticstart as preresponsepessimisticstart,
primaryconstraintdate as primaryconstraintdate,
project_id as project_id,
projectid as projectid,
projectobjectid as projectobjectid,
resumedate as resumedate,
reviewfinishdate as reviewfinishdate,
secondaryconstraintdate as secondaryconstraintdate,
source_system_name as source_system_name,
startdate as startdate,
suspenddate as suspenddate,
taskstatusindicator as taskstatusindicator,
text as text,
udftypedatatype as udftypedatatype,
udftypeobjectid as udftypeobjectid,
udftypesubjectarea as udftypesubjectarea,
udftypetitle as udftypetitle,
workpackageid as workpackageid,
cast(null as string) as workpackagename,
cast(execution_date as date),
{{run_date}} as created_date,
{{run_date}} as updated_date,
{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_p6', 'curated_project_udfvalue') }}