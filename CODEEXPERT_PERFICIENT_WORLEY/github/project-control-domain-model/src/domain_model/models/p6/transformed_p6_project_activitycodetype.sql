{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'oracle_p6/transformed_p6_project_activitycodetype/',
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
externalearlystartdate as externalearlystartdate,
externallatefinishdate as externallatefinishdate,
feedback as feedback,
floatpath as floatpath,
floatpathorder as floatpathorder,
hasfuturebucketdata as hasfuturebucketdata,
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
resumedate as resumedate,
reviewfinishdate as reviewfinishdate,
secondaryconstraintdate as secondaryconstraintdate,
source_system_name as source_system_name,
suspenddate as suspenddate,
taskstatusindicator as taskstatusindicator,
workpackageid as workpackageid,
cast(null as string) as workpackagename,
cast(execution_date as date),
{{run_date}} as created_date,
{{run_date}} as updated_date,
{{ generate_load_id(model) }} as load_id
FROM
{{ source('curated_p6', 'curated_project_activitycodetype') }}
