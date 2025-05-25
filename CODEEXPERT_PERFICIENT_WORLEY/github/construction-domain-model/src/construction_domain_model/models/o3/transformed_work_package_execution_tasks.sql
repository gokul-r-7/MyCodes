{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_work_package_execution_tasks/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
    ) 
}}
 
select
{{ dbt_utils.generate_surrogate_key(['projectid','entitytype','tasktype','name']) }} as workpackageexecutiontasks_key,
CAST(id AS INT) as workpackageexecutiontaskid,
CAST(entityid AS INT) AS entityid,
CAST(entitytypeid AS INT) AS entitytypeid,
entitytype,
entityname,
category,
tasktype,
CAST(taskunitofmeasureid AS INT) AS taskunitofmeasureid,
taskuom,
CAST(workpackageexecutiontasktypeid AS INT) AS workpackageexecutiontasktypeid,
drawing,
CAST(totalunits AS FLOAT) AS totalunits,
CAST(estimatedhours AS FLOAT) AS estimatedhours,
CAST(projectid AS INT) AS projectid,
CAST(actionentitytypeid AS INT) AS actionentitytypeid,
name as workpackageexecutiontaskname,
description as workpackageexecutiontaskdescription,
CAST(createdbyuserid AS INT) AS createdbyuserid,
CAST(datecreated AS TIMESTAMP) AS datecreated,
createdbyuser,
CAST(modifiedbyuserid AS INT) AS modifiedbyuserid,
datemodified_ts as datemodified,
modifiedbyuser,
CAST(totalstepcount AS INT) AS totalstepcount,
CAST(totalstepweight AS FLOAT) AS totalstepweight,
scaffoldid,
CAST(statusid AS INT) AS statusid,
statuscolor,
status, 
CAST(multiplier AS FLOAT) AS multiplier,
CAST(percentcomplete AS FLOAT) AS percentcomplete,
CAST(earnedhours AS FLOAT) AS earnedhours,
CAST(constructioncomponentid AS INT) AS constructioncomponentid,
CAST(componententitytypeid AS INT) AS componententitytypeid,
"float" as _float,
CAST(componentmissing AS BOOLEAN) AS componentmissing,
CAST(workpackagecomponentid AS INT) AS workpackagecomponentid,
CAST(executiontasksortorder AS INT) AS executiontasksortorder,
CAST(needscalculation AS BOOLEAN) AS needscalculation,
executiontaskscheduleactivityid,
CAST(executiontaskplannedstartdate AS TIMESTAMP) AS executiontaskplannedstartdate,
CAST(executiontaskdatedue AS TIMESTAMP) AS executiontaskdatedue,
CAST(executiontaskactualstartdate AS TIMESTAMP) AS executiontaskactualstartdate,
CAST(executiontaskactualfinishdate AS TIMESTAMP) AS executiontaskactualfinishdate,
CAST(o3estimatedhours AS FLOAT) AS o3estimatedhours,
CAST(overrideestimatedhours AS FLOAT) AS overrideestimatedhours,
CAST(componentbacklogtaskid AS INT) AS componentbacklogtaskid,
CAST(calculationrateofplacement AS FLOAT) AS calculationrateofplacement,
componentidentifier,
CAST(progresstypeid AS INT) AS progresstypeid,
progresstype,
CAST(completedunits AS INT) AS completedunits,
category2,
category3,
category4,
category5,
CAST(allowpartialprogress AS BOOLEAN) AS allowpartialprogress,
code,
CAST(completedbyuserid AS INT) AS completedbyuserid,
completedbyuser,
CAST(tradequantity AS FLOAT) AS tradequantity,
tradeduration,
is_current,
   cast(execution_date as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_o3', 'curated_work_package_execution_tasks') }}
where is_current = 1
{%- if execution_date_arg != "" %}
    and execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(execution_date as DATE) > (select max(execution_date) from {{ this }})
    {%- endif %}
{%- endif %}  