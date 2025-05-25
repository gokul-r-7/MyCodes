{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_work_package_execution_tasks_steps/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
    ) 
}}
 
select
    {{ dbt_utils.generate_surrogate_key(['projectid','entitytype','id']) }} as workpackageexecutiontaskssteps_key,
    CAST(id AS INT) as workpackageexecutiontaskstepid,
    projectid,
    CAST(workpackageexecutiontaskid AS INT) as workpackageexecutiontaskid,
    CAST(entityid AS INT) as entityid,
    CAST(entitytypeid AS INT) as entitytypeid,
    entityname,
    name as workpackageexecutiontaskname,
    CAST(executiontaskstepsortorder AS INT) as workpackageexecutiontasksort,
    CAST(datecompleted AS TIMESTAMP) as datecompleted,
    CAST(completedbyuserid AS INT) as completedbyuserid,
    completedbyuser,
    CAST(createdbyuserid AS INT) as createdbyuserid,
    CAST(datecreated AS TIMESTAMP) as datecreated,
    createdbyuser,
    CAST(modifiedbyuserid AS INT) as modifiedbyuserid,
    CAST(datemodified AS TIMESTAMP) as datemodified,
    modifiedbyuser,
    CAST(excluded AS BOOLEAN) as excluded,
    CAST(componentbacklogtaskstepid AS INT) as componentbacklogtaskstepid,
    CAST(componentbacklogtaskid AS INT) as componentbacklogtaskid,
    CAST(entitystatusrestrictprogressing AS BOOLEAN) as entitystatusrestrictprogressing,
    CAST(sortorder AS INT) as sortorder,
    taskname,
    category,
    tasktype,
    CAST(taskunitofmeasureid AS INT) as taskunitofmeasureid,
    taskuom,
    CAST(executiontasksortorder AS INT) as executiontasksortorder,
    CAST(weight AS FLOAT) as weight,
    CAST(steprateofplacement AS FLOAT) AS steprateofplacement,
    CAST(stepestimatedhours AS FLOAT) AS stepestimatedhours,
    CAST(stepearnedhours AS FLOAT) AS stepearnedhours,
    CAST(percentcomplete AS FLOAT) AS percentcomplete,
    CAST(allowpartialprogress AS BOOLEAN) as allowpartialprogress,
    CAST(iscompleted AS BOOLEAN) as iscompleted,
    sourcekey,
    referencenumber,
    is_current,
    CAST(simpleentityid AS INT) as simpleentityid,
   cast(execution_date as DATE) as execution_date,
    CAST({{run_date}} as DATE) as model_created_date,
    CAST({{run_date}} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_o3', 'curated_work_package_execution_tasks_steps') }}
where is_current = 1
{%- if execution_date_arg != "" %}
    and execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        and cast(execution_date as DATE) > (select max(execution_date) from {{ this }})
    {%- endif %}
{%- endif %}   