{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_change_history/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["o3"]
    ) 
}}
 
SELECT
   --CAST({{ dbt_utils.generate_surrogate_key(['ch.primary_key']) }} 
    ch.primary_key as change_history_key,
    ch.id,  
    ch.uniqueid,
    cast(ch.entitytypeid as INTEGER) as entitytypeid,
    ch.entityname,
    ch.projectid,
    cast(ch.entityid as INTEGER) as entityid,
    cast(ch.eventtypeid as INTEGER) as eventtypeid,
    ch.eventname,
    ch.entitytype,
    cast(ch.actionid as INTEGER) as actionid,
    ch.actiontype,
    ch.description,
    cast(ch.changehistoryid as INTEGER) as changehistoryid,
    ch.fieldname,
    ch.newvalue,
    ch.oldvalue,
    ch.datemodified_ts as timemodified,
    ch.useraccountid,  
    ch.scheduleactivityid,
    ch.cwpreleaseid,
    ch.modelparserconfigurationid,
    ch.integrationid,
    ch.attachmentid,
    ch.contract,
    cast(ch.contractid as INT) as contractid,  
    ch.actionname,
    cast(ch.importid as INTEGER) as importid,
    cast(ch.datecreated as timestamp) as datecreated,  
    cast(ch.createdbyuserid as INT) as createdbyuserid,  
    ch.createdbyuser,
    ch.archive,
    ch.primary_key,
    ch.source_system_name,
    ch.is_current,
    cw.name as cwpspk,
    iw.name as iwpspk,
    ew.name as ewpspk,
    tw.name as twpspk,

    CAST(ch.execution_date as DATE) as execution_date,
    CAST({{ run_date }} as DATE) as model_created_date,
    CAST({{ run_date }} as DATE) as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id

FROM {{ source('curated_o3', 'curated_change_history') }} ch

-- Only join to CWPS
LEFT JOIN {{ source('curated_o3', 'curated_cwps') }} cw
  ON ch.entityname = cw.name AND cw.is_current = 1

-- Only join to IWPS if not matched with CWPS
LEFT JOIN {{ source('curated_o3', 'curated_iwps') }} iw
  ON ch.entityname = iw.name AND iw.is_current = 1 AND cw.name IS NULL

-- Only join to EWPS if not matched with CWPS or IWPS
LEFT JOIN {{ source('curated_o3', 'curated_ewps') }} ew
  ON ch.entityname = ew.name AND ew.is_current = 1 AND cw.name IS NULL AND iw.name IS NULL

-- Only join to TWPS if not matched with any of the above
LEFT JOIN {{ source('curated_o3', 'curated_twps') }} tw
  ON ch.entityname = tw.name AND tw.is_current = 1 AND cw.name IS NULL AND iw.name IS NULL AND ew.name IS NULL
WHERE ch.is_current = 1
{%- if execution_date_arg != "" %}
    AND execution_date >= '{{ execution_date_arg }}'
{%- else %}
    {%- if is_incremental() %}
        AND cast(execution_date as DATE) > (SELECT max(execution_date) FROM {{ this }})
    {%- endif %}
{%- endif %}  