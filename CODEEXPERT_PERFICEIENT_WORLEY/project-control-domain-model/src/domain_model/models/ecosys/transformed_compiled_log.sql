{%- set execution_date = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'ecosys/transformed_compiled_log_final/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys"]
        ) 
}}

WITH actuals AS (

    SELECT
        EAL.actuallog_costobjectid as costobjectid,
        EAL.actuallog_padate as period_date,
        OD.reporting_week_end_date AS reporting_week_end_date,
        EAL.actuallog_description AS description,
        EAL.actuallog_apistatusmemofield AS status,
        EAL.actuallog_createdate AS created_date,
        'Actuals' AS type
    FROM {{ source('curated_ecosys', 'curated_ecosys_actuallog') }} EAL
    LEFT JOIN {{ source('curated_ecosys', 'curated_ecosys_our_dates') }} OD
    ON TO_DATE(EAL.actuallog_padate, 'YYYY-MM-DD') = TO_DATE(OD.reporting_week_end_date, 'MM/DD/YYYY')

), progress AS (

    SELECT
        EPL.earnedvaluelog_costobjectid as costobjectid,
        EPL.earnedvaluelog_perioddate as period_date,
        OD.reporting_week_end_date AS reporting_week_end_date,
        EPL.earnedvaluelog_description AS description,
        '' AS status,
        EPL.earnedvaluelog_createdate as created_date,
        'Progress' AS type
    FROM {{ source('curated_ecosys', 'curated_ecosys_earnedvaluelog') }} EPL
    LEFT JOIN {{ source('curated_ecosys', 'curated_ecosys_our_dates') }} OD
    ON TO_DATE(EPL.earnedvaluelog_perioddate, 'YYYY-MM-DD') = TO_DATE(OD.reporting_week_end_date, 'MM/DD/YYYY')

), forecast AS (

    SELECT
        EFL.workingforecastlog_costobjectid as costobjectid,
        -- Select the casted value here
        CAST(DATE(EFL.workingforecastlog_period) AS VARCHAR(255)) AS period_date,
        -- CAST(TO_DATE(EFL.workingforecastlog_period, 'DD-Mon-YYYY') AS VARCHAR(255)) AS workingforecastlog_period,
        OD.reporting_week_end_date AS reporting_week_end_date,
        EFL.workingforecastlog_description as description,
        '' AS status,
        EFL.workingforecastlog_createdate as created_date,
        'Forecast' AS type
    FROM {{ source('curated_ecosys', 'curated_ecosys_workingforecastlog') }} EFL
    LEFT JOIN {{ source('curated_ecosys', 'curated_ecosys_our_dates') }} OD
    -- Do not CAST here. Just match by DATE
    ON TO_DATE(EFL.workingforecastlog_period, 'dd-MMM-yyyy') = TO_DATE(OD.reporting_week_end_date, 'MM/DD/YYYY')

), snapshotlog AS (

    SELECT
        ESL.snapshotlog_costobjectid as costobjectid,
        CAST(ESL.snapshotlog_snapshotid AS VARCHAR(255)) AS period_date,
        OD.reporting_week_end_date AS reporting_week_end_date,
        CASE
            WHEN ESL.snapshotlog_description ILIKE '%purged snapshot%' THEN 'Snapshot Purged'
            ELSE 'Snapshot Created'
        END AS description,
        '' AS status,
        CAST(ESL.snapshotlog_createdate AS VARCHAR(255)) AS created_date,
        'Snapshot' AS type
    FROM {{ source('curated_ecosys', 'curated_ecosys_snapshotlog') }} ESL
    LEFT JOIN {{ source('curated_ecosys', 'curated_ecosys_our_dates') }} OD
    ON TO_DATE(ESL.snapshotlog_snapshotid, 'YYYY-MM-DD') = TO_DATE(OD.reporting_week_end_date, 'MM/DD/YYYY')

),

combined_data as (
    SELECT * FROM actuals
    UNION ALL
    SELECT * FROM progress
    UNION ALL
    SELECT * FROM forecast
    UNION ALL
    SELECT * FROM snapshotlog
)
select costobjectid,
period_date,
reporting_week_end_date,
description,
status,
created_date,
type,
'ecosys' as source_system_name,
to_timestamp('{{ execution_date }}', 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date,
to_timestamp('{{ execution_date }}', 'yyyy-MM-dd HH:mm:ss') AS dim_snapshot_date,	
{{run_date}} as dbt_created_date,
{{run_date}} as dbt_updated_date,
{{ generate_load_id(model) }} as dbt_load_id
from combined_data