{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_unallocated_actuals/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}
select
    project_id,
    cost_object_hierarchy_path_id,
    pa_date,
    cost_type_id,
    snapshot_date,
    actual_revenue,
    actual_cost,
    actual_hours,
    cost_type_internal_id,
    control_account_internal_id,
    project_internal_id,
    CAST({{ run_date }} AS DATE) as created_date,  
    CAST({{ run_date }} AS DATE) as updated_date,
    {{ generate_load_id(model) }} as load_id

from
    (
        select
            hdr.header_projectid as PROJECT_ID,
            dt.cost_object_hierarchy_path_id as COST_OBJECT_HIERARCHY_PATH_ID,
            dt.PA_DATE as PA_DATE,
            dt.cost_type_id as COST_TYPE_ID,
            to_date(SUBSTRING(dt.PA_DATE, 7, 11), 'dd-MMM-yyyy') as SNAPSHOT_DATE,
            cast(dt.alternate_actual_costs as float) as ACTUAL_REVENUE,
            cast(dt.actual_costs as float) as ACTUAL_COST,
            cast(dt.actual_units as float) as ACTUAL_HOURS,
            dt.ctinternalid as COST_TYPE_INTERNAL_ID,
            dt.controlaccountinternalid as CONTROL_ACCOUNT_INTERNAL_ID,
            dt.projectinternalid as PROJECT_INTERNAL_ID,
            CURRENT_DATE AS execution_date,
            CURRENT_DATE AS created_date,
            CURRENT_DATE AS updated_date,
            {{ generate_load_id(model) }} as load_id
        FROM
            {{ source('curated_ecosys', 'curated_header') }} hdr
            JOIN {{ ref('transformed_vw_ecosys_secorg_ecodata_latest') }} dt ON dt.projectinternalid = hdr.header_projectinternalid
            and hdr.is_current = 1
            and dt.is_current = 1 and dt.status = 'active'
            and concat(dt.PROJECT_ID, dt.snapshot_id) not in(
                select
                    distinct concat(
                        snapshotlog_costobjectid,
                        snapshotlog_snapshotintid
                    ) as objectid
                from
                    {{ ref('transformed_ecosys_status') }} 
            ) ---and dt.STATUS = 'Active' - added subquery in place of status
            JOIN {{ source('curated_ecosys', 'curated_ecosys_calendar') }} cal ON cal.calendar_enddate = to_date(SUBSTRING(dt.PA_DATE, 7, 11), 'dd-MMM-yyyy')
            and cal.calendar_datetype = 'Month'
        WHERE
            dt.cost_type_id = 'X999'
    ) z
ORDER BY
    PROJECT_INTERNAL_ID,
    CONTROL_ACCOUNT_INTERNAL_ID,
    SNAPSHOT_DATE
