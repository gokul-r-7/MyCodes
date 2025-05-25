{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_finance_outstanding/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

SELECT
    fin.findata_costobjectid as COST_OBJECT_ID,
    cast(fin.findata_transactiondate as date) as TRANSACTION_DATE,
    cast(fin.findata_recrevenue as float) as REC_REVENUE,
    cast(fin.findata_invtodate as float) as INVD_TO_DATE,
    cast(fin.findata_wip as float) as WIP,
    cast(fin.findata_recdtodate as float) as RECD_TO_DATE,
    cast(fin.findata_outstandingcurrent as float) as OUTSTANDING_CURRENT,
    cast(fin.findata_outstanding130 as float) as OUTSTANDING_1_TO_30_DAYS,
    cast(fin.findata_outstanding3160 as float) as OUTSTANDING_31_TO_60_DAYS,
    cast(fin.findata_outstanding61above as float) as OUTSTANDING_61_PLUS_DAYS,
    cast(fin.findata_dso as float) as DSO,
    fin.findata_projectinternalid as PROJECT_INTERNAL_ID,
    fin.execution_date,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM
    {{ source('curated_ecosys','curated_ecosys_findata')}} fin
    JOIN {{ source('curated_ecosys','curated_ecosys_calendar')}} cal ON date(fin.findata_transactiondate) = cal.calendar_enddate
    and cal.calendar_datetype = 'Month'
    and fin.is_current = 1
WHERE
    fin.is_current = 1
    and findata_dso <> 'miguel.lara@worley.com'