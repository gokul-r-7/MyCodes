{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_finance_contingency/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}


select
    fin.findata_costobjectid as cost_object_id,
    cast(fin.findata_transactiondate as date) as transaction_date,
    pvt.contingency_category,
    cast((pvt.contigency_category_value / 100.00) as float) as contingency_weighting,
    fin.findata_projectinternalid as project_internal_id,
    fin.execution_date,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    {{ source('curated_ecosys','curated_ecosys_findata')}} fin
    join (
        select
            findata_costobjectid,
            findata_transactiondate,
            case
            when category = 'findata_contingencybreakdownservice' then 'Service'
            when category = 'findata_contingencybreakdownproc' then 'Procurement'
            when category = 'findata_contingencybreakdownconstruction' then 'Construction'
            when category = 'findata_contingencybreakdownother' then 'Other'
            when category = 'findata_contingencybreakdowncloseout' then 'CloseOut'
            else null end as contingency_category,
            contigency_category_value
        from
            (
                select
                    *
                from
                    {{ source('curated_ecosys','curated_ecosys_findata')}}
                where
                    findata_contingencybreakdownother <> '319020-00381'
                    and is_current = 1
            ) z unpivot (
                contigency_category_value for category in (
                    findata_contingencybreakdownservice,
                    findata_contingencybreakdownproc,
                    findata_contingencybreakdownconstruction,
                    findata_contingencybreakdownother,
                    findata_contingencybreakdowncloseout
                )
            )
    ) as pvt
        on fin.findata_costobjectid = pvt.findata_costobjectid
        and fin.is_current = 1
        and fin.findata_transactiondate = pvt.findata_transactiondate
    join {{ source('curated_ecosys','curated_ecosys_calendar')}} cal
        on date(fin.findata_transactiondate) = cal.calendar_enddate
        and cal.calendar_datetype = 'Month'
where
    fin.is_current = 1