{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    *,

    cast(DATE_PART('year', datevalue) as integer) as datevalue_year,
    TO_CHAR(datevalue, 'Mon') as datavalue_month_name,
    TO_CHAR(datevalue, 'Mon-YY') as datavalue_month_year_name,
    cast(DATE_PART('Month', datevalue) as integer) as datevalue_month_no
FROM
    (
        SELECT
            project_id,
            requisition_no,
            milestone_id,
            milestone_name,
            milestone_sequence,
            requisition_no||'_'||milestone_name as key,
            COALESCE(project_id, 'default_req') || '_' ||COALESCE(requisition_no, 'default_req') || '_' || COALESCE(milestone_name, 'default_milestone') AS key1,
            CASE
            WHEN milestone_planned_date IS NULL THEN '1970-01-01'
            ELSE milestone_planned_date END AS milestone_planned_date_new,
            CASE
            WHEN milestone_forecast_date IS NULL THEN '1970-01-01'
            ELSE milestone_forecast_date END AS milestone_forecast_date_new,
            CASE
            WHEN milestone_actual_date IS NULL THEN '1970-01-01'
            ELSE milestone_actual_date END AS milestone_actual_date_new
        FROM
            {{ ref('fact_requisition_data_obt') }}
    ) UNPIVOT (
        datevalue FOR milestonendatename IN (
            milestone_planned_date_new,
            milestone_forecast_date_new,
            milestone_actual_date_new
        )
    )