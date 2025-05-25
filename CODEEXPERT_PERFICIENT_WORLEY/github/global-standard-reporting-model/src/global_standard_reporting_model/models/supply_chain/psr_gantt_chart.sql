{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    *,
    CASE
    WHEN date_key BETWEEN tmr_planned_date
    and po_planned_date THEN 1
    WHEN date_key BETWEEN po_plan_date_new
    and po_forecast_date_new THEN 1
    ELSE 0 end as duration,
    CASE
    WHEN po_planned_date < po_forecast_date then 'fx'
    else null end as CF
FROM
    (
        SELECT
            DISTINCT t2.date_key,
            t1.project_code,
            t1.requistion_no || '-' || t1.requisition_title as "Requisition No/Title",
            t1.requistion_no,
            t1.requisition_title,
            t1.milestone_name,
            t1.Buyer,
            t1.project_region,
            t1.sub_project,
            t1.discipline,
            t1.supplier,
            t1.milestone_planned_date,
            t1.milestone_forecast_date,
            t1.milestone_actual_date,
            t1.tmr_planned_date,
            t1.po_planned_date,
            t1.po_forecast_date,
            CASE
            WHEN t1.po_planned_date > t1.po_forecast_date then t1.po_forecast_date
            ELSE t1.po_planned_date end as po_plan_date_new,
            CASE
            WHEN t1.po_planned_date < t1.po_forecast_date then t1.po_forecast_date
            ELSE t1.po_planned_date end as po_forecast_date_new
        FROM
            {{ ref('psr_s_curve_obt') }} t1
            LEFT JOIN {{ ref('dim_date_new') }} t2 ON 1 = 1
        WHERE
            t2.date_key between t1.tmr_planned_date
            AND po_forecast_date_new
    ) 