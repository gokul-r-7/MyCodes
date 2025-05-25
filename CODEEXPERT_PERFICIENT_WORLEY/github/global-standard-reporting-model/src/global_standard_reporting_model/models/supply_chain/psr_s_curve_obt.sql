{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}
SELECT
    t1.*,
    t2.tmr_issued_to_rfq_closure,
    t2.rfq_closure_to_tbe_approval,
    t2.tmr_received_to_po_issued,
    t2.po_issued_to_po_delivered,
    t3.po_planned_date,
    t3.po_forecast_date,
    t4.tmr_planned_date,
    t4.tmr_forecast_date
FROM
    (
        SELECT
            psr.*,
            dd.*,
            CASE
            WHEN EXTRACT(
                dow
                FROM
                    dd.date_key
            ) = 0 THEN dd.date_key
            ELSE dd.date_key + (
                7 - EXTRACT(
                    dow
                    FROM
                        dd.date_key
                )
            ) END AS weekend_date_sunday,
            TO_NUMBER(TO_CHAR(dd.date_key, 'YYYYMMDD'), '99999999') as date_sort,
            CASE
            WHEN milestone_actual_date IS NOT NULL THEN DATEDIFF(day, milestone_actual_date, milestone_planned_date)
            ELSE NULL END AS avg_lt_planned,
            CASE
            WHEN milestone_actual_date IS NOT NULL THEN DATEDIFF(
                day,
                milestone_actual_date,
                milestone_forecast_date
            )
            ELSE NULL END AS avg_lt_forecast
        FROM
            (
                SELECT
                    *,
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
            ) psr
            LEFT JOIN {{ ref('dim_date_new') }} dd ON psr.datevalue = dd.date_key
    ) t1
    LEFT JOIN (
        SELECT
            project_code,
            requistion_no,
            CASE
            WHEN "9000" IS NULL
            OR "11000" IS NULL THEN NULL
            ELSE DATEDIFF(day, "9000", "11000") END AS tmr_issued_to_rfq_closure,
            CASE
            WHEN "11000" IS NULL
            OR "13000" IS NULL THEN NULL
            ELSE DATEDIFF(day, "11000", "13000") END AS rfq_closure_to_tbe_approval,
            CASE
            WHEN "27000" IS NULL
            OR "4000" IS NULL THEN NULL
            ELSE DATEDIFF(day, "4000", "27000") END AS tmr_received_to_po_issued,
            CASE
            WHEN "27000" IS NULL
            OR "31000" IS NULL THEN NULL
            ELSE DATEDIFF(day, "27000", "31000") END AS po_issued_to_po_delivered
        FROM
            (
                SELECT
                    project_code,
                    requistion_no,
                    milestone_sequence,
                    milestone_actual_date
                FROM
                    {{ ref('fact_requisition_data_obt') }}
            ) PIVOT (
                MAX(milestone_actual_date) FOR milestone_sequence IN (4000, 9000, 11000, 13000, 27000, 31000)
            )
    ) t2 ON t1.project_code = t2.project_code
    AND t1.requistion_no = t2.requistion_no
    LEFT JOIN (
        SELECT * FROM {{ ref('psr_po') }}
    ) t3  ON t1.project_code = t3.project_code
    AND t1.requistion_no = t3.requistion_no
    LEFT JOIN (
        SELECT * FROM {{ ref('psr_tmr') }}
    ) t4  ON t1.project_code = t4.project_code
    AND t1.requistion_no = t4.requistion_no