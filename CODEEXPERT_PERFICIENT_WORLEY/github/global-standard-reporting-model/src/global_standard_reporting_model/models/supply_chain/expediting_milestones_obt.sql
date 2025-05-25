{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    po.*,
    CASE
    when po.milestone_actual_date is null then 'Open'
    else 'Completed' end as "Milestone Status",
    CASE
    when po.milestone_actual_date is null then (
        CASE
        when DATEDIFF(
            day,
            CURRENT_DATE,
            (
                CASE
                when po.baseline_date is null then po.milestone_forecast_complete_date
                else po.baseline_date end
            )
        ) > 0 then 'Late'
        else 'On Time' end
    )
    else null end as "Open Milestone Status",
    CASE
    when po.milestone_actual_date is not null then (
        CASE
        when DATEDIFF(
            day,
            po.milestone_actual_date,
            (
                CASE
                when po.baseline_date is null then po.milestone_forecast_complete_date
                else po.baseline_date end
            )
        ) > 0 then 'Late'
        else 'On Time' end
    )
    else null end as "Completed Milestone Status",
    CASE
    when po.milestone_actual_date is not null then 'Completed'
    when po.milestone_forecast_complete_date >= CURRENT_DATE
    and po.milestone_forecast_complete_date > po.baseline_date then 'Planned'
    when po.milestone_forecast_complete_date >= CURRENT_DATE then 'Planned'
    when po.milestone_forecast_complete_date < CURRENT_DATE
    and po.baseline_date is not null then 'Overdue'
    when po.baseline_date is null then 'No Planned Date'
    else null end as "Status",
    esr.delivery_type,
    tmr.discipline,
    pro.group_l1,
    pro.category_l2,
    pro.sub_category_l3,
    pro.family_l4,
    pro.unspsc as worley_code_l5,
    pro.worley_project_office as office_location
FROM
     {{ ref('fact_po_header_obt') }} po
    LEFT JOIN (
        SELECT
            po_number,
            delivery_type,
            ROW_NUMBER() OVER (
                PARTITION BY po_number
                ORDER BY
                    delivery_type
            ) AS rn
        FROM
             {{ ref('fact_esr_data_obt') }}
    ) esr ON po.po_number = esr.po_number
    AND esr.rn = 1
    LEFT JOIN (
        SELECT
            tmr_number,
            discipline,
            ROW_NUMBER() OVER (
                PARTITION BY tmr_number
                ORDER BY
                    discipline
            ) AS rn
        FROM
             {{ ref('fact_tmr_data_obt') }}
    ) tmr ON po.tmr_number = tmr.tmr_number
    AND tmr.rn = 1
    LEFT JOIN (
        SELECT
            po_number,
            group_l1,
            category_l2,
            sub_category_l3,
            family_l4,
            unspsc,
            worley_project_office,
            ROW_NUMBER() OVER (
                PARTITION BY po_number
                ORDER BY
                    group_l1,
                    category_l2,
                    sub_category_l3,
                    family_l4,
                    unspsc,
                    worley_project_office
            ) AS rn
        from
             {{ ref('erm_procurement_obt') }}
    ) pro ON po.po_number = pro.po_number
    AND pro.rn = 1