{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    esr.*,
    CASE
    when esr.Total_Received > 0 then (
        CASE
        when (esr.order_qty - esr.Total_Received) > 0 then 'Partially Received'
        else 'Received' end
    )
    else 'Not Received' end as "Lines Received Status",
    DATEDIFF(
        day,
        esr.forecast_delivery_date,
        esr.contract_delivery_date
    ) as "New - Delivery Float",
    CASE
    when esr.Arrival_On_Site_Actual is null
    and esr.exp_ros <= CURRENT_DATE then 'Late'
    when esr.Arrival_On_Site_Actual is null
    and esr.exp_ros > CURRENT_DATE then 'On Track'
    when esr.Arrival_On_Site_Actual > esr.exp_ros then 'Received Late'
    else 'On Time' end as "ROS vs AAOS",
    CASE
    when esr.order_qty =0 then 0 
    else (esr.Total_Received / esr.order_qty) end as "Received%",
    CASE
    when esr.Outstanding_Qty > 0 then 'QTY Outstanding'
    else 'Received Complete' end as "Received Status",
    CASE
    when esr.SRN_Qty = 0 then 0
    else esr.Exp_Qty / esr.SRN_Qty end as "Release%",
    CASE
    when esr.SRN_Qty is null
    and esr.mrr_qty is null then 'Not Released'
    when esr.SRN_Qty is null
    and esr.mrr_qty is not null then 'Released/No SRN'
    else 'Released' end as "Release Status",
    tmr.discipline,
    pro.group_l1,
    pro.category_l2,
    pro.sub_category_l3,
    pro.family_l4,
    pro.unspsc as worley_code_l5,
    pro.worley_project_office as office_location
FROM
     {{ ref('fact_esr_data_obt') }} esr
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
    ) tmr ON esr.tmr_number = tmr.tmr_number
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
    ) pro ON esr.po_number = pro.po_number
    AND pro.rn = 1