{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )
}}

SELECT *,
    CAST(srn_date_new as date) as srn_date1
    FROM(
    SELECT
        esr.*,
        CASE
        WHEN POSITION('**ARCH' IN esr.expediting_comments) > 0 THEN SUBSTRING(
            esr.expediting_comments,
            1,
            POSITION('**ARCH' IN esr.expediting_comments) - 1
        )
        WHEN POSITION('**arch' IN esr.expediting_comments) > 0 THEN SUBSTRING(
            esr.expediting_comments,
            1,
            POSITION('**arch' IN esr.expediting_comments) - 1
        )
        WHEN POSITION('** ARCH' IN esr.expediting_comments) > 0 THEN SUBSTRING(
            esr.expediting_comments,
            1,
            POSITION('** ARCH' IN esr.expediting_comments) - 1
        )
        WHEN POSITION('ARCH' IN esr.expediting_comments) > 0 THEN SUBSTRING(
            esr.expediting_comments,
            1,
            POSITION('ARCH' IN esr.expediting_comments) - 1
        )
        ELSE esr.expediting_comments END AS expediting_comments_new,
        tam.unspsc,
        tam.group_l1,
        tam.category_l2,
        tam.sub_category_l3,
        tam.family_l4,
        tam.commodity_l5,
        tam.worley_project_office as office_location,
        case
        when esr.forecast_arrival_on_site < current_date then esr.forecast_arrival_on_site
        else null end as delayed,
        CASE
        WHEN esr.total_received > 0 THEN CASE
        WHEN (esr.order_qty - esr.total_received) > 0 THEN 'Partially Received'
        ELSE 'Received' END
        ELSE 'Not Received' END as line_received_status,
        case
        when prs.total_received_qty > 0 then case
        when (prs.total_order_qty - prs.total_received_qty) > 0 then 'Partially Received'
        else 'Received' END
        ELSE 'Not Received' END as po_received_status,
        case
        when esr.arrival_on_site_actual is null
        and esr.exp_ros <= current_date then 'Late'
        when esr.arrival_on_site_actual is null
        and esr.exp_ros > current_date then 'On Track'
        when esr.arrival_on_site_actual > esr.exp_ros then 'Received Late'
        else 'On Time' end as ros_vs_aaos,
        datediff(
            day,
            forecast_delivery_date,
            contract_delivery_date
        ) as new_delivery_float,
        case
        when esr.order_qty = 0
        or esr.total_received is null then 0
        else esr.total_received / esr.order_qty end as received_perct,
        case
        when outstanding_qty > 0 then 'QTY Outstanding'
        else 'Received Complete' end as received_status,
        case
        when esr.srn_qty = 0
        or esr.exp_qty is null then 0
        else esr.exp_qty / esr.srn_qty end as release_perct,
        case
        when esr.srn_qty is null
        and esr.mrr_qty is null then 'Not Released'
        when esr.srn_qty is null
        and esr.mrr_qty is not null then 'Released/No SRN'
        else 'Released' end as release_status,
        cast(prs.total_order_qty as integer) as total_order_qty,
        cast(prs.total_received_qty as integer) as total_received_qty,
        cast(prs.total_order_qty as integer) - cast(prs.total_received_qty as integer) as total_outstanding_qty,
        tmr.discipline,
        case
        when esr.srn_date ='' then null else esr.srn_date end as srn_date_new,
        case 
        when esr.earliest_forecast_delivery_date > esr.earliest_contract_delivery_date then '#F43A4F' 
        else null end as fx_red,
        case 
        when esr.forecast_arrival_on_site > esr.exp_ros then 'Late to ROS'
        when esr.forecast_delivery_date > esr.contract_delivery_date and esr.ros_float>=0 
        then 'Late to Contractual Date but meets ROS'
        when esr.contract_delivery_date = esr.forecast_delivery_date then 'On Schedule Contractual Date'
        else null end as legend_expediting,
        case
        when esr.forecast_arrival_on_site > current_date then esr.forecast_arrival_on_site
        else null end as forecast
    from
        {{ ref('fact_esr_data_obt') }} esr
        LEFT JOIN (
            SELECT
                DISTINCT po_number,
                unspsc,
                group_l1,
                category_l2,
                sub_category_l3,
                family_l4,
                commodity_l5,
                worley_project_office
            FROM
                {{ ref('erm_procurement_obt') }}
        ) tam ON tam.po_number = esr.po_number
        LEFT JOIN (
            SELECT
                SUM(order_qty) as total_order_qty,
                SUM(total_received) as total_received_qty,
                project_id,
                po_number
            FROM
                {{ ref('fact_esr_data_obt') }}
            GROUP by
                project_id,
                po_number
        ) prs on prs.project_id = esr.project_id
        and prs.po_number = esr.po_number 
        LEFT JOIN (
            SELECT DISTINCT
                tmr_number,
                discipline
            FROM {{ ref('fact_tmr_data_obt') }}
        ) tmr on tmr.tmr_number = esr.tmr_number
)