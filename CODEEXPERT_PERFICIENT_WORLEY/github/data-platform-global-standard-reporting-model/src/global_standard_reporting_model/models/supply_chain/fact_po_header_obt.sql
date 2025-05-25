{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    *,
    CASE
    when milestone_actual_date is null then 'Open'
    else 'Completed' end as milestone_status,
    CASE
    when milestone_actual_date is null then (
        CASE
        when DATEDIFF(
            day,
            CURRENT_DATE,
            (
                CASE
                when baseline_date is null then milestone_forecast_complete_date
                else baseline_date end
            )
        ) > 0 then 'Late'
        else 'On Time' end
    )
    else null end as open_milestone_status,
    CASE
    when milestone_actual_date is not null then (
        CASE
        when DATEDIFF(
            day,
            milestone_actual_date,
            (
                CASE
                when baseline_date is null then milestone_forecast_complete_date
                else baseline_date end
            )
        ) > 0 then 'Late'
        else 'On Time' end
    )
    else null end as completed_milestone_status,
    CASE
    when milestone_actual_date is not null then 'Completed'
    when milestone_forecast_complete_date >= CURRENT_DATE
    and milestone_forecast_complete_date > baseline_date then 'Planned'
    when milestone_forecast_complete_date >= CURRENT_DATE then 'Planned'
    when milestone_forecast_complete_date < CURRENT_DATE
    and baseline_date is not null then 'Overdue'
    when baseline_date is null then 'No Planned Date'
    else null end as status,
    CASE
    WHEN POSITION('**ARCH' IN expediting_comments) > 0 THEN SUBSTRING(
        expediting_comments,
        1,
        POSITION('**ARCH' IN expediting_comments) - 1
    )
    WHEN POSITION('**arch' IN expediting_comments) > 0 THEN SUBSTRING(
        expediting_comments,
        1,
        POSITION('**arch' IN expediting_comments) - 1
    )
    WHEN POSITION('** ARCH' IN expediting_comments) > 0 THEN SUBSTRING(
        expediting_comments,
        1,
        POSITION('** ARCH' IN expediting_comments) - 1
    )
    WHEN POSITION('ARCH' IN expediting_comments) > 0 THEN SUBSTRING(
        expediting_comments,
        1,
        POSITION('ARCH' IN expediting_comments) - 1
    )
    ELSE expediting_comments END AS expediting_comments_new,
    CASE
    WHEN earliest_ros IS NOT NULL
    AND earliest_forecast_arrival_date IS NOT NULL THEN CASE
    WHEN (earliest_ros - earliest_forecast_arrival_date) >= 0 THEN 'ROS On Time'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -15
    AND -1 THEN '-1 to -15 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -30
    AND -16 THEN '-16 to -30 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -60
    AND -31 THEN '-31 to -60 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -90
    AND -61 THEN '-61 to -90 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -180
    AND -91 THEN '-91 to -180 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) BETWEEN -365
    AND -181 THEN '-181 to -365 Days'
    WHEN (earliest_ros - earliest_forecast_arrival_date) <= -365 THEN '> -365 Days' END
    ELSE NULL END AS ros_float,
    CASE
    when milestone_forecast_start_date > baseline_date
    and baseline_date is not null then '#F43A4F'
    else '#000000' end as forecaststartdate_font,
    CASE
    when milestone_forecast_complete_date > baseline_date
    and baseline_date is not null then '#F43A4F'
    else '#000000' end as forecastcompletedate_font,
    CASE
    when milestone_forecast_start_date < CURRENT_DATE
    and milestone_actual_date is null
    and milestone_forecast_start_date is not null then 1
    else 0 end as forecaststartdate_icons,
    CASE
    when milestone_forecast_complete_date < CURRENT_DATE
    and milestone_actual_date is null
    and milestone_forecast_complete_date is not null then 1
    else 0 end as forecastcompletedate_icons,
    CASE
    when milestone_forecast_complete_date < CURRENT_DATE
    and milestone_forecast_complete_date is not null then 'Yes'
    else 'No' end as forecast_date_in_past,
    case
    when milestone_actual_date is null
    and milestone = 'Received at Jobsite' then 'No'
    when milestone_actual_date is not null
    and milestone = 'Received at Jobsite' then 'Yes'
    else null end as po_closed,
    cast(earliest_ros - earliest_forecast_arrival_date as integer) as ros_float_value,
    case
    when milestone_actual_date is null then
    datediff(day , baseline_date , milestone_forecast_complete_date)
    else datediff(day,baseline_date, milestone_actual_date) end as baseline_float,
    case
    when milestone_actual_date is null then
    datediff(day ,  milestone_forecast_complete_date,current_date)
    else datediff(day,milestone_forecast_complete_date,milestone_actual_date) end as forecast_to_completion,
    CASE
    when milestone_actual_date is null and milestone_forecast_complete_date < CURRENT_DATE then '#F43A4F'
    ELSE '#003645' end as forecast_complete_font,
    COALESCE(project_id, 'default_req') || '_' ||COALESCE(po_number, 'default_req')  AS project_po
FROM
    (
        SELECT
            DISTINCT 
            cast(projet_no as integer) as project_no,
            cast(project as varchar(100)) as project_id,
            sub_project,
            tmr_number,
            purchase_order_no as po_number,
            cast(latest_revision as integer) as latest_rev,
            customer_po_no as customer_po,
            client_reference as client_reference_no,
            description,
            supplier,
            buyer,
            expediter,
        null as
            inspector,
            po_first_issue_date as po_1st_issue,
            issue_date as latest_issue_date,
            total_line_items,
            last_contact_date,
            next_contact_date,
            earliest_contract_dlv_date as earliest_contract_delivery_date,
            earliest_forecast_dlv_date as earliest_forecast_delivery_date,
            earliest_ros,
            earliest_forecast_arrival_date as earliest_forecast_arrival_date,
            incoterm,
            trans_terms,
            cast(expediting_level as integer) as expediting_level,
            cast(inspection_level as integer) as inspection_level,
            contractual_dlv_date as contract_delivery_date,
            expedite_comment as expediting_comments,
            ros_date as ros_date,
            items_not_released,
            released_items as total_items_released,
            milestones as milestone,
            cast(milestone_seq as integer) as milestone_seq,
            baseline as baseline_date,
            forecast_start as milestone_forecast_start_date,
            forecast_complete as milestone_forecast_complete_date,
            actual_date as milestone_actual_date,
            cast(deadline_float as integer) as milestone_float,
            cast(duration_before_milestone as integer) as duration_before_milestone,
            cast(milestone_duration as integer) as milestone_duration,
            remark as remarks,
            template_no,
            cast(template as integer) as template,
            template_name,
            tam.unspsc,
            tam.group_l1,
            tam.category_l2,
            tam.sub_category_l3,
            tam.family_l4,
            tam.commodity_l5,
            tam.worley_project_office as office_location,
            etl_load_date as dataset_refershed_date,
            podates.po_acknowledgement
        from
            {{ source('supply_chain_dim', 'transformed_po_hdr_details_cext_w_exp_sch_api_nv') }} po
            INNER JOIN (
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
            ) tam ON tam.po_number = po.purchase_order_no
            left JOIN
            (WITH PO_Dates AS (
                    SELECT 
                        description as descr,
                        MIN(CASE WHEN milestones = 'PO Issued' THEN actual_date ELSE NULL END) AS PODate,
                        MIN(CASE WHEN milestones = 'PO Ack Received' THEN actual_date ELSE NULL END) AS POACKDate
                    FROM {{ source('supply_chain_dim', 'transformed_po_hdr_details_cext_w_exp_sch_api_nv') }}
                    GROUP BY description
                ),
                DiffDays AS (
                    SELECT
                        descr,
                        PODate,
                        POACKDate,
                        DATE_PART('day', CURRENT_DATE) - DATE_PART('day', PODate) AS Diffdays
                    FROM PO_Dates
                )
                SELECT 
                    descr,
                    CASE 
                        WHEN POACKDate IS NOT NULL THEN 'PO Acknowledged'
                        WHEN PODate IS NULL THEN 'PO Not Issued'
                        ELSE CASE
                            WHEN Diffdays <= 5 THEN '<5 Days'
                            WHEN Diffdays > 5 AND Diffdays <= 10 THEN '6-10 Days'
                            WHEN Diffdays > 10 AND Diffdays <= 20 THEN '11-20 Days'
                            ELSE '>20 Days'
                        END
                    END AS PO_Acknowledgement
                FROM DiffDays) podates on podates.descr = po.description
                    )
