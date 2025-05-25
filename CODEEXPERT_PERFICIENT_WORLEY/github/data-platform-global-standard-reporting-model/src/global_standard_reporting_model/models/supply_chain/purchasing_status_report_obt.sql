{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT *,
    CASE 
        WHEN POSITION('(F)' IN desc_milestone_paf) > 0 
             AND (
                CASE 
                    WHEN POSITION('(P)' IN desc_milestone_paf) > 0 THEN milestone_planned_date
                    WHEN POSITION('(F)' IN desc_milestone_paf) > 0 THEN milestone_forecast_date
                    WHEN POSITION('(A)' IN desc_milestone_paf) > 0 THEN milestone_actual_date
                    ELSE NULL
                END
             ) >= milestone_planned_date 
             AND (
                CASE 
                    WHEN POSITION('(P)' IN desc_milestone_paf) > 0 THEN milestone_planned_date
                    WHEN POSITION('(F)' IN desc_milestone_paf) > 0 THEN milestone_forecast_date
                    WHEN POSITION('(A)' IN desc_milestone_paf) > 0 THEN milestone_actual_date
                    ELSE NULL
                END
             ) <= CURRENT_DATE THEN 'Late'
        WHEN POSITION('(P)' IN desc_milestone_paf) > 0 AND milestone_planned_date >= CURRENT_DATE THEN 'On Track'
        WHEN POSITION('(P)' IN desc_milestone_paf) > 0 AND milestone_planned_date < CURRENT_DATE THEN 'Late'
        WHEN POSITION('(A)' IN desc_milestone_paf) > 0 AND milestone_actual_date <= milestone_planned_date THEN 'Completed Early'
        WHEN POSITION('(A)' IN desc_milestone_paf) > 0 AND milestone_actual_date > milestone_planned_date + 5 THEN 'Completed Late'
        WHEN POSITION('(A)' IN desc_milestone_paf) > 0 THEN 'Completed'
        ELSE NULL
    END AS milestone_status_flag,
    CASE
    when po_forecast_date > po_planned_date then '#F43A4F'
        else null end as fx_red,
    CASE
    when po_planned_date > po_forecast_date then po_forecast_date else po_planned_date
        end as po_plan_date_new,
    cast(DATE_PART('year', ros_date) as integer) as ros_date_year,
    'Qtr '|| DATE_PART('quarter', ros_date) as ros_date_quarter,
    TO_CHAR(ros_date, 'Month') as ros_date_month_name,
    cast(DATE_PART('day', ros_date) as integer) as ros_date_day,
    requisition_no || ' - ' || requisition_title as requisition_no_title,
    cast(tmr_to_po_placement_forecast_duration-tmr_to_po_placement_planned_duration as integer) as duration_progress,
    CASE 
        WHEN POSITION('**ARCH' IN comment) > 0 THEN 
            SUBSTRING(comment, 1, POSITION('**ARCH' IN comment) - 1)
        WHEN POSITION('**arch' IN comment) > 0 THEN 
            SUBSTRING(comment, 1, POSITION('**arch' IN comment) - 1)
        ELSE 
            comment 
        END as  comment_new,
    CASE
        when float_new>=0 then '#409A3C' else '#DA291C' end as df_cf
    FROM(
    SELECT
        project_no,
        project_id,
        current_revision,
        requisition_title,
        originator,
        buyer,
        expediter,
        criticality,
        discipline,
        supplier,
        potential_suppliers,
        budget,
        commitment_value,
        requisition_no,
        po_number,
        ros_date,
        lead_time,
        sub_project,
        float_new,
        milestone_id,
        milestone_name,
        milestone_planned_date,
        milestone_forecast_date,
        milestone_actual_date,
        milestone_duration,
        milestone_float,
        milestone_comment,
        milestone_sequence,
        comment,
        contract_owner,
        contract_holder,
        project_region,
        customer_po,
        rr_discipline,
        client_reference_no,
        case 
        when status = 'Planned' and milestone_planned_date<=current_date
        then 'Behind Schedule' else status end as status,
        ros_date_new,
        actual_date_new,
        dataset_refreshed_date,
        milestone_lead_time,
        cast(avg_lt_planned as integer) as avg_lt_planned,
        cast(avg_lt_forecast as integer) as avg_lt_forecast,
        cast(tmr_issued_to_rfq_closure as integer) as tmr_issued_to_rfq_closure ,
        cast(rfq_closure_to_tbe_approval as integer) as rfq_closure_to_tbe_approval,
        cast(tmr_received_to_po_issued as integer) as tmr_received_to_po_issued,
        cast(po_issued_to_po_delivered as integer) as po_issued_to_po_delivered,
        po_planned_date,
        po_forecast_date,
        tmr_planned_date,
        tmr_forecast_date,
        case
        WHEN milestone_planned_date<=current_date then 'Yes'
        else 'No' end as planned_dates_in_past,
        requisition_no||'_'||milestone_name as key,
        COALESCE(project_id, 'default_req') || '_' ||COALESCE(requisition_no, 'default_req') || '_' || COALESCE(milestone_name, 'default_milestone') AS key1,
        DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date) as variance,
        case
        WHEN DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)>=0  then 'Positive or Zero Variance'
        WHEN DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)<=1 and 
            DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)>=-7 then 'Negative One Week Variance'
        WHEN DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)<=-8 and 
            DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)>=-14 then 'Negative Two Week Variance'
        WHEN DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)<=-15 and 
            DATEDIFF(DAY,milestone_forecast_date,milestone_planned_date)>=-21 then 'Negative Three Week Variance'
        else 'Negative More than Three week Variance' end as variance_filter,
        case
        WHEN float_new>0 then 'Positive or Zero Float'
        WHEN float_new<0 then 'Negative Float'
        WHEN float_new is null then 'Not Available'
        else 'Positive or Zero Float' end as positive_negative_float,
        case
        WHEN (milestone_forecast_date>milestone_planned_date) and milestone_planned_date is not null then '#F43A4F'
        else '#003645' end as forecastdatecf,
        case
        WHEN milestone_forecast_date<current_date and milestone_actual_date is null and milestone_forecast_date is not null then 1
        else 0 end as forecastdateicons,
        project_id||'-'||milestone_name as milestone_key,
        CASE
        when milestone_actual_date is null then 'Yes' else 'No' end as actual_date_filter,
        case 
        WHEN milestone_actual_date is null then 'Open' else 'Completed' end as complete_open,
        case 
        WHEN milestone_actual_date is null and milestone_planned_date < current_date and
            milestone_planned_date is not null then 'Delayed'
        WHEN milestone_actual_date is null and milestone_planned_date >= current_date then 'Planned'
        WHEN milestone_actual_date is not null and milestone_actual_date>milestone_planned_date then 'Completed/Late'
        WHEN milestone_actual_date is not null and milestone_actual_date<=milestone_planned_date then 'Completed'
        else null end as milestone_status_detail,
        CASE 
        WHEN milestone_actual_date IS NULL AND milestone_planned_date = milestone_forecast_date 
        THEN requisition_title||'-'||milestone_name|| '- (P)'
        WHEN milestone_actual_date IS NULL AND milestone_planned_date < milestone_forecast_date 
        THEN requisition_title||'-'||milestone_name || '- (F)'
        WHEN milestone_actual_date IS NULL AND milestone_planned_date > milestone_forecast_date 
        THEN requisition_title||'-'||milestone_name || '- (P)'
        WHEN milestone_actual_date IS NOT NULL THEN requisition_title||'-'||milestone_name || '- (A)'
        ELSE '' END AS desc_milestone_paf,
        CASE 
        WHEN tmr_planned_date IS NULL OR po_planned_date IS NULL THEN NULL
        ELSE DATEDIFF(day, tmr_planned_date, po_planned_date)
            END AS tmr_to_po_placement_planned_duration,
        CASE 
        WHEN tmr_forecast_date IS NULL OR po_forecast_date IS NULL THEN NULL
        ELSE DATEDIFF(day, tmr_forecast_date, po_forecast_date)
            END AS tmr_to_po_placement_forecast_duration
        
    FROM
        (
            SELECT
                DISTINCT t1.*,
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
                        {{ ref('fact_requisition_data_obt') }} psr 
                ) t1
                LEFT JOIN (
                    SELECT
                        project_id,
                        requisition_no,
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
                                project_id,
                                requisition_no,
                                milestone_sequence,
                                milestone_actual_date
                            FROM
                                {{ ref('fact_requisition_data_obt') }} 
                        ) PIVOT (
                            MAX(milestone_actual_date) FOR milestone_sequence IN (4000, 9000, 11000, 13000, 27000, 31000)
                        )
                ) t2 ON t1.project_id = t2.project_id
                AND t1.requisition_no = t2.requisition_no
                LEFT JOIN (
                    SELECT
                        *
                    FROM
                        {{ ref('psr_po') }} 
                ) t3 ON t1.project_id = t3.project_id
                AND t1.requisition_no = t3.requisition_no
                LEFT JOIN (
                    SELECT
                        *
                    FROM
                        {{ ref('psr_tmr') }} 
                ) t4 ON t1.project_id = t4.project_id
                AND t1.requisition_no = t4.requisition_no
        )
    )