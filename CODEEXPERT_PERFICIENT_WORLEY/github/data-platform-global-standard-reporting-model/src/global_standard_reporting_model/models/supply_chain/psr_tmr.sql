{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    project_id,
    requisition_no,
    grouped_milestone_code,
    discipline,
    MAX(milestone_planned_date) as tmr_planned_date,
    MAX(milestone_forecast_date) as tmr_forecast_date
FROM
(
        SELECT
            *,
            CASE
            WHEN milestone_sequence IN (1000, 4000, 4100, 5000) THEN 'TMR ISSUED FOR INQUIRY'
            WHEN milestone_sequence IN (16500, 23500, 24000, 26000, 27000, 27005, 27010, 27100) THEN 'PO ISSUED'
            WHEN milestone_sequence = 400 THEN 'EOI'
            WHEN milestone_sequence IN (200, 500, 600, 990, 995) THEN 'PREQUAL & BID LIST'
            WHEN milestone_sequence IN (5100, 5500, 5600, 9500) THEN 'STRATEGY / IPP'
            WHEN milestone_sequence IN (9000, 9100, 11050) THEN 'RFQ'
            WHEN milestone_sequence IN (10000, 11000, 11040, 11060) THEN 'BID RECEIPT AND DISTRIBUTION'
            WHEN milestone_sequence IN (11050, 11500) THEN 'SHORTLISTING'
            WHEN milestone_sequence IN (11010,11020,11070,11090,12000,12100,13000,13100,14100) THEN 'TBE APPROVED'
            WHEN milestone_sequence IN (13020, 17000) THEN 'CBA / RFA ISSUED FOR APPROVAL'
            WHEN milestone_sequence IN (13010,13030,14000,14200,14500,15000,15100,16000,16100
                                        ,17000,18000,18500,22500,23900,23910) THEN 'CBA / RFA APPROVED'
            WHEN milestone_sequence IN (13510,14020,16510,18020,18025,18100,22000,23000,23100) THEN 'TMR ISSUED FOR PURCHASE'
            ELSE milestone_name END as grouped_milestone_code,
            CASE
            when milestone_planned_date is null
            and milestone_forecast_date is null then 1
            else 0 end as flag
        FROM
            {{ ref('fact_requisition_data_obt') }}
    )
WHERE
    grouped_milestone_code = 'TMR ISSUED FOR INQUIRY' 
    and requisition_no is not null
    and flag = 0
GROUP BY
    project_id,
    requisition_no,
    grouped_milestone_code,
    discipline