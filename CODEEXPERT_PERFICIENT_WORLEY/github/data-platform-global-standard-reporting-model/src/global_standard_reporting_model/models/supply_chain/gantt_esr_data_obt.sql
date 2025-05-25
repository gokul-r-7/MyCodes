{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )
}}

SELECT * ,
CAST(prog as NUMERIC(38,4)) as prog1
FROM(
SELECT
    *,
    case
    when cast(second_dur as decimal(38,4)) = 0
    or first_dur is null then 0
    else cast(cast(first_dur as decimal(38,4)) / second_dur as decimal(38,4)) end as prog
from
    (
        SELECT
            *,
            DATEDIFF(day, po_1st_issue, earliest_contract_delivery_date) as first_dur,
            DATEDIFF(day, po_1st_issue, earliest_forecast_delivery_date) as second_dur,
            DATEDIFF(
                day,
                earliest_forecast_delivery_date,
                earliest_contract_delivery_date
            ) as float1,
            po_number || '-' || description as po_number_description
        FROM
            (
                SELECT
                    project_id,
                    tmr_number,
                    po_number,
                    supplier,
                    COALESCE(project_id, 'default_req') || '_' || COALESCE(po_number, 'default_req') AS project_po,
                    MAX(po_1st_issue) as po_1st_issue,
                    MAX(earliest_forecast_delivery_date) as earliest_forecast_delivery_date,
                    MAX(earliest_forecast_arrival_date) as earliest_forecast_arrival_date,
                    MAX(earliest_contract_delivery_date) as earliest_contract_delivery_date,
                    MAX(delivery_float) as delivery_float,
                    MAX(forecast_delivery_date) as forecast_delivery_date,
                    MAX(contract_delivery_date) as contract_delivery_date,
                    MAX(forecast_arrival_on_site) as forecast_arrival_on_site,
                    MAX(ros_float) as ros_float,
                    MAX(description) as description,
                    cast(sum(exp_qty) as integer) as exp_qty,
                    MAX(earliest_ros) as earliest_ros
                from
                    {{ ref('fact_esr_data_obt') }}
                group BY
                    project_id,
                    tmr_number,
                    po_number,
                    supplier,
                    project_po
            )
    )
)