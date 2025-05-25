{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )
}}


SELECT
    date_pk as date_key,
    DATE_PART_YEAR(date_pk) as year,
    cast(date_part(month, date_pk) as integer) as month,
    Substring(to_char(date_pk:: Date, 'Month'), 1, 3) as month_name_mmm,
    TO_CHAR(date_pk, 'Month') AS month_name,
    TO_CHAR(date_pk, 'Mon YYYY') AS monthyear,
    CASE
    WHEN DATEPART(WEEKDAY, date_pk) = 5 THEN date_pk:: DATE
    ELSE (
        DATEADD(DAY, - (DATEPART(WEEKDAY, date_pk) + 1) % 7, date_pk) - 1):: DATE END AS weekend_date,
    date_key+(6 - EXTRACT(DOW FROM date_key)) as weekend_saturday,
    (date_key+(6 - EXTRACT(DOW FROM date_key)))-6 as weekend_sunday
FROM
    {{ source('supply_chain_dim', 'transformed_erm_dim_date') }}
 
