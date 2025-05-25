{{

     config( 
        materialized = "table",
        tags=["construction"]
        )

}}



with recursive cte(val_date) as
    (
       select
          min(cast(plannedstartdate as date)) -14 as val_date
          from  {{source('domain_integrated_model','transformed_cwps')}} 
        where projectid = 41
 
    union all
    select
        cast(dateadd(day, 1, val_date) as date) as val_date
    from
        cte
    where
        val_date < (select  max(cast(plannedstartdate as date))
          from  {{source('domain_integrated_model','transformed_cwps')}}  where projectid = 41 )  + 190
    )
select
    val_date as Date, DATE_PART_YEAR(val_date)  as Year,
    Date_part(month, date (val_date) ) as MonthNo,
    Substring(to_char(val_date::Date, 'Month'),1,3) as Month,
    cast(DATE_PART_YEAR(val_date) as varchar(10)) as Year_Varchar,
    Substring(to_char(val_date::Date, 'Month'),1,3) + ' '  + cast( DATE_PART_YEAR(val_date) as varchar(10)) as Month_Year,
    
    CASE when EXTRACT(DOW FROM date (val_date)) = 0 then  val_date
    else val_date + 7 - 1 * cast(DATE_PART(dayofweek, date (val_date)) as INT) end as Weekenddate,
cast(DATE_PART(Days, date (Weekenddate)) as int) as Week_Days
from cte  
order by
    val_date
