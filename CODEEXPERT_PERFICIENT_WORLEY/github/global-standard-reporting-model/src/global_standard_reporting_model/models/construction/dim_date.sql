
{{

     config( 
        materialized = "table",
        tags=["construction"]
        )

}}


with recursive cte(val_date) as
    (select 
        cast('2021-01-01' as date) as val_date
    union all
    select
        cast(dateadd(day, 1, val_date) as date) as val_date
    from 
        cte
    where 
        val_date <'2036-12-31'
    )
select 
    val_date as Date_key, DATE_PART_YEAR(val_date)  as Year,
    Date_part(month, date (val_date) ) as Month,
    Substring(to_char(val_date::Date, 'Month'),1,3) as Month_Name,
    cast( DATE_PART_YEAR(val_date) as varchar(10)),
    Substring(to_char(val_date::Date, 'Month'),1,3) + ' '  + cast( DATE_PART_YEAR(val_date) as varchar(10)) as MonthYear
from cte  
order by
    val_date