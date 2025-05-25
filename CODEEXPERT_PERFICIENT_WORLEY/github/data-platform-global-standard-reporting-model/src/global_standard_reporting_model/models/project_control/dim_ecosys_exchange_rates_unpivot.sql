{{
     config(
         materialized = "table",
         tags=["project_control"]
         )

}}

with base_rates as
(select 
    case extract(dow from last_day(startminorperiod)) +1
        when extract(dow from last_day(startminorperiod)) + 1 = 1 then dateadd(day, -extract(dow from last_day(startminorperiod))-2 ,last_day(startminorperiod))
        when extract(dow from last_day(startminorperiod)) + 1 = 2 then dateadd(day, -extract(dow from last_day(startminorperiod))-2 ,last_day(startminorperiod))
        when extract(dow from last_day(startminorperiod)) + 1 = 3 then dateadd(day, -extract(dow from last_day(startminorperiod))-2 ,last_day(startminorperiod))
        when extract(dow from last_day(startminorperiod)) + 1 = 4 then dateadd(day, -extract(dow from last_day(startminorperiod))-2 ,last_day(startminorperiod))
        when extract(dow from last_day(startminorperiod)) + 1 = 5 then dateadd(day, -extract(dow from last_day(startminorperiod))-2 ,last_day(startminorperiod))
        when extract(dow from last_day(startminorperiod)) + 1 = 6 then dateadd(day, 0 ,last_day(startminorperiod))
        else dateadd(day, -1 ,last_day(startminorperiod))
    end as snapshot_date,currency,  value
from {{ ref('dim_ecosys_exchange_rates') }}
unpivot (
    value for currency in (
           aed,ars,aud,bhd,bnd,brl,
           cad,clp,cny,cop,eur,gbp,
           idr,inr,kwd,kzt,mxn,myr,
           ngn,nzd,omr,qar,sar,sek,
           sgd,thb,ttd,usd,uzs,zar
            )
    ) as unpvt
),
cross_rates as (
    select  
        cast(a.snapshot_date as date),
        b.currency as cur,
        a.value / b.value as rate,
        a.currency as to_currency
    from base_rates a   
    join base_rates b
        on a.snapshot_date = b.snapshot_date
    where a.snapshot_date > 2024
)

select
    snapshot_date,
    cur,
    max(case when to_currency = 'aed' then rate end) as aed,
    max(case when to_currency = 'ars' then rate end) as ars,
    max(case when to_currency = 'aud' then rate end) as aud,
    max(case when to_currency = 'bhd' then rate end) as bhd,
    max(case when to_currency = 'bnd' then rate end) as bnd,
    max(case when to_currency = 'brl' then rate end) as brl,
    max(case when to_currency = 'cad' then rate end) as cad,
    max(case when to_currency = 'clp' then rate end) as clp,
    max(case when to_currency = 'cny' then rate end) as cny,
    max(case when to_currency = 'cop' then rate end) as cop,
    max(case when to_currency = 'eur' then rate end) as eur,
    max(case when to_currency = 'gbp' then rate end) as gbp,
    max(case when to_currency = 'idr' then rate end) as idr,
    max(case when to_currency = 'inr' then rate end) as inr,
    max(case when to_currency = 'kwd' then rate end) as kwd,
    max(case when to_currency = 'kzt' then rate end) as kzt,
    max(case when to_currency = 'mxn' then rate end) as mxn,
    max(case when to_currency = 'myr' then rate end) as myr,
    max(case when to_currency = 'ngn' then rate end) as ngn,
    max(case when to_currency = 'nzd' then rate end) as nzd,
    max(case when to_currency = 'omr' then rate end) as omr,
    max(case when to_currency = 'qar' then rate end) as qar,
    max(case when to_currency = 'sar' then rate end) as sar,
    max(case when to_currency = 'sek' then rate end) as sek,
    max(case when to_currency = 'sgd' then rate end) as sgd,
    max(case when to_currency = 'thb' then rate end) as thb,
    max(case when to_currency = 'ttd' then rate end) as ttd,
    max(case when to_currency = 'usd' then rate end) as usd,
    max(case when to_currency = 'uzs' then rate end) as uzs,
    max(case when to_currency = 'zar' then rate end) as zar
from cross_rates
group by snapshot_date,cur 