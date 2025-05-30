{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_exchange_rates/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select
    to_date(exchange_rates_startminorperiod,'dd-MMM-yy') as startminorperiod,
    exchange_rates_exchangeratetableidname as exchangeratetableidname,
    cast(exchange_rates_aed as float) as aed , 
    cast(exchange_rates_aoa as float) as aoa , 
    cast(exchange_rates_ars as float) as ars , 
    cast(exchange_rates_aud as float) as aud , 
    cast(exchange_rates_azn as float) as azn , 
    cast(exchange_rates_bgn as float) as bgn , 
    cast(exchange_rates_bhd as float) as bhd , 
    cast(exchange_rates_bnd as float) as bnd , 
    cast(exchange_rates_brl as float) as brl , 
    cast(exchange_rates_cad as float) as cad , 
    cast(exchange_rates_chf as float) as chf , 
    cast(exchange_rates_clp as float) as clp , 
    cast(exchange_rates_cny as float) as cny , 
    cast(exchange_rates_cop as float) as cop , 
    cast(exchange_rates_czk as float) as czk , 
    cast(exchange_rates_dkk as float) as dkk , 
    cast(exchange_rates_egp as float) as egp , 
    cast(exchange_rates_eur as float) as eur , 
    cast(exchange_rates_gbp as float) as gbp , 
    cast(exchange_rates_ghs as float) as ghs , 
    cast(exchange_rates_hkd as float) as hkd , 
    cast(exchange_rates_idr as float) as idr , 
    cast(exchange_rates_inr as float) as inr , 
    cast(exchange_rates_iqd as float) as iqd , 
    cast(exchange_rates_irr as float) as irr , 
    cast(exchange_rates_jmd as float) as jmd , 
    cast(exchange_rates_jod as float) as jod , 
    cast(exchange_rates_jpy as float) as jpy , 
    cast(exchange_rates_kes as float) as kes , 
    cast(exchange_rates_kgs as float) as kgs , 
    cast(exchange_rates_krw as float) as krw , 
    cast(exchange_rates_kwd as float) as kwd , 
    cast(exchange_rates_kzt as float) as kzt , 
    cast(exchange_rates_lyd as float) as lyd , 
    cast(exchange_rates_mad as float) as mad , 
    cast(exchange_rates_mnt as float) as mnt , 
    cast(exchange_rates_mxn as float) as mxn , 
    cast(exchange_rates_myr as float) as myr , 
    cast(exchange_rates_mzn as float) as mzn , 
    cast(exchange_rates_nad as float) as nad , 
    cast(exchange_rates_ngn as float) as ngn , 
    cast(exchange_rates_nok as float) as nok , 
    cast(exchange_rates_nzd as float) as nzd , 
    cast(exchange_rates_omr as float) as omr , 
    cast(exchange_rates_pen as float) as pen , 
    cast(exchange_rates_pgk as float) as pgk , 
    cast(exchange_rates_php as float) as php , 
    cast(exchange_rates_pln as float) as pln , 
    cast(exchange_rates_qar as float) as qar , 
    cast(exchange_rates_ron as float) as ron , 
    cast(exchange_rates_rub as float) as rub , 
    cast(exchange_rates_sar as float) as sar , 
    cast(exchange_rates_sek as float) as sek , 
    cast(exchange_rates_sgd as float) as sgd , 
    cast(exchange_rates_skk as float) as skk , 
    cast(exchange_rates_syp as float) as syp , 
    cast(exchange_rates_thb as float) as thb , 
    cast(exchange_rates_tnd as float) as tnd , 
    cast(exchange_rates_try as float) as try , 
    cast(exchange_rates_ttd as float) as ttd , 
    cast(exchange_rates_tzs as float) as tzs , 
    cast(exchange_rates_uah as float) as uah , 
    cast(exchange_rates_usd as float) as usd , 
    cast(exchange_rates_uyu as float) as uyu , 
    cast(exchange_rates_uzs as float) as uzs , 
    cast(exchange_rates_vef as float) as vef , 
    cast(exchange_rates_vnd as float) as vnd , 
    cast(exchange_rates_xaf as float) as xaf , 
    cast(exchange_rates_xpf as float) as xpf , 
    cast(exchange_rates_zar as float) as zar , 
    cast(exchange_rates_zmk as float) as zmk ,
    execution_date ,
    source_system_name,
    {{run_date}} as created_date,
	{{run_date}} as updated_date,
	{{ generate_load_id(model) }} as load_id
FROM
    {{ source('curated_ecosys', 'curated_ecosys_exchange_rates') }}
where
    exchange_rates_startminorperiod <> '2010-01'