{{ config(materialized='table') }}

select 
    distinct
        cast(disciplineid as int),
        cast(discipline as varchar(10)),
        cast(disciplinedescription as varchar)
from
          {{ source('curated_vg_o3', 'curated_cwps') }}

--ALTER TABLE dim_discipline ADD CONSTRAINT discipline_pk PRIMARY KEY (disciplineid);
