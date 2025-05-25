{{ config(materialized='table') }}

select 
    distinct 
        cast(constructionworkareaid as int),
        cast(constructionworkarea as varchar(10)),
        cast(cwadescription as varchar)
from 
        {{ source('curated_vg_o3', 'curated_cwps') }}

--ALTER TABLE dim_construction_work_area ADD CONSTRAINT construction_work_area_pk PRIMARY KEY (constructionworkareaid);
