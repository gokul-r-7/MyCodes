{{ config(materialized='table') }}

select 
    distinct 
        cast(constructiontypeid as int),
        cast(constructiontype as varchar)
from
        {{ source('curated_vg_o3', 'curated_cwps') }}

--ALTER TABLE dim_construction_type ADD CONSTRAINT construction_type_pk PRIMARY KEY (constructiontypeid);
