{{ config(materialized='table') }}

select 
    distinct
        cast(constrainttypeid as int),
        cast(constrainttype as varchar)
from
          {{ source('curated_vg_o3', 'curated_constraints') }}

--ALTER TABLE  {{ source('curated_vg_o3', 'curated_constraints') }} ADD CONSTRAINT constraint_type_pk PRIMARY KEY (constrainttypeid);
