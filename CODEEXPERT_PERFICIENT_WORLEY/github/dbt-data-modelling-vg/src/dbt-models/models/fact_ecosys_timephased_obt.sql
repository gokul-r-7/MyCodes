{{ config(materialized='table') }}

select
    pcs.cost_object_category_value_id,
    pcs.cost_object_category_value_name,
    pcs.parent_category_id,
    pcs.cost_type_pcs_map_id,
    pcs.parent_cost_type_pcs_map_id,
    pcs.display_name,
    pcs.library,
    pcs.level_1_description,
    pcs.level_2_description,
    pcs.level_3_description,
    pcs.level_4_description,
    pcs.level_5_description,
    et.*,
    split_part(et.ca_id,'-',4) as areacode
from {{ref('fact_ecosys_timephased')}} et
inner join 
(
    select * from {{ref('dim_ecosys_cost_type_pcs_mapping_list')}}
    where inactive = false and legacy_wbs = false and cost_object_category_value_internal_id <> 5127902
) pcs
    on et.cost_type_internal_id = pcs.cost_object_category_value_internal_id
where et.projectnumber in ('418005-00861','418095-50786') 
and pcs.level_2_description = '3000 - Engineering'
and pcs.level_3_description <> '3900 - Engineering Support & Management'