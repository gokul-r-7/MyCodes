{{
     config(
         materialized = "table",
         tags=["project_control"]
         )
}}


with recursive cte(display_name,path,cost_object_category_value_internal_id,cost_object_category_value_id,level,legacy_wbs) as
(
   select 
      replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') display_name,
      replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') || '|' as path,
      cost_object_category_value_internal_id,
      cost_object_category_value_id,
      1 as level,
      legacy_wbs
   from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_costtypepcsmapping') }}
   where (parent_category_id = '' or parent_category_id is null) and is_current = 1

   union all

   select 
      replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') display_name,
      c.path || replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') || '|' as path,
      pm.cost_object_category_value_internal_id,
      pm.cost_object_category_value_id,
      c.level + 1 as level,
      pm.legacy_wbs
   from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_costtypepcsmapping') }} pm , cte c
   where pm.parent_category_id = c.cost_object_category_value_id and pm.legacy_wbs = c.legacy_wbs and pm.is_current=1
),
temp as 
(
    select 
    cost_object_category_value_internal_id,
    display_name,
    library,
    level_1_description,
    coalesce(level_2_description,level_1_description || ' (No detail)') as level_2_description,
    coalesce(level_3_description,coalesce(level_2_description,level_1_description) || ' (No detail)') as level_3_description,
    coalesce(level_4_description,coalesce(level_3_description,level_2_description,level_1_description) || ' (No detail)') as level_4_description,
    coalesce(level_5_description,coalesce(level_4_description,level_3_description,level_2_description,level_1_description) || ' (No detail)') as level_5_description
    from
    (
    select
        cost_object_category_value_internal_id,
        display_name,
        case when split_part(path,'|',1) = '' then null else split_part(path,'|',1) end as library,
        case when split_part(path,'|',2) = '' then null else split_part(path,'|',2) end as level_1_description,
        case when split_part(path,'|',3) = '' then null else split_part(path,'|',3) end as level_2_description,
        case when split_part(path,'|',4) = '' then null else split_part(path,'|',4) end as level_3_description,
        case when split_part(path,'|',5) = '' then null else split_part(path,'|',5) end as level_4_description,
        case when split_part(path,'|',6) = '' then null else split_part(path,'|',6) end as level_5_description
    from cte
    )t
)

select
   cast(ctm.cost_object_category_value_id as varchar(100)) as cost_object_category_value_id,
	cast(ctm.cost_object_category_value_name as varchar(100)) as cost_object_category_value_name,
	cast(ctm.parent_category_id as varchar(100)) as parent_category_id,
	cast(ctm.cost_object_category_value_internal_id as varchar(100)) as cost_object_category_value_internal_id,
	cast(case when ctm.legacy_wbs = true 
		then 'LEGACY_' || ctm.cost_object_category_value_id
		else 'GLOBAL_' || ctm.cost_object_category_value_id
	end as varchar(100)) as cost_type_pcs_map_id,
	cast(case when ctm.parent_category_id <> ''
      then
      case when ctm.legacy_wbs = true
         then 'LEGACY_' || ctm.parent_category_id
         else 'GLOBAL_' || ctm.parent_category_id
      end
	end as varchar(100)) as parent_cost_type_pcs_map_id,
   ctm.legacy_wbs,
   ctm.inactive,
   cast(ctm.create_user as varchar(100)) as create_user,
   ctm.create_date,
   cast(ctm.last_update_user as varchar(100)) as last_update_user,
   ctm.last_update_date,
   cast(ctm.pcs_revenue as varchar(100)) as pcs_revenue,
   cast(ctm.mapct_and_revenue_category as varchar(100)) as mapct_and_revenue_category,
   cast(ctm.pcs_cost as varchar(100)) as pcs_cost,
   cast(ctm.pcs_hours as varchar(100)) as pcs_hours,
   cast(ctm.pcs_nb_cost as varchar(100)) as pcs_nb_cost,
   cast(ctm.pcs_nb_hours as varchar(100)) as pcs_nb_hours,
   cast(ctm.construction_pcs_revenue as varchar(100)) as construction_pcs_revenue,
   cast(ctm.construction_pcs_cost as varchar(100)) as construction_pcs_cost,
   cast(ctm.construction_pcs_hours as varchar(100)) as construction_pcs_hours,
   cast(ctm.epc_mapping as varchar(100)) as epc_mapping,
   cast(ctm.change_major_group as varchar(100)) as change_major_group,
   cast(ctm.change_minor_group as varchar(100)) as change_minor_group,
   ctm.ct_labour,
   ctm.direct_labour,
   cast(t.display_name as varchar(100)) as display_name,
   cast(t.library as varchar(100)) as library,
   cast(t.level_1_description as varchar(100)) as level_1_description,
   cast(t.level_2_description as varchar(100)) as level_2_description,
   cast(t.level_3_description as varchar(100)) as level_3_description,
   cast(t.level_4_description as varchar(100)) as level_4_description,
   cast(t.level_5_description as varchar(100)) as level_5_description
from 
   {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_costtypepcsmapping') }} ctm , temp t
where 
   ctm.cost_object_category_value_internal_id = t.cost_object_category_value_internal_id 
   and ctm.is_current=1