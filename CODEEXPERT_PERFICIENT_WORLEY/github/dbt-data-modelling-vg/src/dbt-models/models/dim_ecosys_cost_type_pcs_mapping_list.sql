{{ config(materialized='table') }}
 
 WITH recursive cte(display_name,path,cost_object_category_value_internal_id,cost_object_category_value_id,level,LEGACY_WBS) AS
(
   SELECT 
      replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') display_name,
      replace(cost_object_category_value_id || ' - ' || cost_object_category_value_name, ' *', '') || '|' as path,
      cost_object_category_value_internal_id,
      cost_object_category_value_id,
      1 as level,
      LEGACY_WBS
   FROM {{ source('curated_vg_ecosys', 'curated_ecosys_costtypepcsmapping') }}
   WHERE parent_category_id = ''

   UNION ALL

   SELECT 
      replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') display_name,
      c.path || replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') || '|' as path,
      pm.cost_object_category_value_internal_id,
      pm.cost_object_category_value_id,
      c.level + 1 as level,
      pm.LEGACY_WBS
   FROM {{ source('curated_vg_ecosys', 'curated_ecosys_costtypepcsmapping') }} pm , cte c
   WHERE pm.parent_category_id = c.cost_object_category_value_id and pm.LEGACY_WBS = c.LEGACY_WBS

),
temp as 
(
SELECT 
   cost_object_category_value_internal_id,
   display_name,
   LIBRARY,
   LEVEL_1_DESCRIPTION,
   COALESCE(LEVEL_2_DESCRIPTION,LEVEL_1_DESCRIPTION || ' (No detail)') as LEVEL_2_DESCRIPTION,
   COALESCE(LEVEL_3_DESCRIPTION,COALESCE(LEVEL_2_DESCRIPTION,LEVEL_1_DESCRIPTION) || ' (No detail)') as LEVEL_3_DESCRIPTION,
   COALESCE(LEVEL_4_DESCRIPTION,COALESCE(LEVEL_3_DESCRIPTION,LEVEL_2_DESCRIPTION,LEVEL_1_DESCRIPTION) || ' (No detail)') as LEVEL_4_DESCRIPTION,
   COALESCE(LEVEL_5_DESCRIPTION,COALESCE(LEVEL_4_DESCRIPTION,LEVEL_3_DESCRIPTION,LEVEL_2_DESCRIPTION,LEVEL_1_DESCRIPTION) || ' (No detail)') as LEVEL_5_DESCRIPTION
FROM
(
   SELECT
      cost_object_category_value_internal_id,
      display_name,
      CASE WHEN SPLIT_PART(path,'|',1) = '' THEN NULL ELSE SPLIT_PART(path,'|',1) END as LIBRARY,
      CASE WHEN SPLIT_PART(path,'|',2) = '' THEN NULL ELSE SPLIT_PART(path,'|',2) END as LEVEL_1_DESCRIPTION,
      CASE WHEN SPLIT_PART(path,'|',3) = '' THEN NULL ELSE SPLIT_PART(path,'|',3) END as LEVEL_2_DESCRIPTION,
      CASE WHEN SPLIT_PART(path,'|',4) = '' THEN NULL ELSE SPLIT_PART(path,'|',4) END as LEVEL_3_DESCRIPTION,
      CASE WHEN SPLIT_PART(path,'|',5) = '' THEN NULL ELSE SPLIT_PART(path,'|',5) END as LEVEL_4_DESCRIPTION,
      CASE WHEN SPLIT_PART(path,'|',6) = '' THEN NULL ELSE SPLIT_PART(path,'|',6) END as LEVEL_5_DESCRIPTION
   FROM cte
)t
)

SELECT
   ctm.COST_OBJECT_CATEGORY_VALUE_ID,
	ctm.COST_OBJECT_CATEGORY_VALUE_NAME,
	ctm.PARENT_CATEGORY_ID,
	ctm.COST_OBJECT_CATEGORY_VALUE_INTERNAL_ID,
	case when ctm.LEGACY_WBS = true 
		then 'LEGACY_' || ctm.COST_OBJECT_CATEGORY_VALUE_ID
		else 'GLOBAL_' || ctm.COST_OBJECT_CATEGORY_VALUE_ID
	end as COST_TYPE_PCS_MAP_ID,
	case when ctm.PARENT_CATEGORY_ID <> ''
      then
      case when ctm.LEGACY_WBS = true
         then 'LEGACY_' || ctm.PARENT_CATEGORY_ID
         else 'GLOBAL_' || ctm.PARENT_CATEGORY_ID
      end
	end as PARENT_COST_TYPE_PCS_MAP_ID,
	ctm.LEGACY_WBS,
	ctm.INACTIVE,
	ctm.CREATE_USER,
	ctm.CREATE_DATE,
	ctm.LAST_UPDATE_USER,
	ctm.LAST_UPDATE_DATE,
	ctm.PCS_REVENUE,
	ctm.MAPCT_AND_REVENUE_CATEGORY,
	ctm.PCS_COST,
	ctm.PCS_HOURS,
	ctm.PCS_NB_COST,
	ctm.PCS_NB_HOURS,
	ctm.CONSTRUCTION_PCS_REVENUE,
	ctm.CONSTRUCTION_PCS_COST,
	ctm.CONSTRUCTION_PCS_HOURS,
	ctm.EPC_MAPPING,
	ctm.CHANGE_MAJOR_GROUP,
	ctm.CHANGE_MINOR_GROUP,
	ctm.CT_LABOUR,
	ctm.DIRECT_LABOUR,
   t.DISPLAY_NAME,
   t.LIBRARY,
   t.LEVEL_1_DESCRIPTION,
   t.LEVEL_2_DESCRIPTION,
   t.LEVEL_3_DESCRIPTION,
   t.LEVEL_4_DESCRIPTION,
   t.LEVEL_5_DESCRIPTION
FROM {{ source('curated_vg_ecosys', 'curated_ecosys_costtypepcsmapping') }} ctm , temp t
where ctm.COST_OBJECT_CATEGORY_VALUE_INTERNAL_ID = t.COST_OBJECT_CATEGORY_VALUE_INTERNAL_ID