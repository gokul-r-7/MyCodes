{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_ecosys_costtypepcsmapping/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
		tags=["ecosys"]
        ) 
}}

select
    cost_object_category_value_id,
    cost_object_category_value_name,
    parent_category_id,
    cost_object_category_value_internal_id,
    legacy_wbs,
    inactive,
    create_user,
    create_date,
    last_update_user,
    last_update_date,
    pcs_revenue,
    mapct_and_revenue_category,
    pcs_cost,
    pcs_hours,
    pcs_nb_cost,
    pcs_nb_hours,
    construction_pcs_revenue,
    construction_pcs_cost,
    construction_pcs_hours,
    epc_mapping,
    change_major_group,
    change_minor_group,
    ct_labour,
    direct_labour,
    execution_date,
    source_system_name,
    primary_key,
    is_current,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from
    {{ source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') }}
where
    is_current = 1

-- WITH hierarchy AS (
--     SELECT display_name, path, cost_object_category_value_internal_id, cost_object_category_value_id, parent_category_id, legacy_wbs, level
--  FROM {{ ref('transformed_costtypepcsmapping_hierarchy') }}
-- ),
-- temp AS (
--     SELECT
--         cost_object_category_value_internal_id,
--         display_name,
--         CASE WHEN SPLIT_PART(path, '|', 1) = '' THEN NULL ELSE SPLIT_PART(path, '|', 1) END AS LIBRARY,
--         CASE WHEN SPLIT_PART(path, '|', 2) = '' THEN NULL ELSE SPLIT_PART(path, '|', 2) END AS LEVEL_1_DESCRIPTION,
--         COALESCE(SPLIT_PART(path, '|', 3), LEVEL_1_DESCRIPTION || ' (No detail)') AS LEVEL_2_DESCRIPTION,
--         COALESCE(SPLIT_PART(path, '|', 4), COALESCE(LEVEL_2_DESCRIPTION, LEVEL_1_DESCRIPTION) || ' (No detail)') AS LEVEL_3_DESCRIPTION,
--         COALESCE(SPLIT_PART(path, '|', 5), COALESCE(LEVEL_3_DESCRIPTION, LEVEL_2_DESCRIPTION, LEVEL_1_DESCRIPTION) || ' (No detail)') AS LEVEL_4_DESCRIPTION,
--         COALESCE(SPLIT_PART(path, '|', 6), COALESCE(LEVEL_4_DESCRIPTION, LEVEL_3_DESCRIPTION, LEVEL_2_DESCRIPTION, LEVEL_1_DESCRIPTION) || ' (No detail)') AS LEVEL_5_DESCRIPTION
--     FROM hierarchy
-- )
-- SELECT
--     ctm.cost_object_category_value_id,
--     ctm.cost_object_category_value_name,
--     ctm.parent_category_id,
--     ctm.cost_object_category_value_internal_id,
--     CASE
--         WHEN ctm.LEGACY_WBS = TRUE THEN 'LEGACY_' || ctm.COST_OBJECT_CATEGORY_VALUE_ID
--         ELSE 'GLOBAL_' || ctm.COST_OBJECT_CATEGORY_VALUE_ID
--     END AS cost_type_pcs_map_id,
--     CASE
--         WHEN ctm.PARENT_CATEGORY_ID <> '' THEN
--             CASE
--                 WHEN ctm.LEGACY_WBS = TRUE THEN 'LEGACY_' || ctm.PARENT_CATEGORY_ID
--                 ELSE 'GLOBAL_' || ctm.PARENT_CATEGORY_ID
--             END
--     END AS parent_cost_type_pcs_map_id,
--     ctm.legacy_wbs,
--     ctm.inactive,
--     ctm.pcs_revenue,
--     ctm.mapct_and_revenue_category,
--     ctm.pcs_cost,
--     ctm.pcs_hours,
--     ctm.pcs_nb_cost,
--     ctm.pcs_nb_hours,
--     ctm.construction_pcs_revenue,
--     ctm.construction_pcs_cost,
--     ctm.construction_pcs_hours,
--     ctm.epc_mapping,
--     ctm.change_major_group,
--     ctm.change_minor_group,
--     ctm.ct_labour,
--     ctm.direct_labour,
--     t.display_name,
--     t.library,
--     t.level_1_description,
--     t.level_2_description,
--     t.level_3_description,
--     t.level_4_description,
--     t.level_5_description,
--     {{ run_date }} AS created_date,
--     {{ run_date }} AS updated_date,
--     {{ generate_load_id(model) }} AS load_id
-- FROM {{ source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') }} ctm
-- JOIN temp t
-- ON ctm.COST_OBJECT_CATEGORY_VALUE_INTERNAL_ID = t.COST_OBJECT_CATEGORY_VALUE_INTERNAL_ID
-- WHERE ctm.is_current = 1