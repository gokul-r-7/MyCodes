{% macro expand_hierarchy() %}
{% set max_iterations = 6 %}
{% set base_table = ref('transformed_costtypepcsmapping_base') %}
{% set mapping_table = source('curated_ecosys', 'curated_ecosys_costtypepcsmapping') %}
{% set hierarchy_query %}

-- Level 0
SELECT
    cost_object_category_value_internal_id,
    cost_object_category_value_id,
    parent_category_id,
    LEGACY_WBS,
    path,
    display_name,
    level
FROM {{ base_table }}

{% endset %}

{% for i in range(1, max_iterations) %}
    {% set hierarchy_query %}
        {{ hierarchy_query }}
        UNION ALL
        SELECT
            pm.cost_object_category_value_internal_id,
            pm.cost_object_category_value_id,
            pm.parent_category_id,
            pm.LEGACY_WBS,
            h.path || replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') || '|' AS path,
            replace(pm.cost_object_category_value_id || ' - ' || pm.cost_object_category_value_name, ' *', '') AS display_name,
            h.level + 1 AS level
        FROM {{ mapping_table }} pm
        JOIN (
            {{ hierarchy_query }}
        ) h
        ON pm.parent_category_id = h.cost_object_category_value_id
        AND pm.LEGACY_WBS = h.LEGACY_WBS
        AND pm.is_current = 1
        WHERE h.level = {{ i - 1 }}
    {% endset %}
{% endfor %}

{{ hierarchy_query }}

{% endmacro %}