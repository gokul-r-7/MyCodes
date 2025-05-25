{% macro generate_load_id(model) -%}
    '{{ invocation_id }}' || '.' || '{{ model.unique_id }}'
{%- endmacro %}