{% macro log_audit_table(results) %}
    -- depends_on: {{ ref('transformed_audit_table') }}
    {%- if execute -%}
        {{ print("Running log_audit_table Macro") }}
        {%- set run_date = "CURRENT_TIMESTAMP" -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        
        {%- if parsed_results | length  > 0 -%}
            {% set insert_dbt_results_query -%}
                insert into {{ ref('transformed_audit_table') }}
                    (
                        load_id,
                        invocation_id,
                        database_name,
                        schema_name,
                        name,
                        resource_type,
                        status,
                        execution_time,
                        rows_affected,
                        model_execution_date
                ) values
                    {%- for parsed_result_dict in parsed_results -%}
                        (
                            '{{ parsed_result_dict.get('load_id') }}',
                            '{{ parsed_result_dict.get('invocation_id') }}',
                            '{{ parsed_result_dict.get('database_name') }}',
                            '{{ parsed_result_dict.get('schema_name') }}',
                            '{{ parsed_result_dict.get('name') }}',
                            '{{ parsed_result_dict.get('resource_type') }}',
                            '{{ parsed_result_dict.get('status') }}',
                            {{ parsed_result_dict.get('execution_time') }},
                            {{ parsed_result_dict.get('rows_affected') }},
                            {{ run_date }}
                        ) {{- "," if not loop.last else "" -}}
                    {%- endfor -%}
            {%- endset -%}
            
                {%- if insert_dbt_results_query | trim != "" -%}
                    {%- do  adapter.execute(insert_dbt_results_query) -%}
                    {{ print("Successfully inserted records into transformed_audit_table") }}
                {%- endif -%}
        {%- endif -%}
    {%- endif -%}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return (' ') }}
{% endmacro %}
~