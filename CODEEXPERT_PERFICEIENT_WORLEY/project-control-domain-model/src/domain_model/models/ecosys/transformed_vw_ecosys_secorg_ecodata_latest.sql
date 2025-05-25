{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_vw_ecosys_secorg_ecodata_latest/',
        options={"write.target-file-size-bytes": "268435456"},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

SELECT
    t.*,
    CASE
    WHEN TO_TIMESTAMP(
        REGEXP_REPLACE(t.execution_date, '  +', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ) = m.max_event_ts THEN 'active'
    ELSE 'inactive' END AS status,
    CAST({{ run_date }} AS DATE) as created_date,  
    CAST({{ run_date }} AS DATE) as updated_date,
    {{ generate_load_id(model) }} as load_id
FROM
    {{ source('curated_ecosys', 'curated_ecosys_ecodata') }} t
    JOIN (
        SELECT
            secorg_id,
            snapshot_id,
            MAX(
                TO_TIMESTAMP(
                    REGEXP_REPLACE(execution_date, '  +', ' '),
                    'yyyy-MM-dd HH:mm:ss'
                )
            ) AS max_event_ts
        FROM
            {{ source('curated_ecosys', 'curated_ecosys_ecodata') }}
        where
            is_current = 1
        GROUP BY
            secorg_id,
            snapshot_id
    ) m ON t.secorg_id = m.secorg_id
    AND t.snapshot_id = m.snapshot_id --AND TO_TIMESTAMP(REGEXP_REPLACE(t.execution_date, '  +', ' '), 'yyyy-MM-dd HH:mm:ss') = m.max_event_ts
where
    t.is_current = 1
