{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_status/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

SELECT
    snapshotlog_costobjectid,
    snapshotlog_snapshotintid,
    'Inactive' as status,
   {{run_date}} as created_date,
   {{run_date}} as updated_date,
   {{ generate_load_id(model) }} as load_id
FROM
    (
        SELECT
            snapshotlog_costobjectid,
            snapshotlog_snapshotintid,
            snapshotlog_DESCRIPTION,
            SPLIT_PART(CAST(snapshotlog_CREATEDATE AS string), 'T', 1) AS CREATE_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY snapshotlog_costobjectid,
                snapshotlog_snapshotintid
                ORDER BY
                    snapshotlog_CREATEDATE DESC
            ) AS RN
        FROM
            {{ source('curated_ecosys', 'curated_ecosys_snapshotlog') }}
        WHERE
            snapshotlog_DESCRIPTION != 'Portfolio Snapshot'
    ) a
WHERE
    RN = 1
    AND LOWER(snapshotlog_DESCRIPTION) LIKE '%purged%'
