{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_transmittal_to_document/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

WITH transmittal AS
    (
        Select  
            concat(source_system_name ,'|', mailid) AS transmittal_sid
            ,concat(source_system_name ,'|', COALESCE(Documentid, registeredas)) AS version_sid
            ,CAST(projectid AS VARCHAR(100)) AS projectid
            ,CAST(execution_date AS TIMESTAMP) AS execution_date
            ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
        FROM    
            {{ source('curated_aconex', 'curated_mail_document') }}
        Where projectid not in ('1207979025')
    ),

Document AS (
        Select
            document_sid,
            revision_sid,
            version_sid
        FROM {{ref('transformed_revision')}}
),

Aconex AS
    (
        SELECT DISTINCT
            source_system_name
            ,trackingid
            ,Documentid
            ,MAX(documentid) OVER (Partition By revision) AS max_documentid
            ,documentnumber
            ,revision
        FROM 
        {{ source('curated_aconex', 'curated_docregister_standard') }} 
        WHERE
            projectid = '1207979025'
            AND eff_end_date = '9999-12-31'
    ),

hexagon AS 
    (
        SELECT DISTINCT
            CASE 
                WHEN A.trackingid IS NULL THEN concat(h.source_system_name , '|' , document_number)
                ELSE concat(A.source_system_name , '|' , A.trackingid)
            END AS document_sid
            ,CASE 
                WHEN A.max_documentid IS NULL THEN concat(h.source_system_name , '|' , document_number)
                ELSE concat(A.source_system_name  , '|' , A.max_documentid)
            END AS version_sid
            ,H.source_system_name
            ,H.transmittal_number
            ,H.document_number
            ,H.document_revision
            ,CAST('1207979025' AS VARCHAR(100)) AS projectid
            ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
        FROM
            {{ source('curated_hexagon', 'curated_hexagon_ofe') }}  H
        LEFT JOIN Aconex A ON 
                H.document_number = A.documentnumber
                AND H.document_revision = A.Revision
        WHERE document_number != 'nan'
    )

SELECT DISTINCT
    concat(h.source_system_name , '|' , transmittal_number) AS transmittal_sid
    ,CAST(NULL AS VARCHAR(100)) AS document_sid
    ,CAST(NULL AS VARCHAR(100)) AS revision_sid
    ,CASE 
        WHEN H.version_sid = 'nan' THEN concat(h.source_system_name , '|' , document_number , '|' , document_revision) 
        ELSE H.version_sid 
    END AS version_sid

-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,execution_date AS dim_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    hexagon H
LEFT JOIN Aconex A ON H.document_number = A.documentnumber AND H.document_revision = A.revision

UNION

SELECT
    T.transmittal_sid
    ,D.document_sid
    ,D.revision_sid
    ,D.version_sid

-- Datahub standard Meta data fields
    ,T.projectid AS dim_project_rls_key
    ,T.execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN T.eff_end_date = '9999-12-31' THEN current_date()
        ELSE T.eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM transmittal T
LEFT JOIN Document D ON T.version_sid = D.version_sid
