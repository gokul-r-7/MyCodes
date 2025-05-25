{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}


WITH transmittal AS
    (
        Select  
            source_system_name
            ,mailid
            ,Documentid
            ,registeredas
            ,CAST(projectid AS VARCHAR) AS projectid
            ,CAST(execution_date AS TIMESTAMP) AS execution_date
            ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
        FROM    
             {{ source('curated_aconex', 'curated_mail_document') }} 
        WHERE   
            projectid in ('1207979025','268456382','268452089') -- Aconex instance ID
            AND eff_end_date = '9999-12-31' -- AWS data journalling current flag
    ),
Aconex AS
    (
        SELECT DISTINCT
            source_system_name
            ,trackingid
            ,Documentid
            ,MAX(documentid) OVER (Partition By revision) AS "max_documentid"
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
                WHEN A.trackingid IS NULL THEN h.source_system_name + '|' + document_number
                ELSE A.source_system_name + '|' + A.trackingid 
            END AS document_sid
            ,CASE 
                WHEN A.max_documentid IS NULL THEN h.source_system_name + '|' + document_number
                ELSE A.source_system_name  + '|' + A.max_documentid
            END AS version_sid
            ,H.source_system_name
            ,H.transmittal_number
            ,H.document_number
            ,H.document_revision
            ,CAST('1207979025' AS VARCHAR) AS projectid
            ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
        FROM
             {{ source('curated_hexagon', 'curated_hexagon_ofe') }}  H
        LEFT JOIN Aconex A ON 
                H.document_number = A.documentnumber
                AND H.document_revision = A.Revision
        WHERE document_number != 'nan'
    )

SELECT DISTINCT
    h.source_system_name + '|' + transmittal_number AS transmittal_sid
    ,CASE 
        WHEN H.version_sid = 'nan' THEN h.source_system_name + '|' + document_number + '|' + document_revision 
        ELSE H.version_sid 
    END AS version_sid

-- Datahub standard Meta data fields
    ,projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,execution_date AS meta_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id
FROM 
    hexagon H
LEFT JOIN 
    Aconex A ON H.document_number = A.documentnumber AND H.document_revision = A.revision

UNION

SELECT
    source_system_name +'|'+ mailid AS transmittal_sid
    ,CASE 
        WHEN Documentid = '' THEN source_system_name +'|'+ registeredas 
        ELSE source_system_name +'|'+ Documentid 
    END AS version_sid

-- Datahub standard Meta data fields
    ,projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN CURRENT_DATE
        ELSE eff_end_date
    END AS meta_snapshot_date
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id
FROM
    transmittal