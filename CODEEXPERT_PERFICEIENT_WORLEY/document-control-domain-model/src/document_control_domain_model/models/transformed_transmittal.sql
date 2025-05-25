{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_transmittal/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
    )
}}

WITH transmittal AS
(
    SELECT  
        source_system_name
        ,mailid
        ,mailno
        ,subject
        ,correspondencetype AS transmittal_type
        ,reasonforissue
        ,referencenumber
        ,CAST(sentdate AS TIMESTAMP) AS sentdate
        ,CAST(responserequired_responserequireddate AS TIMESTAMP) AS responserequired_responserequireddate
        ,tousers_recipient_name
        ,tousers_recipient_userid
        ,tousers_recipient_distributiontype
        ,tousers_recipient_organizationname
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_mailsentbox') }}
    WHERE correspondencetype in ('Transmittal')
    
    UNION ALL

        SELECT  
        source_system_name
        ,mailid
        ,mailno
        ,subject
        ,correspondencetype AS transmittal_type
        ,reasonforissue
        ,referencenumber
        ,CAST(sentdate AS TIMESTAMP) AS sentdate
        ,CAST(responserequired_responserequireddate AS TIMESTAMP) AS responserequired_responserequireddate
        ,tousers_recipient_name
        ,tousers_recipient_userid
        ,tousers_recipient_distributiontype
        ,tousers_recipient_organizationname
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_mailinbox') }}
    WHERE correspondencetype in ('Supplier Document Transmittal', 'Workflow Transmittal')
),

 hexagon AS 
(
    SELECT
        source_system_name
        ,transmittal_number
        ,transmittal_reason_for_issue
        ,contract
        ,CASE 
            WHEN CAST(transmittal_creation_date as STRING) = 'NaT' THEN CAST(NULL AS TIMESTAMP) 
            ELSE CAST(transmittal_creation_date AS TIMESTAMP) 
        END AS transmittal_creation_date
        ,CASE 
            WHEN CAST(transmittal_suggested_due_date as STRING)  = 'NaT' THEN CAST(NULL AS TIMESTAMP) 
            ELSE CAST(transmittal_suggested_due_date AS TIMESTAMP) 
        END AS transmittal_suggested_due_date
        ,recipient
        ,recipient_e_mail_address
        ,workflow_step
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
    FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }}
)

SELECT

    concat(source_system_name, '|' , mailid) AS transmittal_sid   -- Waiting for new mail table which will have Document ID
    ,mailno AS transmittal_number
    ,transmittal_type
    ,subject AS title --  Need to check with Adam is this the correct field??
    ,reasonforissue AS reason_for_issue
    ,referencenumber AS reference_number
    ,TO_TIMESTAMP(sentdate, 'yyyy-MM-dd HH:mm:ss.SSXXX') AS sent_date
    ,CAST(next_day(Dateadd(day,-1,sentdate),'friday') AS TIMESTAMP) as sent_date_weekend
    ,CASE WHEN responserequired_responserequireddate IS NULL THEN false ELSE true END AS response_required --confirm with Adam is it the correct field?
    ,responserequired_responserequireddate AS response_due_date
    ,CAST(next_day(Dateadd(day,-1,responserequired_responserequireddate),'friday') AS TIMESTAMP) as response_due_date_weekend
    ,tousers_recipient_name AS recipient -- temp field to be replaced once the people domain is completed.
    ,tousers_recipient_organizationname AS recipient_organization
    ,tousers_recipient_distributiontype AS distribution_type
    
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    transmittal T

UNION ALL

SELECT DISTINCT

    concat(source_system_name, '|', transmittal_number) AS transmittal_sid
    ,transmittal_number
    ,'Hexagon Transmittal' AS transmittal_type
    ,null AS title
    ,transmittal_reason_for_issue AS reason_for_issue
    ,contract AS reference_number
    ,transmittal_creation_date AS sent_date
    ,cast(next_day(Dateadd(day,-1,transmittal_creation_date),'friday') AS TIMESTAMP) as sent_date_weekend
    ,CASE WHEN transmittal_suggested_due_date IS NULL THEN false ELSE true END AS response_Required
    ,transmittal_suggested_due_date AS response_due_date
    ,Cast(next_day(Dateadd(day,-1,transmittal_suggested_due_date),'friday') AS TIMESTAMP) as response_due_date_weekend
    ,recipient
    ,split_part(split_part(recipient_e_mail_address,'@',2),'.',1) AS recipient_organization
    ,workflow_step AS distribution_type

-- Datahub standard Meta data fields
    ,'1207979025' AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,execution_date AS dim_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    hexagon
