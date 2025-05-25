
{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}

    /*

SQL Query Title: Venture Global Custom Mapping to Worley Global Data Model
Domain: Document Management
Data Governance Confidentiality Level:
Project Name: Venture Global CP2
   
*/

WITH transmittal AS
(
    SELECT  
        source_system_name
        ,mailid
        ,mailno
        ,subject
        ,reasonforissue
        ,referencenumber
        ,CAST(sentdate AS TIMESTAMP) AS sentdate
        ,CAST(responserequired_responserequireddate AS TIMESTAMP) AS responserequired_responserequireddate
        ,tousers_recipient_name
        ,tousers_recipient_userid
        ,tousers_recipient_distributiontype
        ,tousers_recipient_organizationname
        ,CAST(projectid AS VARCHAR) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_mailsentbox') }}
    --FROM "document_control"."curated_aconex"."curated_mailsentbox"
    WHERE eff_end_date = '9999-12-31'  -- AWS data journalling current flag
        AND correspondencetype = 'Transmittal'
        --AND projectid in ('1207979025','268456382','268452089') -- Aconex instance ID     
)

--  hexagon AS 
-- (
--     SELECT
--         source_system_name
--         ,transmittal_number
--         ,transmittal_reason_for_issue
--         ,contract
--         ,CASE 
--             WHEN CAST(transmittal_creation_date as char) = 'NaT' THEN CAST(NULL AS TIMESTAMP) 
--             ELSE CAST(transmittal_creation_date AS TIMESTAMP) 
--         END AS transmittal_creation_date
--         ,CASE 
--             WHEN CAST(transmittal_suggested_due_date as char)  = 'NaT' THEN CAST(NULL AS TIMESTAMP) 
--             ELSE CAST(transmittal_suggested_due_date AS TIMESTAMP) 
--         END AS transmittal_suggested_due_date
--         ,recipient
--         ,recipient_e_mail_address
--         ,workflow_step
--         ,CAST(execution_date AS TIMESTAMP) AS execution_date
--     FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }}
--     --FROM "document_control"."curated_hexagon"."curated_hexagon_ofe"
-- )

SELECT
    source_system_name +'|'+ mailid AS transmittal_key   -- Waiting for new mail table which will have Document ID
    ,source_system_name +'|'+ mailid AS transmittal_sid   -- Waiting for new mail table which will have Document ID
    ,mailno AS transmittal_number
    ,subject AS title --  Need to check with Adam is this the correct field??
    ,reasonforissue AS reason_for_issue
    ,referencenumber AS reference_number
    ,sentdate AS sent_date
    ,CASE WHEN responserequired_responserequireddate IS NULL THEN false ELSE true END AS response_required --confirm with Adam is it the correct field?
    ,responserequired_responserequireddate AS response_due_date
    ,tousers_recipient_name AS recipient -- temp field to be replaced once the people domain is completed.
    ,tousers_recipient_organizationname AS recipient_organization
    ,'' AS distribution_email  -- Which field to use? 
    ,tousers_recipient_distributiontype AS distribution_type
-- Datahub standard Meta data fields
    ,projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN getdate()
        ELSE eff_end_date
    END AS meta_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    transmittal T

-- UNION ALL

-- SELECT DISTINCT
--      source_system_name + '|' + transmittal_number AS transmittal_key
--     ,source_system_name + '|' + transmittal_number AS transmittal_sid
--     ,transmittal_number
--     ,null AS title
--     ,transmittal_reason_for_issue AS reason_for_issue
--     ,contract AS reference_number
--     ,transmittal_creation_date AS sent_date
--     ,CASE WHEN transmittal_suggested_due_date IS NULL THEN false ELSE true END AS response_Required
--     ,transmittal_suggested_due_date AS response_due_date
--     ,recipient
--     ,split_part(split_part(recipient_e_mail_address,'@',2),'.',1) AS recipient_organization
--     ,recipient_e_mail_address as distribution_email
--     ,workflow_step AS distribution_type
-- -- Datahub standard Meta data fields
--     ,'1207979025' AS meta_project_rls_key
--     ,execution_date AS meta_ingestion_date
--     ,execution_date AS meta_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
--     ,{{run_date}} as dbt_created_date
--     ,{{run_date}} as dbt_updated_date
--     ,{{ generate_load_id(model) }} as dbt_load_id
-- FROM
--     hexagon