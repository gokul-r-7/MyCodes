{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_supplier_report/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

WITH supplier_report as 
(
    SELECT
        `file`
        ,package_number
        ,document_no
        ,title
        ,revision
        ,submission_status
        ,review_status
        ,assigned_to_org
        ,due_in
        ,due_out
        ,milestone_date
        ,`lock`
        ,actual_in
        ,actual_out
        ,as_built_required
        ,assigned_to_users
        ,attribute_1
        ,attribute_2
        ,attribute_3
        ,attribute_4
        ,category
        ,check_1
        ,comments
        ,comments_in
        ,comments_out
        ,confidential
        ,contractor_doc_no
        ,contractor_rev
        ,created_by
        ,date_2
        ,date_created
        ,date_to_client
        ,date_modified
        ,days_late
        ,description
        ,discipline
        ,`file_name`
        ,planned_submission_date
        ,print_size
        ,project_field_3
        ,reference
        ,required_by
        ,review_source
        ,select_list_1
        ,select_list_2
        ,select_list_3
        ,select_list_4
        ,select_list_5
        ,select_list_6
        ,select_list_7
        ,select_list_8
        ,select_list_9
        ,select_list_10
        ,`size`
        ,status
        ,submission_sequence
        ,supplied_by
        ,tag_no
        ,transmittal
        ,type
        ,vdr_code
        ,vendor_doc_no
        ,vendor_rev
        ,`version`
        ,Project_code
        ,instance_name
        ,source_system_name	
        ,project_id
        ,execution_date
    FROM 
        {{ source('curated_aconex', 'curated_supplier_report') }}
),
supplier_report_transformed as
(
    SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY project_id,document_no ORDER BY CAST(version AS INT) DESC) AS row_num
    FROM
        supplier_report 
)

SELECT
    `file`
    ,package_number
    ,document_no as document_number
    ,title
    ,revision
    ,submission_status
    ,review_status
    ,assigned_to_org
    ,CASE 
        WHEN due_in IS NULL OR TRIM(due_in) = '' THEN NULL 
        WHEN due_in RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(due_in, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS due_in
    ,CASE 
        WHEN due_out IS NULL OR TRIM(due_out) = '' THEN NULL 
        WHEN due_out RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(due_out, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS due_out
    ,CASE 
        WHEN milestone_date IS NULL OR TRIM(milestone_date) = '' THEN NULL 
        WHEN milestone_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(milestone_date, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS milestone_date
    ,`lock`
    ,CASE 
        WHEN actual_in IS NULL OR TRIM(actual_in) = '' THEN NULL 
        WHEN actual_in RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(actual_in, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS actual_in
    ,CASE 
        WHEN actual_out IS NULL OR TRIM(actual_out) = '' THEN NULL 
        WHEN actual_out RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(actual_out, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS actual_out
	,as_built_required
    ,assigned_to_users as assigned_to
    ,attribute_1
    ,attribute_2 as tag_number
    ,attribute_3
    ,attribute_4 as sdr_codes
    ,category as document_identifier
    ,check_1 as design_critical
    ,comments
    ,comments_in
    ,comments_out
    ,confidential
    ,contractor_doc_no
    ,contractor_rev
    ,created_by
    ,date_2
    ,date_created
    ,date_to_client
    ,date_modified
    ,days_late
    ,description
    ,discipline as area_or_asset_number
    ,`file_name`
    ,CASE 
        WHEN planned_submission_date IS NULL OR TRIM(planned_submission_date) = '' THEN NULL 
        WHEN planned_submission_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
        THEN to_date(
            regexp_replace(planned_submission_date, '^([0-9])/([0-9])/([0-9]{4})$', '0\1/0\2/\3'), 
            'dd/MM/yyyy'
        ) 
        ELSE NULL  -- If the date does not match the expected pattern, return NULL
    END AS planned_submission_date
    ,print_size
    ,project_field_3
    ,reference
    ,required_by
    ,review_source
    ,select_list_1
    ,select_list_2 as FILE_CLASSIFICATION
    ,select_list_3
    ,select_list_4
    ,select_list_5
    ,select_list_6
    ,select_list_7 as supplier
    ,select_list_8 as purchase_order_number
    ,select_list_9 as jip33
    ,select_list_10
    ,`size`
    ,status
    ,COALESCE(submission_sequence, 0) as submission_sequence
    ,supplied_by
    ,tag_no
    ,transmittal
    ,type as DOCUMENT_TYPE_NAME
    ,vdr_code as discipline
    ,vendor_doc_no
    ,vendor_rev
    ,`version`
    ,Project_code
    ,instance_name
    ,source_system_name
    ,CASE WHEN row_num = 1 THEN TRUE ELSE FALSE END AS is_current --Is this verions flagged as this has historical
    ,CAST(project_id AS VARCHAR(100)) AS dim_project_rls_key
    ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
    ,to_timestamp(execution_date, 'yyyy-MM-dd HH:mm:ss') AS dim_snapshot_date	
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    supplier_report_transformed WHERE `version` IS NOT NULL AND `version` <> ''
