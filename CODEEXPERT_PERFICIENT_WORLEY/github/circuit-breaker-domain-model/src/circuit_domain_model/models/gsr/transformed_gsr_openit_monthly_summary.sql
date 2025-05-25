{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_gsr_openit_monthly_summary/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}

select DISTINCT CASE
    WHEN POSITION(
      'GSR' IN openit.openit_monthly_summary_applicationname
    ) > 0 THEN 'GSR' || RIGHT(
      '0000' || SUBSTRING(
        openit.openit_monthly_summary_applicationname,
        POSITION(
          'GSR' IN openit.openit_monthly_summary_applicationname
        ) + 3,
        LENGTH(openit.openit_monthly_summary_applicationname) - POSITION(
          'GSR' IN openit.openit_monthly_summary_applicationname
        ) - 2
      ),
      4
    )
    ELSE NULL
  END as openit_monthly_summary_id,
     ROUND(
        CAST(
            CASE
                WHEN openit_monthly_summary_maxavailable = 0 THEN NULL
                WHEN openit_monthly_summary_maxinuse IS NULL OR openit_monthly_summary_maxavailable IS NULL THEN NULL
                WHEN openit_monthly_summary_maxinuse = '' OR openit_monthly_summary_maxavailable = '' THEN NULL
                WHEN NOT openit_monthly_summary_maxinuse rlike '^[0-9]+(\.[0-9]*)?$' THEN NULL
                WHEN NOT openit_monthly_summary_maxavailable rlike '^[0-9]+(\.[0-9]*)?$' THEN NULL
                ELSE 
                
                    CAST(openit_monthly_summary_maxinuse AS DECIMAL(18,9))
                    /CAST(openit_monthly_summary_maxavailable AS DECIMAL(18,9))
        
            END AS NUMERIC(18, 9)
        ),
        2
    ) AS maxutilization,
  --Round(maxutilization, 2) AS maxutilization,
  LEFT(
    openit_monthly_summary_applicationname,
    POSITION('GSR' IN openit_monthly_summary_applicationname) - 1
  ) AS openit_monthly_summary_applicationname,
  case
    when UPPER(openit_monthly_summary_applicationversion) = 'NAN' then Null
    else openit_monthly_summary_applicationversion
  end as openit_monthly_summary_applicationversion,
  case
    when UPPER(openit_monthly_summary_year) = 'NAN' then Null
    else openit_monthly_summary_year
  end as openit_monthly_summary_year,
  case
    when UPPER(openit_monthly_summary_month) = 'NAN' then Null
    else openit_monthly_summary_month
  end as openit_monthly_summary_month,
  case
    when UPPER(openit_monthly_summary_elapsedtime) = 'NAN' then Null
    else openit_monthly_summary_elapsedtime
  end as openit_monthly_summary_elapsedtime,
  case
    when UPPER(openit_monthly_summary_maxinuse) = 'NAN' then Null
    else openit_monthly_summary_maxinuse
  end as openit_monthly_summary_maxinuse,
  case
    when UPPER(openit_monthly_summary_maxavailable) = 'NAN' then Null
    else openit_monthly_summary_maxavailable
  end as openit_monthly_summary_maxavailable,
  case
    when UPPER(openit_monthly_summary_denials) = 'NAN' then Null
    else openit_monthly_summary_denials
  end as openit_monthly_summary_denials,
  case
    when UPPER(openit_monthly_summary_distinctuser) = 'NAN' then Null
    else openit_monthly_summary_distinctuser
  end as openit_monthly_summary_distinctuser,
  case
    when UPPER(openit_monthly_summary_distincthost) = 'NAN' then Null
    else openit_monthly_summary_distincthost
  end as openit_monthly_summary_distincthost,
  gsr_id as gsr_id,
  case
    when UPPER(gsr_name) = 'NAN' then Null
    else gsr_name
  end as gsr_name,
  case
    when UPPER(gsr_publisher_gsr) = 'NAN' then Null
    else gsr_publisher_gsr
  end as gsr_publisher_gsr,
  case
    when UPPER(gsr_publisher_website) = 'NAN' then Null
    else gsr_publisher_website
  end as gsr_publisher_website,
  case
    when UPPER(gsr_gsr_status) = 'NAN' then Null
    else gsr_gsr_status
  end as gsr_gsr_status,
  cast(
    case
      when upper(gsr_target_retirement_date) IN ('','NAT') then Null
      else gsr_target_retirement_date
    end as timestamp
  ) as gsr_target_retirement_date,
  cast(
    case
      when UPPER(gsr_decommissioned_date) IN ('','NAT') then Null
      else gsr_decommissioned_date
    end as timestamp
  ) as gsr_decommissioned_date,
  case
    when UPPER(gsr_global_agreement) = 'NAN' then Null
    else gsr_global_agreement
  end as gsr_global_agreement,
  case
    when UPPER(gsr_worley_category) = 'NAN' then Null
    else gsr_worley_category
  end as gsr_worley_category,
  case
    when UPPER(gsr_function) = 'NAN' then Null
    else gsr_function
  end as gsr_function,
  case
    when UPPER(gsr_software_function) = 'NAN' then Null
    else gsr_software_function
  end as gsr_software_function,
  case
    when UPPER(gsr_software_origin) = 'NAN' then Null
    else gsr_software_origin
  end as gsr_software_origin,
  case
    when UPPER(gsr_software_type) = 'NAN' then Null
    else gsr_software_type
  end as gsr_software_type,
  case
    when UPPER(gsr_software_long_description) = 'NAN' then Null
    else gsr_software_long_description
  end as gsr_software_long_description,
  case
    when UPPER(gsr_product_manager) = 'NAN' then Null
    else gsr_product_manager
  end as gsr_product_manager,
  case
    when UPPER(gsr_functional_sme) = 'NAN' then Null
    else gsr_functional_sme
  end as gsr_functional_sme,
  case
    when UPPER(gsr_technical_sme) = 'NAN' then Null
    else gsr_technical_sme
  end as gsr_technical_sme,
  case
    when UPPER(gsr_technical_product_manager) = 'NAN' then Null
    else gsr_technical_product_manager
  end as gsr_technical_product_manager,
  case
    when UPPER(gsr_level_3_resolver_group) = 'NAN' then Null
    else gsr_level_3_resolver_group
  end as gsr_level_3_resolver_group,
  case
    when UPPER(gsr_installation_resolver_group) = 'NAN' then Null
    else gsr_installation_resolver_group
  end as gsr_installation_resolver_group,
  case
    when UPPER(gsr_supported_by_it_team) = 'NAN' then Null
    else gsr_supported_by_it_team
  end as gsr_supported_by_it_team,
  case
    when UPPER(gsr_application_packaged) = 'NAN' then Null
    else gsr_application_packaged
  end as gsr_application_packaged,
  case
    when UPPER(gsr_available_in_software_center) = 'NAN' then Null
    else gsr_available_in_software_center
  end as gsr_available_in_software_center,
  case
    when UPPER(gsr_architecture_review) = 'NAN' then Null
    else gsr_architecture_review
  end as gsr_architecture_review,
  cast(
    case
      when UPPER(gsr_architecture_review_date) IN ('','NAT') then Null
      else gsr_architecture_review_date
    end as timestamp
  ) as gsr_architecture_review_date,
  case
    when UPPER(gsr_architecture_reference) = 'NAN' then Null
    else gsr_architecture_reference
  end as gsr_architecture_reference,
  case
    when UPPER(gsr_legal_review) = 'NAN' then Null
    else gsr_legal_review
  end as gsr_legal_review,
  cast(
    case
      when UPPER(gsr_legal_review_date) IN ('','NAT') then Null
      else gsr_legal_review_date
    end as timestamp
  ) as gsr_legal_review_date,
  case
    when UPPER(gsr_legal_reference) = 'NAN' then Null
    else gsr_legal_reference
  end as gsr_legal_reference,
  case
    when UPPER(gsr_security_review) = 'NAN' then Null
    else gsr_security_review
  end as gsr_security_review,
  cast(
    case
      when UPPER(gsr_security_review_date) IN ('','NAT') then Null
      else gsr_security_review_date
    end as timestamp
  ) as gsr_security_review_date,
  case
    when UPPER(gsr_security_reference) = 'NAN' then Null
    else gsr_security_reference
  end as gsr_security_reference,
  case
    when UPPER(gsr_security_compliance) = 'NAN' then Null
    else gsr_security_compliance
  end as gsr_security_compliance,
  case
    when UPPER(gsr_pia_review) = 'NAN' then Null
    else gsr_pia_review
  end as gsr_pia_review,
  cast(
    case
      when UPPER(gsr_pia_review_date) IN ('','NAT') then Null
      else gsr_pia_review_date
    end as timestamp
  ) as gsr_pia_review_date,
  case
    when UPPER(gsr_pia_reference) = 'NAN' then Null
    else gsr_pia_reference
  end as gsr_pia_reference,
  case
    when UPPER(gsr_superseded_by) = 'NAN' then Null
    else gsr_superseded_by
  end as gsr_superseded_by,
  case
    when UPPER(gsr_sso_enabled) = 'NAN' then Null
    else gsr_sso_enabled
  end as gsr_sso_enabled,
  case
    when UPPER(gsr_mobile_app_included) = 'NAN' then Null
    else gsr_mobile_app_included
  end as gsr_mobile_app_included,
  case
    when UPPER(gsr_ecr) = 'NAN' then Null
    else gsr_ecr
  end as gsr_ecr,
  case
    when UPPER(gsr_additional_information) = 'NAN' then Null
    else gsr_additional_information
  end as gsr_additional_information,
  case
    when UPPER(gsr_core) = 'NAN' then 'No'
    else gsr_core
  end as gsr_core,
  case
    when UPPER(gsr_applicable_project_delivery_phase) = 'NAN' then Null
    else gsr_applicable_project_delivery_phase
  end as gsr_applicable_project_delivery_phase,
  case
    when UPPER(gsr_installations) = 'NAN' then Null
    else gsr_installations
  end as gsr_installations,
  case
    when UPPER(gsr_used_installations) = 'NAN' then Null
    else gsr_used_installations
  end as gsr_used_installations,
  case
    when UPPER(gsr_application_support_partner) = 'NAN' then Null
    else gsr_application_support_partner
  end as gsr_application_support_partner,
  case
    when UPPER(gsr_level_2_resolver_group) = 'NAN' then Null
    else gsr_level_2_resolver_group
  end as gsr_level_2_resolver_group,
  case
    when UPPER(gsr_access_method) = 'NAN' then Null
    else gsr_access_method
  end as gsr_access_method,
  case
    when UPPER(gsr_application_url) = 'NAN' then Null
    else gsr_application_url
  end as gsr_application_url,
  case
    when UPPER(gsr_number_of_versions_and_editons) = 'NAN' then Null
    else gsr_number_of_versions_and_editons
  end as gsr_number_of_versions_and_editons,
  case
    when UPPER(gsr_function2) = 'NAN' then Null
    else gsr_function2
  end as gsr_function2,
  case
    when UPPER(gsr_windows_11_ready) = 'NAN' then Null
    else gsr_windows_11_ready
  end as gsr_windows_11_ready,
  case
    when UPPER(gsr_servicenow_reference) = 'NAN' then Null
    else gsr_servicenow_reference
  end as gsr_servicenow_reference,
  case
    when UPPER(gsr_exception_request_reference) = 'NAN' then Null
    else gsr_exception_request_reference
  end as gsr_exception_request_reference,
  case
    when UPPER(gsr_it_standard) = 'NAN' then Null
    else gsr_it_standard
  end as gsr_it_standard,
  case
    when UPPER(gsr_item_type) = 'NAN' then Null
    else gsr_item_type
  end as gsr_item_type,
  case
    when UPPER(gsr_path) = 'NAN' then Null
    else gsr_path
  end as gsr_path,
   CAST(openit_monthly_summary_year || '-' || 
                CASE openit_monthly_summary_month
                    WHEN 'Jan' THEN '01'
                    WHEN 'Feb' THEN '02'
                    WHEN 'Mar' THEN '03'
                    WHEN 'Apr' THEN '04'
                    WHEN 'May' THEN '05'
                    WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07'
                    WHEN 'Aug' THEN '08'
                    WHEN 'Sep' THEN '09'
                    WHEN 'Oct' THEN '10'
                    WHEN 'Nov' THEN '11'
                    WHEN 'Dec' THEN '12'
                END || '-01' AS date) AS openit_monthly_summary_transformed_date,
    
    CASE
        WHEN openit.execution_date IS NULL THEN NULL
        ELSE CAST(openit.execution_date AS TIMESTAMP)
    END AS execution_date,
    
  {{run_date}} as model_created_date,
  {{run_date}} as model_updated_date,
  {{ generate_load_id(model) }} as model_load_id
    
from {{ source('curated_hexagon', 'curated_consumption_openit_monthly_summary') }} openit
join {{ source('curated_hexagon', 'curated_entitlement_global_software_register') }} gsr
on CASE
    WHEN POSITION(
      'GSR' IN openit.openit_monthly_summary_applicationname
    ) > 0 THEN 'GSR' || RIGHT(
      '0000' || SUBSTRING(
        openit.openit_monthly_summary_applicationname,
        POSITION(
          'GSR' IN openit.openit_monthly_summary_applicationname
        ) + 3,
        LENGTH(openit.openit_monthly_summary_applicationname) - POSITION(
          'GSR' IN openit.openit_monthly_summary_applicationname
        ) - 2
      ),
      4
    )
    ELSE NULL
  END = gsr.gsr_id