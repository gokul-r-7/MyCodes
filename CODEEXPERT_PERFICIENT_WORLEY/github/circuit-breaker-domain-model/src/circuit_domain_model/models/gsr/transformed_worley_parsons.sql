{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_worley_parsons/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}



select DISTINCT CASE
    WHEN POSITION('GSR' IN worley_parsons_id) > 0 THEN 'GSR' || RIGHT(
      '0000' || SUBSTRING(
        worley_parsons_id,
        POSITION('GSR' IN worley_parsons_id) + 3,
        LENGTH(worley_parsons_id) - POSITION('GSR' IN worley_parsons_id) - 2
      ),
      4
    )
    ELSE 'GSR' || RIGHT('0000' || worley_parsons_id, 4)
  END as worley_parsons_id,
  case
    when UPPER(worley_parsons_application_name) = 'NAN' then Null
    else worley_parsons_application_name
  end as worley_parsons_application_name,
  case
    when UPPER(worley_parsons_component_name) = 'NAN' then Null
    else worley_parsons_component_name
  end as worley_parsons_component_name,
  case
    when UPPER(worley_parsons_versions) = 'NAN' then Null
    else worley_parsons_versions
  end as worley_parsons_versions,
  case
    when UPPER(worley_parsons_publisher_name) = 'NAN' then Null
    else worley_parsons_publisher_name
  end as worley_parsons_publisher_name,
  case
    when UPPER(worley_parsons_application_maintenance_status) = 'NAN' then Null
    else worley_parsons_application_maintenance_status
  end as worley_parsons_application_maintenance_status,
  case
    when UPPER(worley_parsons_license_type) = 'NAN' then Null
    else worley_parsons_license_type
  end as worley_parsons_license_type,
  case
    when UPPER(worley_parsons_concurrent_named) = 'NAN' then Null
    else worley_parsons_concurrent_named
  end as worley_parsons_concurrent_named,
  worley_parsons_all as worley_parsons_license_quantity_all,
  worley_parsons_network_license as worley_parsons_license_quantity_network_license,
  worley_parsons_standalone as worley_parsons_license_quantity_standalone,
  worley_parsons_dongle as worley_parsons_license_quantity_dongle,
  worley_parsons_others as worley_parsons_license_quantity_others,
  worley_parsons_support_maintenance as worley_parsons_license_quantity_support_maintenance,
  worley_parsons_token_weight as worley_parsons_token_quantity_token_weight,
  worley_parsons_token_unit as worley_parsons_token_quantity_token_unit,
  worley_parsons_total_tokens as worley_parsons_token_quantity_total_tokens,
  case
    when UPPER(
      worley_parsons_licensed_locations_local_licenses_usually_have_internal_restrictions
    ) = 'NAN' then Null
    else worley_parsons_licensed_locations_local_licenses_usually_have_internal_restrictions
  end as worley_parsons_licensed_locations_local_licenses_usually_have_internal_restrictions,
  case
    when UPPER(worley_parsons_vendors_restrictions) = 'NAN' then Null
    else worley_parsons_vendors_restrictions
  end as worley_parsons_vendors_restrictions,
  case
    when UPPER(
      worley_parsons_evidence_of_entitlement_procon_contract_reference_number
    ) = 'NAN' then Null
    else worley_parsons_evidence_of_entitlement_procon_contract_reference_number
  end as worley_parsons_evidence_of_entitlement_procon_contract_reference_number,
  case
    WHEN UPPER(worley_parsons_license_start_date) = 'NAN' then Null
    else worley_parsons_license_start_date
  END as worley_parsons_license_start_date,
  case
    when UPPER(worley_parsons_license_end_date) = 'NAN' then Null --when worley_parsons_entry_discontinued_date IN ('31 Nov 2023') then Null
    else worley_parsons_license_end_date
  END as worley_parsons_license_end_date,
  case
    when UPPER(worley_parsons_application_owner) = 'NAN' then Null
    else worley_parsons_application_owner
  end as worley_parsons_application_owner,
  case
    when UPPER(worley_parsons_license_manager) = 'NAN' then Null
    else worley_parsons_license_manager
  end as worley_parsons_license_manager,
  case
    when UPPER(worley_parsons_additional_info_free_text_field) = 'NAN' then Null
    else worley_parsons_additional_info_free_text_field
  end as worley_parsons_additional_info_free_text_field,
  case
    when UPPER(worley_parsons_installation_path) = 'NAN' then Null
    else worley_parsons_installation_path
  end as worley_parsons_installation_path,
  case
    when UPPER(worley_parsons_license_servers) = 'NAN' then Null
    else worley_parsons_license_servers
  end as worley_parsons_license_servers,
  case
    when UPPER(worley_parsons_deployment) = 'NAN' then Null
    else worley_parsons_deployment
  end as worley_parsons_deployment,
  case
    when UPPER(worley_parsons_citrix) = 'NAN' then Null
    else worley_parsons_citrix
  end as worley_parsons_citrix,
  case
    when UPPER(worley_parsons_software_category) = 'NAN' then Null
    else worley_parsons_software_category
  end as worley_parsons_software_category,
  case
    when UPPER(worley_parsons_status) = 'NAN' then Null
    else worley_parsons_status
  end as worley_parsons_status,
  case
    when UPPER(worley_parsons_entry_discontinued_date) = 'NAN' then Null -- when worley_parsons_entry_discontinued_date IN ('25/1/2024','25/1/2025','26/1/2025') then Null
    else worley_parsons_entry_discontinued_date
  end as worley_parsons_entry_discontinued_date,
    
    CASE
        WHEN execution_date IS NULL THEN NULL
        ELSE CAST(execution_date AS TIMESTAMP)
    END AS execution_date,
    
  {{run_date}} as model_created_date,
  {{run_date}} as model_updated_date,
  {{ generate_load_id(model) }} as model_load_id
    
from {{ source('curated_hexagon', 'curated_entitlement_worley_parsons') }}