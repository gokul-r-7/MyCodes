{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_worley_parsons_gsr/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}







SELECT DISTINCT CASE
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
    cast(case
        WHEN worley_parsons_license_start_date = '' then Null
        else worley_parsons_license_start_date
    END as timestamp) as worley_parsons_license_start_date,
    cast(case
        when worley_parsons_license_end_date = '' then Null 
        else worley_parsons_license_end_date
    END as timestamp) as worley_parsons_license_end_date,
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
    cast(case
        when worley_parsons_entry_discontinued_date = '' then Null -- when worley_parsons_entry_discontinued_date IN ('25/1/2024','25/1/2025','26/1/2025') then Null
        else worley_parsons_entry_discontinued_date
    end as timestamp) as worley_parsons_entry_discontinued_date,
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
            when gsr_target_retirement_date = '' then Null
            else gsr_target_retirement_date
        end as timestamp
    ) as gsr_target_retirement_date,
    cast(
        case
            when gsr_decommissioned_date = '' then Null
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
    fm.functionalmapping_function as gsr_function,
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
            when gsr_architecture_review_date = '' then Null
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
            when gsr_legal_review_date = '' then Null
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
            when gsr_security_review_date = '' then Null
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
            when gsr_pia_review_date = '' then Null
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
    
    CASE
        WHEN wp.execution_date IS NULL THEN NULL
        ELSE CAST(wp.execution_date AS TIMESTAMP)
    END AS execution_date,
    
  {{run_date}} as model_created_date,
  {{run_date}} as model_updated_date,
  {{ generate_load_id(model) }} as model_load_id
    
from {{ source('curated_hexagon', 'curated_entitlement_worley_parsons') }} wp
left join {{ source('curated_hexagon', 'curated_entitlement_global_software_register') }} gsr 
on gsr.gsr_id = CASE
        -- Check if it starts with 'GSR', format accordingly
        WHEN LEFT(
            LEFT(
                wp.worley_parsons_id,
                instr(',', wp.worley_parsons_id + ',') - 1
            ),
            3
        ) = 'GSR' THEN 'GSR' + RIGHT(
            '0000000' + SUBSTRING(
                LEFT(
                    wp.worley_parsons_id,
                    instr(',', wp.worley_parsons_id + ',') - 1
                ),
                4,
                LEN(
                    LEFT(
                        wp.worley_parsons_id,
                        instr(',', wp.worley_parsons_id + ',') - 1
                    )
                ) - 3
            ),
            4
        )
        ELSE -- If no 'GSR', treat it as numeric and add 'GSR' prefix
        'GSR' + RIGHT(
            '0000000' + LEFT(
                wp.worley_parsons_id,
                instr(',', wp.worley_parsons_id + ',') - 1
            ),
            4
        )
    END
    left join {{ source('curated_hexagon', 'curated_entitlement_worley_functionalgroup_to_functionalmapping') }} fm 
    on LEFT(gsr.gsr_worley_category,5) = fm.functionalmapping_functional_group_primary_short
where wp.worley_parsons_license_end_date not in (
        'unlimited',
        'Unknown',
        'No expiry date',
        '31 Nov 2023',
        'N/A'
    ) and  worley_parsons_entry_discontinued_date not in ('15/12','25/1/2024','25/1/2025','26/1/2025')