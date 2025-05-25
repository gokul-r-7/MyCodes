locals {
  genai_azure_app_api_key_map = {
    dev = "api://97b15505-eec9-4146-a0bc-cba4ce8e8c61"
    qa  = "api://0eb28ac5-7390-4163-8b79-0bdd68012f07"
    prd = "api://031baa23-add1-453f-b8b1-3abcaa6b0eb4"
  }
  genai_azure_app_api_key_value = local.genai_azure_app_api_key_map[var.environment]

  aconex_bot_report_secrets_extension_key_map = {
    dev = "82fjIf"
    qa  = "SE5tDS"
    prd = "3YVlfC"
  }
  aconex_bot_us_scrt_arn_ext_key_value = local.aconex_bot_report_secrets_extension_key_map[var.environment]
  account = jsondecode(nonsensitive(data.aws_ssm_parameter.account_metadata.value))
  kms_keys = [
    module.kms_key.key_arn,
    module.kms_key_primary.key_arn
  ]
}

locals {
  retention_in_days = var.environment == "prd" ? 90 : 30
}