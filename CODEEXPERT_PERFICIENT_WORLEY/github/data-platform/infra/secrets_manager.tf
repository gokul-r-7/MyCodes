locals {
  schema = ["aconex", "vg_e3d", "construction", "document_control", "global_standard_reporting", "project_control", "supply_chain", "finance", "dac", "engineering","customer", "circuit_breaker","health_safety_environment","integrations","worley_nerve_center"] ## add the list of schema here 
}

#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "secrets_manager" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "terraform-aws-modules/secrets-manager/aws"
  version = "1.1.2"

  for_each = toset(local.schema)

  # Secret
  name_prefix             = "${var.namespace}-${var.name}-${var.stage}-${var.environment}-dbt-${each.value}-user"
  description             = "dbt user for ${each.value}"
  recovery_window_in_days = 30

  # Policy
  create_policy       = true
  block_public_policy = true

  #TODO lock this down to airflow later

  policy_statements = {
    read = {
      sid = "AllowAccountRead"
      principals = [{
        type        = "AWS"
        identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
      }]
      actions   = ["secretsmanager:GetSecretValue"]
      resources = ["*"]
    }
  }

  # Version
  create_random_password           = true
  random_password_length           = 64
  random_password_override_special = "!@#$%^&*()_+"

  tags = merge(local.account.tags, {
    Namespace   = "worley",
    Stage       = "sydney",
    Terraform   = "true",
    Project     = var.namespace
  }
  )
}
