module "secret_landing_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["secret", "landing"]
}

#checkov:skip=CKV_AWS_304:Automatic secret rotation is not enabled for this secret.
module "secret_landing" {
  source  = "terraform-aws-modules/secrets-manager/aws"
  version = "1.1.2"

  # Secret
  name_prefix             = "${module.secret_landing_label.id}-"
  description             = "Landing Secret"
  recovery_window_in_days = 30

  # Policy
  create_policy       = true
  block_public_policy = true
  policy_statements = {
    read = {
      sid = "AllowAccountRead"
      principals = [{
        type        = "AWS"
        identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.id}:root"]
      }]
      actions   = ["secretsmanager:GetSecretValue"]
      resources = ["*"]
    }
  }

  # Version
  create_random_password           = true
  random_password_length           = 64
  random_password_override_special = "!@#$%^&*()_+"

  tags = module.secret_landing_label.tags
}
