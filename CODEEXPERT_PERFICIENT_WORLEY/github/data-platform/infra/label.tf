module "label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  namespace   = var.namespace
  environment = var.environment
  stage       = var.stage
  name        = var.name
  tags = local.account.tags

  label_order = ["namespace", "name", "stage", "environment", "attributes"]
}

module "metadata_framework_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  name    = "mf"
  context = module.label.context
}

module "ohio_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  namespace   = var.namespace
  environment = var.environment
  stage       = var.ohio_stage
  name        = var.name
  tags = local.account.tags

  label_order = ["namespace", "name", "stage", "environment", "attributes"]
}