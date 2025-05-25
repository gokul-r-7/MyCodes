module "redshift_serverless_namespace_label" {
  source     = "cloudposse/label/null"
  version    = "0.25.0"
  attributes = ["redshift", "serverless", "namespace"]
  context    = module.label.context
}

resource "aws_redshiftserverless_namespace" "redshift_serverless_namespace" {
  count                            = var.create_redshift_serverless ? 1 : 0
  namespace_name                   = module.redshift_serverless_namespace_label.id
  kms_key_id                       = module.kms_redshift.key_arn
  manage_admin_password            = true
  iam_roles = [
    aws_iam_role.redshift.arn,
    module.redshit_idc_integrations_iam_role.arn
  ]
  default_iam_role_arn = module.redshit_idc_integrations_iam_role.arn
  tags                 = module.redshift_serverless_namespace_label.tags
}

module "redshift_serverless_workgroup_label" {
  source     = "cloudposse/label/null"
  version    = "0.25.0"
  attributes = ["redshift", "serverless", "workgroup"]
  context    = module.label.context
}

resource "aws_redshiftserverless_workgroup" "redshift_serverless_workgroup" {
  count                = var.create_redshift_serverless ? 1 : 0
  namespace_name       = aws_redshiftserverless_namespace.redshift_serverless_namespace[0].id
  workgroup_name       = module.redshift_serverless_workgroup_label.id
  enhanced_vpc_routing = false
  security_group_ids   = [module.redshift_security_group.security_group_id]
  subnet_ids           = data.aws_subnets.private_redshift_serverless.ids
  tags                 = module.redshift_serverless_workgroup_label.tags
}

resource "aws_redshiftserverless_endpoint_access" "redshift_serverless_endpoint" {
  count                  = var.create_redshift_serverless ? 1 : 0
  endpoint_name          = "serverless-${var.environment}-endpoint"
  workgroup_name         = aws_redshiftserverless_workgroup.redshift_serverless_workgroup[0].id
  subnet_ids             = data.aws_subnets.private_redshift_serverless.ids
  vpc_security_group_ids = [module.redshift_security_group.security_group_id]
}
