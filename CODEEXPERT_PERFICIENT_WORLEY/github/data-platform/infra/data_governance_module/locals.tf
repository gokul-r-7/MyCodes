locals {
  aws_account_id  = data.aws_caller_identity.current.account_id
  region          = data.aws_region.current.name
  data_gov_config = yamldecode(file("${path.module}/config/${var.environment}.yml"))
}