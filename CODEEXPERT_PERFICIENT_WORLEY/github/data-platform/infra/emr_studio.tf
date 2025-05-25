module "emr_studio" {
  source = "./emr_studio_module"

  emr_studio_admin_group = var.emr_studio_admin_group
  vpc_id                 = var.vpc_id
  aws_region             = var.aws_region
  kms_key_id             = module.kms_key.key_id
  subnet_ids             = data.aws_subnets.private_dataware.ids
  label_context          = module.label.context
  environment            = var.environment
  account_id             = data.aws_caller_identity.current.account_id
  results_bucket         = module.bucket_athena_results_outputs.s3_bucket_id
  athena_workgroup       = "${var.environment}-idc-workgroup"
}