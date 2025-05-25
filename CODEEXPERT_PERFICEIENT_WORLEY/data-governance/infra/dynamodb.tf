module "role_permissions_store" {
  source  = "terraform-aws-modules/dynamodb-table/aws"
  version = "4.1.0"

  name     = "worley-mf-rbac-${var.stage}-${var.environment}-role-permissions-metadata"
  hash_key = "tag_name"
  #TODO ADD KMS
  point_in_time_recovery_enabled = true

  attributes = [
    {
      name = "tag_name"
      type = "S"
    }
  ]
}

module "database_permissions_store" {
  source  = "terraform-aws-modules/dynamodb-table/aws"
  version = "4.1.0"

  name     = "worley-mf-rbac-${var.stage}-${var.environment}-database-permissions-metadata"
  hash_key = "database_name"

  stream_enabled                 = true
  stream_view_type               = "KEYS_ONLY"
  point_in_time_recovery_enabled = true

  #TODO ADD KMS

  attributes = [
    {
      name = "database_name"
      type = "S"
    }
  ]
}