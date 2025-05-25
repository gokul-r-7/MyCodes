################################################################################
# Landing DynamoDB
################################################################################

module "dynamodb_landing_table_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["dynamodb", "landing", "table"]
}

module "dynamodb_landing_table" {
  source  = "terraform-aws-modules/dynamodb-table/aws"
  version = "4.0.1"

  name     = module.dynamodb_landing_table_label.id
  hash_key = "id"

  server_side_encryption_enabled     = true
  server_side_encryption_kms_key_arn = module.kms_key.key_arn

  point_in_time_recovery_enabled = true

  attributes = [
    {
      name = "id"
      type = "N"
    }
  ]

  tags = module.dynamodb_landing_table_label.tags
}

################################################################################
# Project Mapping DynamoDB
################################################################################

module "dynamodb_project_mapping_table_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["dynamodb", "project", "mapping", "table"]
}

module "dynamodb_project_mapping_table" {
  source  = "terraform-aws-modules/dynamodb-table/aws"
  version = "4.0.1"

  name     = module.dynamodb_project_mapping_table_label.id
  hash_key = "domain"
  range_key = "target_table"

  server_side_encryption_enabled     = true
  server_side_encryption_kms_key_arn = module.kms_key.key_arn

  point_in_time_recovery_enabled = true

  attributes = [
    {
      name = "domain"
      type = "S"
    },
    {
      name = "target_table"
      type = "S"
    }
  ]

  tags = module.dynamodb_landing_table_label.tags
}