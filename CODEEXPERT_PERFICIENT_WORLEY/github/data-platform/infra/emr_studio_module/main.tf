
module "emr_studio" {
  source = "./studio"

  name                = "EMR_Studio_${var.environment}"
  description         = "EMR Studio using SSO authentication"
  auth_mode           = "SSO"
  default_s3_location = "s3://${module.emr_studio_backup_label.id}/example"

  create_security_groups = false

  service_role_statements = [
    {
      sid    = "KMSaccess"
      effect = "Allow"
      actions = [
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:Encrypt",
        "kms:GetKeyPolicy",
        "kms:ListKeys",
        "kms:GenerateDataKey"
      ]
      resources = ["*"]
    }
  ]

  user_role_statements = [
    {
      sid = "AllowS3Access"
      actions = [
        "s3:Get*",
        "s3:List*",
        "s3:PutObject"
      ]
      resources = ["arn:aws:s3:::worley*"]

    },
    {
      sid = "AllowS3AthenaBucketAccess"
      actions = [
        "s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListMultipartUploadParts",
        "s3:PutObject"
      ],
      resources = [
        "arn:aws:s3:::*athena-query-results*"
      ]
    },
    {
      sid    = "AthenaFullAccess"
      effect = "Allow"
      actions = [
        "athena:GetDataCatalog",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup",
        "athena:StartQueryExecution",
        "athena:StopQueryExecution",
        "athena:GetQueryRuntimeStatistics",
        "athena:ListDataCatalogs",
        "athena:ListQueryExecutions",
        "athena:ListWorkGroups",
        "athena:GetCatalogImportStatus",
        "athena:GetQueryResultsStream",
        "athena:StartQueryExecution"
      ]

      resources = [
        "arn:aws:athena:*:${data.aws_caller_identity.current.account_id}:workgroup/*",
        "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:datacatalog/*"
      ]
    },
    {
      sid       = "GlueFullAccess"
      effect    = "Allow"
      actions   = ["glue:*"]
      resources = ["*"]
    },
    {
      sid    = "KMSAccess"
      effect = "Allow"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
        "kms:ListKeys",
        "kms:GenerateDataKey"
      ]
      resources = [data.aws_kms_key.kms_key.arn]
    }
  ]

  engine_security_group_id    = ""
  workspace_security_group_id = ""

  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids

  # SSO Mapping
  session_mappings = {
    admin_group = {
      identity_type = "GROUP"
      identity_id   = var.emr_studio_admin_group
    }
  }

}


module "emr_studio_backup_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = var.label_context
  attributes = ["bucket", "emrstudio", "backup", random_id.emr_bucket_suffix.id]
}

resource "random_id" "emr_bucket_suffix" {
  byte_length = 4
}

module "bucket_emr_studio_backup" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 3.0"

  bucket = module.emr_studio_backup_label.id

  attach_public_policy = true

  attach_deny_insecure_transport_policy = true
  force_destroy                         = true
  allowed_kms_key_arn                   = data.aws_kms_key.kms_key.arn

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = data.aws_kms_key.kms_key.arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "emr_studio_backup_bucket_lifecycle" {
  bucket = module.emr_studio_backup_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

    expiration {
      days = 365
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 31
    }
    transition {
      days          = 31
      storage_class = "INTELLIGENT_TIERING"
    }
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
    noncurrent_version_transition {
      noncurrent_days = 31
      storage_class   = "INTELLIGENT_TIERING"
    }
  }

}

resource "aws_s3_bucket_public_access_block" "emr_studio_bucket_public_access_block" {
  bucket                  = module.emr_studio_backup_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}