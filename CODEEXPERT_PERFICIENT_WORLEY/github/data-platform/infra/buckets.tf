locals {
  domains = ["engineering", "construction", "supply_chain", "finance", "people", "document_control", "project_control", "health_safety_quality", "dac"]
  people_domain = ["people", "non_people"]
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

################################################################################
# Landing Bucket
################################################################################

module "bucket_landing_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "landing", random_id.bucket_suffix.id]
}

module "bucket_landing" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_landing_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  # S3 Bucket Ownership Controls
  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_ownership_controls
  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  expected_bucket_owner = data.aws_caller_identity.current.account_id

  tags = module.bucket_landing_label.tags

  depends_on = [module.bucket_s3_access_logs]

}


resource "aws_s3_bucket_lifecycle_configuration" "landing_bucket_lifecycle" {
  bucket = module.bucket_landing_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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


resource "aws_s3_bucket_public_access_block" "landing_bucket_public_access_block" {
  bucket                  = module.bucket_landing_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# RAW Bucket
################################################################################

module "bucket_raw_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "raw", random_id.bucket_suffix.id]
}

module "bucket_raw" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_raw_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key_primary.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_policy                         = true
  policy                                = data.aws_iam_policy_document.s3_appflow.json
  attach_public_policy                  = true
  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  # attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key_primary.key_arn
  #attach_deny_unencrypted_object_uploads   = true
  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"
  tags                     = module.bucket_raw_label.tags
  depends_on               = [module.bucket_s3_access_logs]

}

resource "aws_s3_object" "raw_folders" {
  for_each = toset(local.domains)
  bucket   = module.bucket_raw.s3_bucket_id
  key      = "${each.value}/"
}

data "aws_iam_policy_document" "s3_appflow" {
  statement {
    resources = [module.bucket_raw.s3_bucket_arn, "${module.bucket_raw.s3_bucket_arn}/*"]
    sid       = "AppFlowAcl"
    actions = [
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:ListBucketMultipartUploads",
      "s3:GetBucketAcl",
      "s3:PutObjectAcl"
    ]
    principals {
      type        = "Service"
      identifiers = ["appflow.amazonaws.com"]
    }
  }
  statement {
    sid    = "denyIncorrectKmsKeySse"
    effect = "Deny"

    actions = [
      "s3:PutObject"
    ]

    resources = [
      module.bucket_raw.s3_bucket_arn, "${module.bucket_raw.s3_bucket_arn}/*"
    ]

    principals {
      identifiers = ["*"]
      type        = "*"
    }

    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [module.kms_key.key_arn, module.kms_key_primary.key_arn]
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_bucket_lifecycle" {
  bucket = module.bucket_raw_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "raw_bucket_public_access_block" {
  bucket                  = module.bucket_raw_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


################################################################################
# RAW Bucket for People Domain
################################################################################

module "bucket_raw_label_people" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "raw", "people", random_id.bucket_suffix.id]
}

module "bucket_raw_people_domain" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_raw_label_people.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "people_data_raw_log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_policy                         = true
  policy                                = data.aws_iam_policy_document.S3_datasync_kms_bucket_policy.json
  attach_public_policy                  = true
  # attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  # attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true
  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"
  tags                     = module.bucket_raw_label_people.tags
  depends_on               = [module.bucket_s3_access_logs]

}

data "aws_iam_policy_document" "S3_datasync_kms_bucket_policy" {
  statement {
    resources = [module.bucket_raw_people_domain.s3_bucket_arn, "${module.bucket_raw_people_domain.s3_bucket_arn}/*"]
    sid       = "DataSyncRole"
    actions = [
      "s3:Put*",
      "s3:List*",
      "s3:Get*",
      "s3:Delete*"
    ]
    principals {
      type        = "AWS"
      identifiers = [module.datasync_s3_iam_role.arn]
    }
  }
  statement {
    sid    = "denyIncorrectKmsKeySse"
    effect = "Deny"

    actions = [
      "s3:PutObject"
    ]

    resources = [
      module.bucket_raw_people_domain.s3_bucket_arn, "${module.bucket_raw_people_domain.s3_bucket_arn}/*"
    ]

    principals {
      identifiers = ["*"]
      type        = "*"
    }

    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [module.kms_key.key_arn, module.kms_key_primary.key_arn]
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_people_bucket_public_access_block" {
  bucket                  = module.bucket_raw_label_people.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "raw_folders_people_domain" {
  for_each = toset(local.people_domain)
  bucket   = module.bucket_raw_people_domain.s3_bucket_id
  key      = "${each.value}/"
}


###############################################################################
# Curated Bucket for People Domain
###############################################################################

module "bucket_curated_people_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "curated", "people", random_id.bucket_suffix.id]
}

module "bucket_curated_people_domain" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_curated_people_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "people_data_curated_log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_curated_people_label.tags

  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_object" "curated_folders_people_domain" {
  for_each = toset(local.people_domain)
  bucket   = module.bucket_curated_people_domain.s3_bucket_id
  key      = "${each.value}/"
}

resource "aws_s3_bucket_lifecycle_configuration" "curated_bucket_lifecycle_people_domain" {
  bucket = module.bucket_curated_people_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "curated_bucket_people_domain_public_access_block" {
  bucket                  = module.bucket_curated_people_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Transformed People Bucket
################################################################################

module "bucket_transformed_people_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "transformed", "people", random_id.bucket_suffix.id]
}

module "bucket_transformed_people_domain" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_transformed_people_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "people_data_transformed_log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_transformed_people_label.tags

  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_object" "transformed_folders_people_domain" {
  for_each = toset(local.people_domain)
  bucket   = module.bucket_transformed.s3_bucket_id
  key      = "${each.value}/"
}

resource "aws_s3_bucket_lifecycle_configuration" "transformed_bucket_people_domain_lifecycle" {
  bucket = module.bucket_transformed_people_domain.s3_bucket_id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "transformed_bucket_people_domain_public_access_block" {
  bucket                  = module.bucket_transformed_people_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


################################################################################
# RAW Sample Bucket
################################################################################

module "bucket_temp_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "temp", "data", random_id.bucket_suffix.id]
}

module "bucket_temp" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_temp_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_raw_label.tags

  depends_on = [module.bucket_s3_access_logs]

}

resource "aws_s3_bucket_lifecycle_configuration" "temp_bucket_lifecycle" {
  bucket = module.bucket_temp_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "temp_bucket_public_access_block" {
  bucket                  = module.bucket_temp_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


################################################################################
# Curated Bucket
################################################################################

module "bucket_curated_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "curated", random_id.bucket_suffix.id]
}

module "bucket_curated" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_curated_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_curated_label.tags

  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_object" "curated_folders" {
  for_each = toset(local.domains)
  bucket   = module.bucket_curated.s3_bucket_id
  key      = "${each.value}/"
}

resource "aws_s3_bucket_lifecycle_configuration" "curated_bucket_lifecycle" {
  bucket = module.bucket_curated_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "curated_bucket_public_access_block" {
  bucket                  = module.bucket_curated_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Quarantine Bucket
################################################################################

module "bucket_quarantine_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "quarantine", random_id.bucket_suffix.id]
}

module "bucket_quarantine" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_quarantine_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_quarantine_label.tags

  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_bucket_lifecycle_configuration" "quarantine_bucket_lifecycle" {
  bucket = module.bucket_quarantine_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "quarantine_bucket_public_access_block" {
  bucket                  = module.bucket_quarantine_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Glue Job Scripts Bucket
################################################################################

module "bucket_glue_job_scripts_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "glue", "job", "scripts", random_id.bucket_suffix.id]
}

module "bucket_glue_jobs_scripts" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_glue_job_scripts_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true
  attach_policy                         = true
  policy                                   = data.aws_iam_policy_document.script_bucket_deny_incorrect_kms_key_sse.json
  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  # attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  # allowed_kms_key_arn                      = local.kms_keys
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags       = module.bucket_glue_job_scripts_label.tags
  depends_on = [module.bucket_s3_access_logs]
}
data "aws_iam_policy_document" "script_bucket_deny_incorrect_kms_key_sse" {
  statement {
    sid    = "denyIncorrectKmsKeySse"
    effect = "Deny"

    actions = [
      "s3:PutObject"
    ]

    resources = [
      module.bucket_glue_jobs_scripts.s3_bucket_arn, "${module.bucket_glue_jobs_scripts.s3_bucket_arn}/*"
    ]

    principals {
      identifiers = ["*"]
      type        = "*"
    }

    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [module.kms_key.key_arn, module.kms_key_primary.key_arn]
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "glue_scripts_bucket_lifecycle" {
  bucket = module.bucket_glue_job_scripts_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "glue_jobs_bucket_public_access_block" {
  bucket                  = module.bucket_glue_job_scripts_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Glue Job Outputs Bucket
################################################################################

module "bucket_glue_job_outputs_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "glue", "job", "outputs", random_id.bucket_suffix.id]
}

module "bucket_glue_job_outputs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_glue_job_outputs_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags       = module.bucket_glue_job_outputs_label.tags
  depends_on = [module.bucket_s3_access_logs]
}


resource "aws_s3_bucket_lifecycle_configuration" "glue_output_bucket_lifecycle" {
  bucket = module.bucket_glue_job_outputs_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "glue_output_bucket_public_access_block" {
  bucket                  = module.bucket_glue_job_outputs_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
################################################################################
# Redshift
################################################################################
module "bucket_redshift_logs_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "redshift", "logs", random_id.bucket_suffix.id]
}

module "bucket_redshift_logs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 3.0"

  bucket = module.bucket_redshift_logs_label.id
  acl    = "log-delivery-write"

  control_object_ownership = true
  object_ownership         = "ObjectWriter"

  attach_public_policy = true

  attach_policy = true
  policy        = data.aws_iam_policy_document.s3_redshift.json

  attach_deny_insecure_transport_policy = true
  force_destroy                         = true

  tags = module.bucket_redshift_logs_label.tags
}

data "aws_iam_policy_document" "s3_redshift" {
  statement {
    sid       = "RedshiftAcl"
    actions   = ["s3:GetBucketAcl"]
    resources = [module.bucket_redshift_logs.s3_bucket_arn]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }

  statement {
    sid       = "RedshiftWrite"
    actions   = ["s3:PutObject"]
    resources = ["${module.bucket_redshift_logs.s3_bucket_arn}/redshift/*"]
    condition {
      test     = "StringEquals"
      values   = ["bucket-owner-full-control"]
      variable = "s3:x-amz-acl"
    }

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "redshift_logs_bucket_lifecycle" {
  bucket = module.bucket_redshift_logs_label.id

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

resource "aws_s3_bucket_public_access_block" "redshift_logs_bucket_public_access_block" {
  bucket                  = module.bucket_redshift_logs_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Athena results Outputs Bucket
################################################################################

module "bucket_athena_results_outputs_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "Athena", "results", "outputs", random_id.bucket_suffix.id]
}

module "bucket_athena_results_outputs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_athena_results_outputs_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags       = module.bucket_athena_results_outputs_label.tags
  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results_bucket_lifecycle" {
  bucket = module.bucket_athena_results_outputs_label.id

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

resource "aws_s3_bucket_public_access_block" "athena_results_bucket_public_access_block" {
  bucket                  = module.bucket_athena_results_outputs_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Datasync Bucket
################################################################################

module "bucket_datasync_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "datasync", random_id.bucket_suffix.id]
}

module "bucket_datasync" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_datasync_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }
  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy                     = true
  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse        = true
  allowed_kms_key_arn                      = module.kms_key.key_arn
  attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags       = module.bucket_datasync_label.tags
  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_bucket_lifecycle_configuration" "datasync_bucket_lifecycle" {
  bucket = module.bucket_datasync_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "datasync_bucket_public_access_block" {
  bucket                  = module.bucket_datasync_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# GenAI Landing Bucket - A dedicated bucket for GenAI Sharepoint and Dataverse
################################################################################
module "genai_bucket_landing_label" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["genai", "landing", random_id.bucket_suffix.id]
}

module "genai_bucket_landing" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.genai_bucket_landing_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  versioning = {
    status = true
  }

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true
  # *** <<< - Uncomment below to lockdown S3 bucket to GenAI Roles - >>>
  # attach_policy = true
  # policy        = data.aws_iam_policy_document.s3_policy_genai.json

  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true

  # S3 Bucket Ownership Controls
  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_ownership_controls
  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  expected_bucket_owner = data.aws_caller_identity.current.account_id

  tags       = module.genai_bucket_landing_label.tags
  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_bucket_lifecycle_configuration" "gen_ai_bucket_lifecycle" {
  bucket = module.genai_bucket_landing_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "genai_bucket_public_access_block" {
  bucket                  = module.genai_bucket_landing_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# *** <<< - Uncomment below to lockdown S3 bucket to GenAI Roles - >>>
# data "aws_iam_role" "tfc_admin_role" {
#   name = var.tfc_admin_role
# }

# output "TFC_Role" {
#   value = data.aws_iam_role.tfc_admin_role.unique_id
#   description = "The ID of the 'tfc_admin_role' role"
# }

# data "aws_iam_policy_document" "s3_policy_genai" {
#   statement {
#     sid       = "genaiallowedusers"
#     actions = ["s3:*"]
#     effect = "Deny"
#     principals {
#       type        = "*"
#       identifiers = ["*"]
#     }
#     resources = [module.genai_bucket_landing.s3_bucket_arn, "${module.genai_bucket_landing.s3_bucket_arn}/*"]
#     condition {
#       test     = "StringNotLike"
#       variable = "aws:userId"
#       values   = [
# "${data.aws_caller_identity.current.user_id}",
# "${data.aws_iam_role.tfc_admin_role.unique_id}:*", 
# "${module.genai_sharepoint_glue_service_iam_role.id}:*", 
# "${module.genai_sfdc_glue_service_iam_role.id}:*",
# "${module.genai_s3_bucket_access_iam_role.id}:*",
# "${module.genai_dataverse_glue_service_iam_role.id}:*", 
# "${data.aws_caller_identity.current.account_id}"]
#    }
#   }
# }
################################################################################
# RAW US OHIO Bucket
################################################################################

module "ohio_bucket_raw_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.ohio_label.context
  attributes = ["bucket", "raw", random_id.bucket_suffix.id]
}

module "ohio_bucket_raw" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  providers = {
    aws = aws.us-east-2
  }

  bucket        = module.ohio_bucket_raw_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key_replica.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_policy                         = true
  attach_public_policy                  = true
  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key_replica.key_arn
  #attach_deny_unencrypted_object_uploads   = true
  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"
  tags                     = module.ohio_bucket_raw_label.tags

}


resource "aws_s3_bucket_lifecycle_configuration" "ohio_raw_bucket_lifecycle" {
  bucket = module.ohio_bucket_raw_label.id

  provider = aws.us-east-2

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "ohio_raw_bucket_public_access_block" {
  bucket = module.ohio_bucket_raw_label.id

  provider = aws.us-east-2

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# S3 Accss Log Bucket
################################################################################

# © 2024 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
# logging AWS Content is provided subject to the terms of the AWS Customer Agreement available at
# http://aws.amazon.com/agreement or other written agreement between Customer and either
# Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.


module "bucket_s3_access_logs_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["s3", "access", "logs", random_id.bucket_suffix.id]
}

module "bucket_s3_access_logs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_s3_access_logs_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  attach_access_log_delivery_policy = true

  attach_public_policy                     = true
  attach_deny_insecure_transport_policy    = true
  attach_require_latest_tls_policy         = true
  attach_deny_incorrect_encryption_headers = true


  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_s3_access_logs_label.tags
}

resource "aws_s3_bucket_lifecycle_configuration" "s3_access_logs_bucket_lifecycle" {
  bucket = module.bucket_s3_access_logs_label.id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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
    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_public_access_block" "s3_access_logs_public_access_block" {
  bucket                  = module.bucket_s3_access_logs_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Transformed/DIM Bucket
################################################################################

module "bucket_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["bucket", "transformed", random_id.bucket_suffix.id]
}

module "bucket_transformed" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.1.2"

  bucket        = module.bucket_transformed_label.id
  force_destroy = var.environment != "prd" ? true : false # only enabled if non-prod

  logging = {
    target_bucket = module.bucket_s3_access_logs.s3_bucket_id
    target_prefix = "log/"
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  attach_public_policy = true

  attach_deny_insecure_transport_policy = true
  #attach_require_latest_tls_policy         = true
  #attach_deny_incorrect_encryption_headers = true
  attach_deny_incorrect_kms_key_sse = true
  allowed_kms_key_arn               = module.kms_key.key_arn
  #attach_deny_unencrypted_object_uploads   = true

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  tags = module.bucket_curated_label.tags

  depends_on = [module.bucket_s3_access_logs]
}

resource "aws_s3_object" "transformed_folders" {
  for_each = toset(local.domains)
  bucket   = module.bucket_transformed.s3_bucket_id
  key      = "${each.value}/"
}

resource "aws_s3_bucket_lifecycle_configuration" "transformed_bucket_lifecycle" {
  bucket = module.bucket_transformed.s3_bucket_id

  rule {
    id     = "SwitchToIntelligentTiering"
    status = "Enabled"

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

resource "aws_s3_bucket_public_access_block" "transformed_bucket_public_access_block" {
  bucket                  = module.bucket_transformed_label.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
