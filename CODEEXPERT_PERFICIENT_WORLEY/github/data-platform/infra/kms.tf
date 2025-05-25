module "kms_key_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["kms", "key"]
}

module "kms_key" {
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"

  description             = "Encryption for Landing S3 Bucket"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  is_enabled              = true
  key_usage               = "ENCRYPT_DECRYPT"
  multi_region            = false # BACKLOG: would be useful to enable it in case, other landing buckets were deployed into different accounts, or even in different regions

  # Policy
  enable_default_policy = true
  # key_owners            = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]

  # Aliases
  aliases                 = [module.kms_key_label.id] # accepts static strings only
  aliases_use_name_prefix = true

  key_statements = [
    {
      sid = "Allow EMR"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey"
      ]
      principals = [
        {
          type        = "Service",
          identifiers = ["elasticmapreduce.amazonaws.com"]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "allow IDC role access"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
      ]
      principals = [
        {
          type        = "AWS",
          identifiers = [module.redshit_idc_integrations_iam_role.arn]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "Allow QuickSight role access"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
      ]
      principals = [
        {
          type        = "AWS",
          identifiers = [data.aws_iam_role.quicksight_service_role.arn]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "AllowS3Encryption"
      actions = [
        "kms:GenerateDataKey",
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["s3.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowGlueEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["glue.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowDataSyncKMSS3bucketAccess"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "AWS"
          identifiers = [module.datasync_s3_iam_role.arn]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowAthenaEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["athena.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },

    {
      sid = "AllowCloudWatchLogs"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type = "Service"
          identifiers = ["logs.ap-southeast-2.amazonaws.com",
          "logs.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },

    {
      sid = "Allow Iceberg Optimizer role access"
      actions = [
        "kms:Encrypt",
        "kms:GenerateDataKey"
      ]
      principals = [
        {
          type        = "AWS",
          identifiers = [module.iceberg_optimizer_iam_role.arn]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    }
  ]

  tags = module.kms_key_label.tags
}

module "kms_redshift_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["kms", "outputs", "bucket"]
}

module "kms_redshift" {
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"

  description             = "Encryption for Quarantine S3 Bucket"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  is_enabled              = true
  key_usage               = "ENCRYPT_DECRYPT"
  multi_region            = false


  # Policy
  enable_default_policy = true
  key_statements = [
    {
      sid = "AllowRedshiftEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["redshift.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    }
  ]

  aliases                 = [module.kms_redshift_label.id]
  aliases_use_name_prefix = true

  tags = module.kms_redshift_label.tags
}

module "kms_appflow_label" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["kms", "key", "appflow"]
}

module "kms_appflow" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"

  description         = "Encryption for AWS AppFlow"
  enable_key_rotation = true
  is_enabled          = true
  key_usage           = "ENCRYPT_DECRYPT"
  multi_region        = false


  # Policy
  enable_default_policy = true
  key_statements = [
    {
      sid = "AllowAppFlowEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["appflow.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    }
  ]

  aliases                 = [module.kms_appflow_label.id]
  aliases_use_name_prefix = true

  tags = module.kms_appflow_label.tags
}

################################################################################
# KMS Keys for Aurora Postgres
################################################################################

module "aurora_kms_key_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["genai", "aurora", "kms", "key"]
}

module "aurora_kms_key" {
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"

  description             = "Encryption for GenAI Aurora DB"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  is_enabled              = true
  key_usage               = "ENCRYPT_DECRYPT"
  multi_region            = false # BACKLOG: would be useful to enable it in case, other landing buckets were deployed into different accounts, or even in different regions

  # Policy
  enable_default_policy = true
  # key_owners            = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]

  # Aliases
  aliases                 = [module.aurora_kms_key_label.id] # accepts static strings only
  aliases_use_name_prefix = true

  key_statements = [
    {
      sid = "AllowRDSEncryption"
      actions = [
        "kms:GenerateDataKey",
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["rds.amazonaws.com"]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id
          ]
        },
      ]
    },
    {
      sid = "AllowGlueEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["glue.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    }
  ]

  tags = module.kms_key_label.tags
}


################################################################################
# Multi region KMS Keys 
################################################################################
module "multiregion_kms_key_label_primary" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["kms", "key", "primary"]
}

module "kms_key_primary" {
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"


  description             = "Encryption for Landing S3 Bucket - Primary"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  is_enabled              = true
  key_usage               = "ENCRYPT_DECRYPT"
  multi_region            = true

  # Policy
  enable_default_policy = true

  # Aliases
  aliases                 = [module.multiregion_kms_key_label_primary.id] # accepts static strings only
  aliases_use_name_prefix = true

  key_statements = [
    {
      sid = "Allow EMR"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey"
      ]
      principals = [
        {
          type        = "Service",
          identifiers = ["elasticmapreduce.amazonaws.com"]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "allow IDC and Glue role access"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
      ]
      principals = [
        {
          type        = "AWS",
          identifiers = [module.redshit_idc_integrations_iam_role.arn, module.glue_service_iam_role.arn]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "AllowS3Encryption"
      actions = [
        "kms:GenerateDataKey",
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["s3.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowGlueEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["glue.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowAthenaEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["athena.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowCloudWatchLogs"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["logs.ap-southeast-2.amazonaws.com", "logs.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowAttachmentOfPersistentResources"
      actions = [
        "kms:CreateGrant",
        "kms:ListGrants",
        "kms:RevokeGrant"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "AWS"
          identifiers = [module.glue_service_iam_role.arn]
        }
      ]

      conditions = [
        {
          test     = "Bool"
          variable = "kms:GrantIsForAWSResource"
          values   = ["true"]
        },
      ]
    }
  ]

  tags = module.multiregion_kms_key_label_primary.tags
}

module "multiregion_kms_key_label_replica" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["kms", "key", "replica"]
}


module "kms_key_replica" {
  source  = "terraform-aws-modules/kms/aws"
  version = "2.2.1"

  providers = {
    aws = aws.us-east-2
  }

  description             = "Encryption for Landing S3 Bucket - Replica"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  is_enabled              = true
  create_replica          = true
  key_usage               = "ENCRYPT_DECRYPT"
  multi_region            = true
  primary_key_arn         = module.kms_key_primary.key_arn

  # Policy
  enable_default_policy = true

  # Aliases
  aliases                 = [module.multiregion_kms_key_label_replica.id] # accepts static strings only
  aliases_use_name_prefix = true

  key_statements = [
    {
      sid = "Allow EMR"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey"
      ]
      principals = [
        {
          type        = "Service",
          identifiers = ["elasticmapreduce.amazonaws.com"]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "allow IDC role access"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:ReEncrypt*",
      ]
      principals = [
        {
          type        = "AWS",
          identifiers = [module.redshit_idc_integrations_iam_role.arn]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        }
      ]
    },
    {
      sid = "AllowS3Encryption"
      actions = [
        "kms:GenerateDataKey",
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["s3.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowGlueEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["glue.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowAthenaEncryption"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["athena.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    },
    {
      sid = "AllowCloudWatchLogs"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["logs.us-east-2.amazonaws.com", "logs.amazonaws.com"]
        }
      ]

      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            data.aws_caller_identity.current.id,
          ]
        },
      ]
    }
  ]

  tags = module.multiregion_kms_key_label_replica.tags
}

