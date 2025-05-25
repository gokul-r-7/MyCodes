module "glue_service_iam_role" {
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com",
                  "lakeformation.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  policy_document_count = 2
  policy_documents      = [
                data.aws_iam_policy_document.glue_service_policy.json,
                data.aws_iam_policy_document.dbt_glue_role_policy.json
                          ]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["iam", "service", "role", "glue"]
  context    = module.label.context
}

############### Iam Role for Notebook scripts #####################

data "aws_iam_policy_document" "glue_service_policy_assumerole" {
  # First Statement: Allow PassRole for AWSGlueServiceRole with a condition for glue.amazonaws.com
  statement {
    actions = ["iam:PassRole"]
    effect  = "Allow"
    resources = [
      "arn:aws:iam::*:role/AWSGlueServiceRole*"
    ]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }

  # Second Statement: Allow PassRole for AWSGlueServiceNotebookRole with a condition for ec2.amazonaws.com
  statement {
    actions = ["iam:PassRole"]
    effect  = "Allow"
    resources = [
      "arn:aws:iam::*:role/AWSGlueServiceNotebookRole*"
    ]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }

  # Third Statement: Allow PassRole for AWSGlueServiceRole with a condition for glue.amazonaws.com
  statement {
    actions = ["iam:PassRole"]
    effect  = "Allow"
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/worley-datalake-sydney-${var.environment}-iam-service-role-glue-notebook"
    ]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }
}


module "glue_notebook_service_iam_role" {
  count = var.environment == "dev" ? 1 : 0
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com",
                  "lakeformation.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  policy_document_count = 3
  policy_documents      = [
                data.aws_iam_policy_document.glue_service_policy.json,
                data.aws_iam_policy_document.dbt_glue_role_policy.json,
                data.aws_iam_policy_document.glue_service_policy_assumerole.json
                          ]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["iam", "service", "role", "glue" , "notebook"]
  context    = module.label.context
}

module "redshit_idc_integrations_iam_role" {

  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["redshift.amazonaws.com"]
  }

  assume_role_actions = ["sts:AssumeRole","sts:SetContext"]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.redshift_idc_policy.json]

  policy_description = "Policy for Redshift to create and manage the AWSIDC application"
  role_description   = "Policy for Redshift to create and manage the AWSIDC application"

  attributes = ["iam", "service", "role", "redshift", "awsidc"]
  context    = module.label.context
}

#checkov:skip=CKV_AWS_290:IAM policies allow write access without constraints
#checkov:skip=CKV_AWS_355:IAM policy document allows all resources with restricted actions
data "aws_iam_policy_document" "glue_service_policy" {
  statement {
    sid       = "DynamoDBTableAccess"
    actions   = ["dynamodb:Query"]
    resources = [module.dynamodb_landing_table.dynamodb_table_arn,
                 module.dynamodb_project_mapping_table.dynamodb_table_arn]
  }

  statement {
    sid       = "DynamoDBTableMetadataFrameworkAccess"
    actions   = ["dynamodb:Query"]
    resources = ["arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${module.metadata_framework_dynamodb_table_label.id}"]
  }

  statement {
    sid       = "DynamoDBTableDatabasePermissionsMetadataAccess"
    actions   = ["dynamodb:Query"]
    resources = ["arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/worley-mf-rbac-${var.stage}-${var.environment}-database-permissions-metadata"]
  }

  statement {
    sid = "AllowS3Access"
    actions = ["s3:Get*",
      "s3:List*",
      "s3:PutObject",
    "s3:DeleteObject"]
    resources = ["arn:aws:s3:::worley*"]

  }

  statement {
    sid       = "AllowLakeFormationAccess"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]

  }

  statement {
    sid = "AllowKMSAccess"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    # resources = [module.kms_key.key_arn]
    resources = [module.kms_key.key_arn, module.kms_key_primary.key_arn, module.kms_key_replica.key_arn]
  }

  statement {
    sid       = "GetSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:*"]
  }

  statement {
    sid = "GetSSMParameter"
    actions = [
      "ssm:DescribeParameters",
      "ssm:GetParameterHistory",
      "ssm:GetParametersByPath",
      "ssm:GetParameters",
      "ssm:GetParameter"
    ]
    resources = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/*"]
  }

  statement {
    sid = "SNSTopicAccess"
    actions = [
      "sns:GetTopic",
      "sns:ListTopics",
      "sns:Publish"
    ]
    resources = ["${module.sns_topic_data_platform_notification.sns_topic_arn}"]
  }

  statement {
    sid     = "AllowKMSAssociate"
    actions = ["logs:AssociateKmsKey"]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]

  }

}

#checkov:skip=CKV_AWS_290:IAM policies allow write access without constraints
#checkov:skip=CKV_AWS_355:IAM policy document allows all resources with restricted actions
data "aws_iam_policy_document" "redshift_idc_policy" {

  statement {
    sid = "RedshiftAccess"
    actions = [
      "redshift:DescribeQev2IdcApplications",
      "redshift-serverless:ListNamespaces",
      "redshift-serverless:ListWorkgroups",
      "redshift-serverless:GetWorkgroup"
    ]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]
  }

  statement {
    sid = "AllowSSOAccess"
    actions = ["sso:DescribeApplication",
      "sso:DescribeInstance",
      "sso:CreateApplication",
      "sso:DeleteApplicationAccessScope",
      "sso:DeleteApplicationGrant",
      "sso:GetApplicationGrant",
      "sso:ListApplicationAccessScopes",
      "sso:PutApplicationAccessScope",
      "sso:PutApplicationAssignmentConfiguration",
      "sso:PutApplicationAuthenticationMethod",
      "sso:PutApplicationGrant",
      "sso:UpdateApplication"
    ]
    resources = ["*"]
  }

  statement {
    sid = "Allows3access"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]
    resources = [
      "${module.bucket_raw.s3_bucket_arn}",
      "${module.bucket_raw.s3_bucket_arn}/*",
      "${module.bucket_curated.s3_bucket_arn}",
      "${module.bucket_curated.s3_bucket_arn}/*",
      "${data.aws_s3_bucket.external_user_store.arn}",
      "${data.aws_s3_bucket.external_user_store.arn}/*",
      "${module.bucket_transformed.s3_bucket_arn}",
      "${module.bucket_transformed.s3_bucket_arn}/*",

    ]
  }

  statement {
    sid = "AllowGlueAccess"
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions"
    ]
    resources = ["*"]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
    ]
    resources = [module.kms_key.key_arn]
  }
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# MWAA

data "aws_iam_policy_document" "mwaa_secrets_manager_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:worley-datalake-${var.stage}-${var.environment}-dbt-*"]
  }
}

resource "aws_iam_policy" "mwaa_secrets_manager_policy" {
  name   = "secrets-manager-policy"
  policy = data.aws_iam_policy_document.mwaa_secrets_manager_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "mwaa_secrets_manager_policy_attachment" {
  role       = split("/", module.mwaa.execution_role_arn)[1]
  policy_arn = aws_iam_policy.mwaa_secrets_manager_policy.arn
}

# QuickSight

data "aws_iam_policy_document" "quicksight_role_kms_policy_doc" {
  statement {
    sid    = "AllowKMSAccess"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }
}

resource "aws_iam_policy" "quicksight_role_kms_policy" {
  name   = "quicksight-kms-policy"
  policy = data.aws_iam_policy_document.quicksight_role_kms_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "quicksight_role_kms_policy_attachment" {
  role       = data.aws_iam_role.quicksight_service_role.id
  policy_arn = aws_iam_policy.quicksight_role_kms_policy.arn
}

################################################################################
# GenAI Glue Policy
################################################################################
data "aws_iam_policy_document" "genai_glue_service_policy" {
  statement {
    sid       = "DynamoDBTableAccess"
    actions   = ["dynamodb:Query"]
    resources = [module.dynamodb_landing_table.dynamodb_table_arn]
  }

  statement {
    sid       = "DynamoDBTableMetadataFrameworkAccess"
    actions   = ["dynamodb:Query"]
    resources = ["arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${module.metadata_framework_dynamodb_table_label.id}"]
  }

  statement {
    sid     = "AllowS3Access"
    actions = ["s3:*"]
    resources = [
      "arn:aws:s3:::${module.genai_bucket_landing_label.id}",
      "arn:aws:s3:::${module.genai_bucket_landing_label.id}/*"
    ]
  }

  statement {
    sid = "AllowDataLakeS3Access"
    actions = ["s3:Get*",
      "s3:List*",
      "s3:PutObject",
    "s3:DeleteObject"]
    resources = ["arn:aws:s3:::worley*"]

  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }

  statement {
    sid       = "GetSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:*"]
  }

  statement {
    sid       = "AllowLakeFormationAccess"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]

  }

  statement {
    sid = "GetSSMParameter"
    actions = [
      "ssm:DescribeParameters",
      "ssm:GetParameterHistory",
      "ssm:GetParametersByPath",
      "ssm:GetParameters",
      "ssm:GetParameter"
    ]
    resources = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/*"]
  }

  statement {
    sid     = "AllowKMSAssociate"
    actions = ["logs:AssociateKmsKey"]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]

  }

}

################################################################################
# GenAI CSP Salesforce Glue Role
################################################################################
#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "genai_sfdc_glue_service_iam_role" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.genai_glue_service_policy.json]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["sfdc", "iam", "service", "role", "glue"]
  context    = module.label.context
}

################################################################################
# GenAI Dataverse Glue Policy
################################################################################
#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "genai_dataverse_glue_service_iam_role" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.genai_glue_service_policy.json]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["dataverse", "iam", "service", "role", "glue"]
  context    = module.label.context
}

################################################################################
# GenAI Sharepoint Glue Role
################################################################################
#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "genai_sharepoint_glue_service_iam_role" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.genai_glue_service_policy.json]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["sharepoint", "iam", "service", "role", "glue"]
  context    = module.label.context
}


################################################################################
# Aurora Role
################################################################################
module "rds_aurora_service_iam_role" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["rds.amazonaws.com"]
  }

  # managed_policy_arns = [
  #   "arn:aws:iam::aws:policy/service-role/AmazonRDSServiceRolePolicy"
  # ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.genai_rds_aurora_service_policy.json]

  policy_description = "Policy for AWS Glue with access to EC2, S3, and Cloudwatch Logs"
  role_description   = "Role for AWS Glue with access to EC2, S3, and Cloudwatch Logs"

  attributes = ["genai", "iam", "service", "role", "rds"]
  context    = module.label.context
}

data "aws_iam_policy_document" "genai_rds_aurora_service_policy" {
  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
    ]
    resources = [module.kms_key.key_arn]
  }
}


################################################################################
# GenAI S3 bucket Access Role from Azure Applications using AssumeRoleWithSAML #
################################################################################
module "genai_s3_bucket_access_iam_role" {
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    Federated = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/sts.windows.net/73ad6539-b4fe-429c-97b6-fbc1b6ada80b/"]
  }

  assume_role_actions = [
    "sts:AssumeRoleWithWebIdentity"
  ]

  assume_role_conditions = [
    {
      test     = "StringEquals"
      variable = "sts.windows.net/73ad6539-b4fe-429c-97b6-fbc1b6ada80b/:aud"
      #values   = ["api://97b15505-eec9-4146-a0bc-cba4ce8e8c61"]
      values = [local.genai_azure_app_api_key_value]
    }
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.genai_s3_bucket_access_policy.json]

  policy_description = "Policy for GenAI applications to access data from AWS S3 bucket"
  role_description   = "Role for GenAI applications to access data from AWS S3 bucket"

  attributes = ["genai", "iam", "s3", "access", "role"]
  context    = module.label.context
}

data "aws_iam_policy_document" "genai_s3_bucket_access_policy" {

  statement {
    sid     = "AllowS3Access"
    actions = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"]
    resources = [
      "arn:aws:s3:::${module.genai_bucket_landing_label.id}",
      "arn:aws:s3:::${module.genai_bucket_landing_label.id}/*"
    ]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
    ]
    resources = [module.kms_key.key_arn]
  }

}
################################################################################
# Datasync S3 raw bucket Access Role
################################################################################
data "aws_iam_policy_document" "datasync_s3_raw_bucket_access_policy" {

  statement {
    sid = "Allows3access"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = [
      "${module.bucket_raw.s3_bucket_arn}",
      "${module.bucket_raw.s3_bucket_arn}/*",
    ]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncryptFrom",
      "kms:ReEncryptTo"
    ]
    resources = [module.kms_key.key_arn]
  }
}


################################################################################
# Lambda IAM Role for Cross account SysInt
################################################################################


module "lambda_iam_role" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["lambda.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.lambda_policy.json]

  policy_description = "Policy for AWS Lambda with access to resources and cross account connection to SysInt"
  role_description   = "Role for AWS Lambda with access to resources and cross account connection to SysInt"

  attributes = ["iam", "lambda", "service", "role"]
  context    = module.label.context
}


data "aws_iam_policy_document" "lambda_policy" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:UpdateItem",
      "dynamodb:PutItem"
    ]
    resources = [
      "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.stage}-int-mf-${var.environment}-metadata-table"
    ]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }

  statement {
    sid = "AllowDataLakeS3Access"
    actions = [
      "s3:Get*",
      "s3:List*"
    ]
    resources = ["arn:aws:s3:::worley*"]

  }

  statement {
    sid     = "AllowS3Access"
    actions = ["s3:*"]
    resources = [
      "arn:aws:s3:::${module.bucket_landing_label.id}",
      "arn:aws:s3:::${module.bucket_landing_label.id}/*"
    ]
  }

  statement {
    sid       = "AthenaQueryExecutionAccess"
    actions   = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution", 
      "athena:GetQueryResults"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "AllowLakeFormationAccess"
    actions   = [
      "lakeformation:GetDataAccess",
      "lakeformation:GetLFTag",
      "lakeformation:GetResourceLFTags",
      "lakeformation:GrantPermissions",
      "lakeformation:ListDataCellsFilter",
      "lakeformation:ListLFTags",
      "lakeformation:ListPermissions"
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*"
    ]

  }

  statement {
    sid    = "AlloweventbridgePutEvents"
    effect = "Allow"
    actions = [
      "events:PutEvents"
    ]
    resources = [
      aws_cloudwatch_event_bus.data_platform.arn
    ]
  }

  statement {
    sid    = "AllowToAssumeDataPlatformRole"
    effect = "Allow"
    actions = [
      "sts:AssumeRole"
    ]
    resources = [
      "arn:aws:iam::${var.sys_int_account_id}:role/apse2-dp-cross-account-iam-role-eip-${var.environment}"
    ]
  }
}


################################################################################
# Iceberg Optimization Enabler Lambda IAM Policy
################################################################################



resource "aws_iam_policy" "iceberg_optimizer_enabler_lambda_policy" {
  name        = "iceberg_optimizer_enabler_lambda_policy"
  path        = "/"
  description = "glue resources access for lambda"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = data.aws_iam_policy_document.glue_resources_policy.json
}

data "aws_iam_policy_document" "glue_resources_policy" {

  statement {
    sid     = "DescribeGlueResources"
    actions = [
              "glue:GetDatabases",
              "glue:GetTables",
              "glue:BatchGetTableOptimizer",
              "glue:CreateTableOptimizer",
              "glue:GetTable",
              "glue:GetConnection"
            ]
          
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:connection/*"
    ]
  }
  statement {
    sid   = "PassOptimizerRole"
    actions = [
              "iam:PassRole"
            ]
    resources = [
      "${module.iceberg_optimizer_iam_role.arn}"
    ]
  }
  statement{
    sid   = "PassConnection"
    actions = [
              "glue:PassConnection"
            ]
    resources = [
      for connection_name in module.glue_connection[*].name : 
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:connection/${connection_name}"
    ]

  }
}

################################################################################
# Iceberg Optimizer Role
################################################################################

data "aws_iam_policy_document" "optimizer_role_policy" {

  statement {
    sid     = "CuratedBucketAccess"
    actions = [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ]
          
    resources = [
        "${module.bucket_curated.s3_bucket_arn}/*",
          module.bucket_curated.s3_bucket_arn

    ]
  }
  statement {
    sid     = "GlueTableAccess"
    actions = [
              "glue:UpdateTable",
               "glue:GetTable"
            ]
          
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*"
    ]

  }
  statement{
     sid     = "CWLogAccess"
     actions = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
    resources = [
        "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/iceberg-compaction/logs:*",
        "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/iceberg-retention/logs:*",
        "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/iceberg-orphan-file-deletion/logs:*"
      ]
  }
  statement{
     sid     = "LakeFormationAccess"
     actions = [
        "lakeformation:GetDataAccess"
      ]
    resources = [
      "*"
    ]
  }
  statement{
    sid     = "EC2Access"
    actions = [
        "ec2:Describe*",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:*Tags"
    ]
    resources = [
      "*"
    ]
  }
}


module "iceberg_optimizer_iam_role" {
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["glue.amazonaws.com"]
  }

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.optimizer_role_policy.json]

  policy_description = "Policy for Iceberg Optimizer Role"
  role_description   = "Role for Iceberg Optimizer"

  attributes = ["iam", "iceberg", "optimizer", "role"]
  context    = module.label.context
}

################################################################################
# DBT Glue Role
################################################################################\

data "aws_iam_policy_document" "dbt_glue_role_policy" {

  statement {
    sid     = "ReadAndWriteDatabases"
    actions = [
                "glue:SearchTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartitionIndex",
                "glue:DeleteDatabase",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:DeleteTableVersion",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:DeletePartitionIndex",
                "glue:GetTableVersion",
                "glue:UpdateColumnStatisticsForTable",
                "glue:CreatePartition",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:UpdateColumnStatisticsForPartition",
                "glue:CreateDatabase",
                "glue:BatchDeleteTableVersion",
                "glue:BatchDeleteTable",
                "glue:DeletePartition",
                "glue:GetUserDefinedFunctions",
                "lakeformation:ListResources",
                "lakeformation:BatchGrantPermissions",
                "lakeformation:ListPermissions", 
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions",
                "lakeformation:RevokePermissions",
                "lakeformation:BatchRevokePermissions",
                "lakeformation:AddLFTagsToResource",
                "lakeformation:RemoveLFTagsFromResource",
                "lakeformation:GetResourceLFTags",
                "lakeformation:ListLFTags",
                "lakeformation:GetLFTag",
                "glue:CancelDataQualityRulesetEvaluationRun",
                "glue:GetDataQualityRulesetEvaluationRun",
                "glue:ListDataQualityRulesetEvaluationRuns",
                "glue:StartDataQualityRulesetEvaluationRun",
                "glue:GetDataQualityResult",
                "glue:ListDataQualityResults"

            ]
          
    resources = [
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*"
    ]
  }
  statement {
    sid     = "ReadOnlyDatabases"
    actions = [
                "glue:SearchTables",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:GetTableVersion",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "lakeformation:ListResources",
                "lakeformation:ListPermissions"
            ]
          
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
    ]

  }
  statement{
     sid     = "StorageAllBuckets"
     actions = [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ]
    resources = [
      "${module.bucket_curated.s3_bucket_arn}",
      "${module.bucket_transformed.s3_bucket_arn}",
      ]
  }
  statement{
     sid     = "ReadAndWriteBuckets"
     actions = [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:DeleteObject"
      ]
    resources = [
      "${module.bucket_transformed.s3_bucket_arn}"
    ]
  }
  statement{
    sid     = "ReadOnlyBuckets"
    actions = [
        "s3:GetObject"
    ]
    resources = [
      "${module.bucket_curated.s3_bucket_arn}"
    ]
  }
   statement{
    sid     = "AllowStatementInASessionToAUser"
    actions = [
        "glue:ListSessions",
        "glue:GetSession",
        "glue:ListStatements",
        "glue:GetStatement",
        "glue:RunStatement",
        "glue:CancelStatement",
        "glue:DeleteSession"
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:session/*"
    ]
  }
  # statement{
  #   sid     = "SessionPermission"
  #   actions = [
  #       "glue:CreateSession"
  #   ]
  #   resources = [
  #     "*"
  #   ]
  # }
}



# module "glue_dbt_iam_role" {
#   source  = "cloudposse/iam-role/aws"
#   version = "0.19.0"

#   principals = {
#     "Service" = ["glue.amazonaws.com",
#                 "lakeformation.amazonaws.com"],
#     "AWS" = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${module.glue_service_iam_role.name}"]
#   }

#   policy_document_count = 1
#   policy_documents      = [data.aws_iam_policy_document.dbt_glue_role_policy.json]

#   policy_description = "Policy for Glue DBT Role"
#   role_description   = "Role for Glue DBT Role"

#   attributes = ["iam", "glue", "dbt", "role"]
#   context    = module.label.context
# }

################################################################################
# Circuit Breaker IAM Role for QuickSight
################################################################################

module "quicksight_service_iam_role" {
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["quicksight.amazonaws.com", "ssm.amazonaws.com", "ec2.amazonaws.com"]
  }
  instance_profile_enabled = true 

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  ]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.quicksight_service_policy.json]

  policy_description = "Policy for QuickSight full access and Session Manager"
  role_description   = "Role for QuickSight full access and Session Manager"

  attributes = ["quicksight", "iam", "service", "role"]
  context    = module.label.context
}

data "aws_iam_policy_document" "quicksight_service_policy" {
  statement {
    sid = "QuickSightFullAccess"
    actions = [
      "quicksight:*"
    ]
    resources = ["*"]
  }


  statement {
    sid = "IAMPassRole"
    actions = [
      "iam:PassRole"
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["quicksight.amazonaws.com", "ssm.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

################################################################################
# Collibra Cross Account IAM Role
################################################################################

################################################################################
# IAM Role
################################################################################
module "cross_account_collibra_role" {
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  # principals = {
    # "AWS" = ["arn:aws:iam::710271938750:role/collibra-edge-20250124095022865000000001"]  As per Snehal this is role doesnt exist, so going with empty role now until i get new role provided
    # "AWS" = [""]
  # }

  principals = {
    "Service" = ["eks.amazonaws.com"]
  }

  assume_role_actions = ["sts:AssumeRole"]

  policy_document_count = 3
  policy_documents      = [
    data.aws_iam_policy_document.collibra_glue_policy.json,
    data.aws_iam_policy_document.redshift_metadata_permissions.json,
    data.aws_iam_policy_document.collibra_s3_athena_access_policy.json
  ]

  policy_description = "Policy for Lake Formation, S3, and Redshift catalog access"
  role_description   = "Cross-account role for Lake Formation, S3, and Redshift access"

  attributes = ["iam", "cross", "account", "lake", "formation", "role"]
  context    = module.label.context
}

################################################################################
# IAM Policies
################################################################################
data "aws_iam_policy_document" "collibra_glue_policy" {
  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetCatalogImportStatus",
      "glue:GetPartition",
      "glue:GetTables",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:GetDatabases",
      "glue:GetTable"
    ]
    resources = [
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*"
    ]

  }

  statement {
    effect = "Deny"
    actions = [
      "lakeformation:PutDataLakeSettings"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "collibra_s3_athena_access_policy" {

  statement {
    sid = "AthenaListAccess"
    actions = [
      "athena:ListWorkGroups",
      "athena:ListEngineVersions",
      "athena:ListDataCatalogs"
    ]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]
      
  }

  statement {
    sid = "AthenaQueryAccess"
    actions = [
      "athena:BatchGetQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:ListQueryExecutions",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup"
    ]
    resources = [
      "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
    ]
  }

  statement {
    sid = "AthenaCatelogAccess"
    actions = [
      "athena:GetDataCatalog",
      "athena:GetTableMetadata",
      "athena:GetDatabase",
      "athena:ListDatabases",
      "athena:ListTableMetadata"
    ]
    resources = [
      "*"
    ]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncryptFrom",
      "kms:ReEncryptTo"
    ]
    resources = [module.kms_key.key_arn]
  }

  statement {
    sid = "s3Access"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload",
      "s3:PutObject"
    ]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = [
      "${module.bucket_raw.s3_bucket_arn}",
      "${module.bucket_raw.s3_bucket_arn}/*",
      "${module.bucket_curated.s3_bucket_arn}",
      "${module.bucket_curated.s3_bucket_arn}/*",
      "${module.bucket_transformed.s3_bucket_arn}",
      "${module.bucket_transformed.s3_bucket_arn}/*"   
    ]
  }
}


data "aws_iam_policy_document" "redshift_metadata_permissions" {
  statement {
    effect = "Allow"
    actions = [
      "redshift:DescribeClusters",
      "redshift:DescribeClusterParameters",
      "redshift:DescribeClusterParameterGroups",
      "redshift:DescribeDatabase",
      "redshift:DescribeSchema",
      "redshift:DescribeTable",
      "redshift:DescribeTables",
      "redshift:GetClusterCredentials",
      "redshift:ListDatabases",
      "redshift:ListSchemas",
      "redshift:ListTables"
    ]
    resources = [
      "arn:aws:redshift:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:cluster:${var.redshift_cluster_identifier}",
      "arn:aws:redshift:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbname:${var.redshift_cluster_identifier}/*",
      "arn:aws:redshift:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${var.redshift_cluster_identifier}/*"
    ]
  }
}


################################################################################
# Event Driven Cross Account access System Integration Policy
################################################################################
data "aws_iam_policy_document" "sys_integration_access_service_policy" {

  statement {
    sid       = "DynamoDBTableProjectStatisticsAccess"
    actions   = ["dynamodb:Query"]
    resources = ["arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*"]
  }

  statement {
    sid       = "AthenaQueryExecutionAccess"
    actions   = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution", 
      "athena:GetQueryResults"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "GlueDataCatalogReadAccess"
    actions   = [
      "glue:GetTable",
      "glue:GetDatabase",
      "glue:GetTableVersion",
      "glue:GetDatabase",
      "glue:SearchTables",
      "glue:GetTables",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*"
    ]
  }

  statement {
    sid     = "AllowS3Access"
    actions = ["s3:*"]
    resources = [
      "arn:aws:s3:::${module.bucket_landing_label.id}",
      "arn:aws:s3:::${module.bucket_landing_label.id}/*"
    ]
  }

  statement {
    sid = "AllowDataLakeS3Access"
    actions = [
      "s3:Get*",
      "s3:List*"
    ]
    resources = ["arn:aws:s3:::worley*"]

  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }

  statement {
    sid       = "GetSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:*"]
  }

  statement {
    sid       = "AllowLakeFormationAccess"
    actions   = [
      "lakeformation:GetDataAccess",
      "lakeformation:GetLFTag",
      "lakeformation:GetResourceLFTags",
      "lakeformation:GrantPermissions",
      "lakeformation:ListDataCellsFilter",
      "lakeformation:ListLFTags",
      "lakeformation:ListPermissions"
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*"
    ]

  }

  statement {
    sid = "GetSSMParameter"
    actions = [
      "ssm:DescribeParameters",
      "ssm:GetParameterHistory",
      "ssm:GetParametersByPath",
      "ssm:GetParameters",
      "ssm:GetParameter"
    ]
    resources = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/*"]
  }

  statement {
    sid     = "AllowKMSAssociate"
    actions = ["logs:AssociateKmsKey"]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]

  }

}

################################################################################
# System Integration Cross Account IAM Role
################################################################################

locals {
  # Set account ID based on the environment
  integration_account_id = {
    "dev" = "626635445084"
    "qa"  = "147997124047"
    "prd" = "440744239630"
  }

   # Get the selected account ID based on the environment
  selected_account_id = lookup(local.integration_account_id, var.environment, null)

  # Ensure that the selected_account_id is provided
  account_id_required = local.selected_account_id != null ? local.selected_account_id : "Account ID for the specified environment is not set"
}

output "selected_account_id" {
  value = local.account_id_required
}
module "sys_integration_cross_account_access_iam_role" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "AWS" = ["arn:aws:iam::${local.account_id_required}:role/apse2-role-lambda-eip-${var.environment}"]
  }

  assume_role_actions = ["sts:AssumeRole"]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.sys_integration_access_service_policy.json]

  policy_description = "Policy for S3, DynamoDB, Athena, LakeFormation, Glue catalog Data Access"
  role_description   = "Cross-account role for System Integration to access S3, DynamoDB, Athena, LakeFormation, Glue catalog Data"

  attributes = ["sys-int", "cross-account", "iam", "role"]
  context    = module.label.context
}



################################################################################
# Glue to Redshift read role & Policy
################################################################################
data "aws_iam_policy_document" "redshift_glue_s3_policy" {

  statement {
    sid = "RedshiftAccess"
    actions = [
      "redshift:DescribeQev2IdcApplications",
      "redshift-serverless:ListNamespaces",
      "redshift-serverless:ListWorkgroups",
      "redshift-serverless:GetWorkgroup"
    ]
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]
  }

  statement {
    sid = "AllowSSOAccess"
    actions = ["sso:DescribeApplication",
      "sso:DescribeInstance",
      "sso:CreateApplication",
      "sso:DeleteApplicationAccessScope",
      "sso:DeleteApplicationGrant",
      "sso:GetApplicationGrant",
      "sso:ListApplicationAccessScopes",
      "sso:PutApplicationAccessScope",
      "sso:PutApplicationAssignmentConfiguration",
      "sso:PutApplicationAuthenticationMethod",
      "sso:PutApplicationGrant",
      "sso:UpdateApplication"
    ]
    resources = ["*"]
  }

  statement {
    sid = "Allows3access"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListMultipartUploadParts",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]
    resources = [
      "${module.bucket_raw.s3_bucket_arn}",
      "${module.bucket_raw.s3_bucket_arn}/*"

    ]
  }

  statement {
    sid = "AllowGlueAccess"
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
    ]
    resources = [module.kms_key.key_arn]
  }
}

module "redshit_glue_iam_role" {
  count = var.environment == "dev" ? 1 : 0

  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["redshift.amazonaws.com"]
  }

  assume_role_actions = ["sts:AssumeRole"]

  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.redshift_glue_s3_policy.json]

  policy_description = "Policy for Redshift to read for glue"
  role_description   = "Policy for Redshift to read for glue"

  attributes = ["iam", "service", "role", "redshift", "glue-s3"]
  context    = module.label.context
}


################################################################################
# DataSync IAM Role for OIC to S3
################################################################################


module "datasync_s3_iam_role" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["datasync.amazonaws.com"]
  }

  assume_role_actions = ["sts:AssumeRole"]

  assume_role_conditions = [
    {
      test = "StringEquals"
      variable = "aws:SourceAccount"
      values = [data.aws_caller_identity.current.account_id]
    }
  ]
  policy_document_count = 2
  policy_documents      = [data.aws_iam_policy_document.data_sync_s3_access_policy.json, data.aws_iam_policy_document.cloudwatch_log_group_people_domain.json]

  policy_description = "Policy for AWS DataSync with access to S3 service to put the data from OCI"
  role_description   = "Role for AWS DataSync with access to S3 & KMS service to put the data from OCI"

  attributes = ["iam", "datasync", "people", "role"]
  context    = module.label.context
}

data "aws_iam_policy_document" "cloudwatch_log_group_people_domain" {
  statement {
    sid = "DatasyncCWloggroupaccess"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutLogEventsBatch",
    ]
    resources = [aws_cloudwatch_log_group.people_domain_cw_log_group.arn]
  }
}

data "aws_iam_policy_document" "data_sync_s3_access_policy" {

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }

  statement {
    sid = "AllowDataSyncS3Bucket"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:GetObjectVersion",
      "s3:GetObjectVersionTagging",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjectsV2",
      "s3:ListBucketMultipartUploads"
    ]
    resources = [
      module.bucket_raw_people_domain.s3_bucket_arn,
      "${module.bucket_raw_people_domain.s3_bucket_arn}/*",
      "arn:aws:s3:::worley-datalake-sydney-test-bucket-peoplelink-qjfx8q",
      "arn:aws:s3:::worley-datalake-sydney-test-bucket-peoplelink-qjfx8q/*"
    ]

  }
}

################################################################################
# AWS DAC API IAM Role for lambda
################################################################################


module "api_dac_lambda_iam_role" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  count   = local.create_apigw_switch
  source  = "cloudposse/iam-role/aws"
  version = "0.19.0"

  principals = {
    "Service" = ["lambda.amazonaws.com"]
  }

  assume_role_actions = ["sts:AssumeRole"]

  assume_role_conditions = [
    {
      test = "StringEquals"
      variable = "aws:SourceAccount"
      values = [data.aws_caller_identity.current.account_id]
    }
  ]
  policy_document_count = 1
  policy_documents      = [data.aws_iam_policy_document.api_dac_lambda_access_policy.json]

  policy_description = "Policy for AWS DataSync with access to S3 service to put the data from OCI"
  role_description   = "Role for AWS DataSync with access to S3 & KMS service to put the data from OCI"

  attributes = ["iam", "api","dac","lambda", "role"]
  context    = module.label.context
}


data "aws_iam_policy_document" "api_dac_lambda_access_policy" {

  statement {
    sid = "KMS"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:ReEncrypt*",
      "kms:ListKeys",
      "kms:GenerateDataKey"
    ]
    resources = [module.kms_key.key_arn]
  }
  statement {
    sid     = "GetClusterCredentials"
    actions = ["redshift:GetClusterCredentials"]
    resources = [
      "arn:aws:redshift:${var.aws_region}:${data.aws_caller_identity.current.account_id}:dbuser:worley-datawarehouse-${var.stage}-${var.environment}-redshift/admin",
      "arn:aws:redshift:${var.aws_region}:${data.aws_caller_identity.current.account_id}:dbname:worley-datawarehouse-${var.stage}-${var.environment}-redshift/*"
    ]
  }

  statement {
    sid     = "DescribeCluster"
    actions = ["redshift:DescribeClusters"]
    resources = ["arn:aws:redshift:${var.aws_region}:${data.aws_caller_identity.current.account_id}:cluster:worley-datawarehouse-${var.stage}-${var.environment}-redshift",
    "arn:aws:redshift-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/*"]
  }

  statement {
    sid       = "GetSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:redshift-worley-datalake-sydney-${var.environment}-redshift-serverless-namespace-api-dac-user-*"]
  }

  statement {
    sid     = "RedshiftDataAPIGetStatement"
    actions = ["redshift-data:GetStatementResult"]
    #need * to call get statement result
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]
  }

  statement {
    sid     = "RedshiftDataAPIExecuteStatement"
    actions = ["redshift-data:ExecuteStatement"]
    resources = [
      "arn:aws:redshift:${var.aws_region}:${data.aws_caller_identity.current.account_id}:cluster:worley-datawarehouse-${var.stage}-${var.environment}-redshift",
      "arn:aws:redshift-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/*"
    ]
  }

  statement {
    sid     = "DataAPIResult"
    actions = ["redshift-data:DescribeStatement"]
    #need * to call describe statement
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = ["*"]
  }
    statement {
    sid     = "CloudwatchLogs"
    actions = [
                "logs:PutLogEvents",
                "logs:CreateLogStream",
                "logs:CreateLogGroup"
            ]
    #need * to call describe statement
    #tfsec:ignore:aws-iam-no-policy-wildcards
    resources = [
                "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.aws_region}-apigw-dac-int-func-datalake-${var.environment}:*:*",
                "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.aws_region}-apigw-dac-int-func-datalake-${var.environment}:*"
            ]
  }
}