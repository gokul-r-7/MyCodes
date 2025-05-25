


# IAM Policy
resource "aws_iam_policy" "athena_workgroup_policy" {
  name        = "athena-workgroup-policy"
  description = "Policy for Athena workgroup with Glue, S3, and Lake Formation permissions"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Athena"
        Effect = "Allow"
        Action = [
          "athena:GetPreparedStatement",
          "athena:CreatePreparedStatement",
          "athena:DeletePreparedStatement",
          "athena:UpdatePreparedStatement",
          "athena:ListPreparedStatements"
        ]
        Resource = ["arn:aws:athena:${var.aws_region}:${var.account_id}:workgroup/${var.athena_workgroup}"]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "AthenaDataCatalog"
        Effect = "Allow"
        Action = ["athena:GetDataCatalog"]
        Resource = [
          "arn:aws:athena:${var.aws_region}:${var.account_id}:workgroup/${var.athena_workgroup}",
          "arn:aws:athena:${var.aws_region}:${var.account_id}:datacatalog/*"
        ]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "Glue"
        Effect = "Allow"
        Action = [
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
          "glue:BatchGetPartition"
        ]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "S3Bucket"
        Effect = "Allow"
        Action = ["s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::${var.results_bucket}/*",
          "arn:aws:s3:::${var.results_bucket}",
          "arn:aws:s3:::aws-athena-query-results-${var.account_id}-${var.aws_region}",
          "arn:aws:s3:::aws-athena-query-results-${var.account_id}-${var.aws_region}/*"
        ]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "S3AccessGrants"
        Effect = "Allow"
        Action = [
          "s3:GetDataAccess",
          "s3:GetAccessGrantsInstanceForPrefix"
        ]
        Resource = ["arn:aws:s3:${var.aws_region}:${var.account_id}:access-grants/default"]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "Lakeformation"
        Effect = "Allow"
        Action = ["lakeformation:GetDataAccess"]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "athena_query_results_policy" {
  name        = "athena-query-results-policy"
  description = "Policy for Athena query results access with S3 and KMS permissions"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ExplicitDenyS3"
        Effect = "Deny"
        Action = ["s3:*"]
        Resource = "*"
        Condition = {
          ArnNotEquals = {
            "s3:AccessGrantsInstanceArn": "arn:aws:s3:${var.aws_region}:${var.account_id}:access-grants/default"
          }
          StringNotEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "ExplictDenyKMS"
        Effect = "Deny"
        Action = ["kms:*"]
        NotResource = "arn:aws:kms:${var.aws_region}:${var.account_id}:key/${var.kms_key_id}"
      },
      {
        Sid    = "ObjectLevelReadPermissions"
        Effect = "Allow"
        Action = [
          "s3:ListMultipartUploadParts",
          "s3:GetObject"
        ]
        Resource = "arn:aws:s3:::${var.results_bucket}/$${identitystore:UserId}/*"
        Condition = {
          ArnEquals = {
            "s3:AccessGrantsInstanceArn": "arn:aws:s3:${var.aws_region}:${var.account_id}:access-grants/default"
          }
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "ObjectLevelWritePermissions"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "arn:aws:s3:::${var.results_bucket}/$${identitystore:UserId}/*"
        Condition = {
          ArnEquals = {
            "s3:AccessGrantsInstanceArn": "arn:aws:s3:${var.aws_region}:${var.account_id}:access-grants/default"
          }
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
        }
      },
      {
        Sid    = "BucketLevelReadPermissions"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.results_bucket}"
        Condition = {
          ArnEquals = {
            "s3:AccessGrantsInstanceArn": "arn:aws:s3:${var.aws_region}:${var.account_id}:access-grants/default"
          }
          StringEquals = {
            "aws:ResourceAccount": var.account_id
          }
          StringLikeIfExists = {
            "s3:prefix": ["$${identitystore:UserId}", "$${identitystore:UserId}/*"]
          }
        }
      },
      {
        Sid    = "KMSPermissions"
        Effect = "Allow"
        Action = [
          "kms:GenerateDataKey",
          "kms:Decrypt"
        ]
        Resource = "arn:aws:kms:${var.aws_region}:${var.account_id}:key/${var.kms_key_id}"
      }
    ]
  })
}


# Output the ARN of the created policy
output "policy_arn" {
  value       = aws_iam_policy.athena_workgroup_policy.arn
  description = "The ARN of the created IAM policy"
}

# Trust policy for the role
data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    sid     = "AthenaTrustPolicy"
    actions = ["sts:AssumeRole", "sts:SetContext"]
    effect  = "Allow"

    principals {
      type        = "Service"
      identifiers = ["athena.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:athena:${var.aws_region}:${var.account_id}:workgroup/${var.athena_workgroup}"]
    }
  }
}

# Create the IAM role
resource "aws_iam_role" "athena_workgroup_role" {
  name               = "athena-workgroup-role"
  description        = "Role for Athena workgroup access"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json

  tags = {
    Environment = "Production"
    Purpose     = "Athena Workgroup Access"
  }
}

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "athena_policy_attachment" {
  role       = aws_iam_role.athena_workgroup_role.name
  policy_arn = aws_iam_policy.athena_workgroup_policy.arn
}

resource "aws_iam_role_policy_attachment" "athena_policy_result_attachment" {
  role       = aws_iam_role.athena_workgroup_role.name
  policy_arn = aws_iam_policy.athena_query_results_policy.arn
}


# Output the role ARN
output "role_arn" {
  value       = aws_iam_role.athena_workgroup_role.arn
  description = "The ARN of the created IAM role"
}

