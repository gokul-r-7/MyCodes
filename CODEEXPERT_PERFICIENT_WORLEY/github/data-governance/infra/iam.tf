resource "aws_iam_policy" "redshift_setup_lambda_policy" {
  name        = "redshift_setup_lambda_policy"
  path        = "/"
  description = "lakeformation full access for lambda"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = data.aws_iam_policy_document.redshift_connection_policy.json
}
data "aws_iam_policy_document" "redshift_connection_policy" {

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
    resources = ["arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:redshift!worley-datalake-${var.stage}-${var.environment}-redshift-serverless-namespace-admin-*"]
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
    sid = "S3Ops"
    actions = [
      "s3:*Object",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.external_user_store_bucket.arn}",
      "${aws_s3_bucket.external_user_store_bucket.arn}/*"
    ]
  }

  statement {
    sid = "DynamoDb"
    actions = [
      "dynamodb:Scan",
      "dynamodb:Query",
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams"
    ]
    resources = [
      module.role_permissions_store.dynamodb_table_arn,
      module.database_permissions_store.dynamodb_table_arn
    ]
  }

  statement {
    sid = "lambdaInvoke"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      module.lakeformation_manage_permissions_function.lambda_function_arn
    ]
  }
}


resource "aws_iam_policy" "lakeformation_setup_lambda_policy" {
  name        = "lakeformation_setup_lambda_policy"
  path        = "/"
  description = "lakeformation full access for lambda"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "lakeformation:*",
          "glue:*",
          "s3:ListAllMyBuckets",
          "s3:GetBucketLocation",
          "iam:ListUsers",
          "iam:ListRoles",
          "iam:GetRole",
          "iam:GetRolePolicy"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "lakeformation:CreateLFTag",
          "lakeformation:GetLFTag",
          "lakeformation:ListLFTags",
          "lakeformation:DeleteLFTag",
          "lakeformation:UpdateLFTag",
          "identitystore:ListGroups",
          "sso:DescribeInstance",
          "sso-directory:DescribeGroup",
          "sso-directory:ListDirectory",
          "sso-directory:ListProfiles",
          "sso-directory:ListUsers",
          "sso-directory:SearchGroups",
          "sso-directory:SearchUsers",
          "sso-directory:DescribeProfile",
          "sso-directory:DescribeUser",
          "sso-directory:DescribeDirectory",
          "sso-directory:DescribeInstanceIdentityStore",
          "sso-directory:DescribeProfileMember",
          "sso-directory:DescribeUserMember",
          "sso-directory:SearchGroups",
          "sso-directory:SearchUsers",
          "sso-directory:SearchProfiles",
          "sso-directory:SearchMembers",
          "sso-directory:SearchDirectory",
          "sso-directory:SearchIdentityStore",
          "sso-directory:SearchProfileMembers",
          "sso-directory:SearchUserMembers",
          "sso-directory:SearchProfileMembers"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "sns:publish"
        ]
        Effect   = "Allow"
        Resource = "${aws_sns_topic.rbac-notifications.arn}"
      },
      {
        Action = [
          "dynamodb:Scan",
          "dynamodb:GetRecords",
          "dynamodb:GetShardIterator",
          "dynamodb:DescribeStream",
          "dynamodb:ListStreams"
        ]
        Effect = "Allow"
        Resource = [
          module.role_permissions_store.dynamodb_table_arn,
          module.database_permissions_store.dynamodb_table_arn
        ]
      },
      {
        Action = [
          "quicksight:CreateRoleMembership"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "sso:CreateApplicationAssignment"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:sso::${var.root_account_id}:application/*/*"
      }
    ]
  })
}

resource "aws_iam_policy" "data_modelling_sns_notification_policy" {
  name        = "data_modelling_sns_notification_lambda_policy"
  path        = "/"
  description = "sns access for lambda"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sns:publish"
        ]
        Effect   = "Allow"
        Resource = "${aws_sns_topic.modelling-dag-notifications.arn}"
      }
    ]
  })
}


###################################
#                                 #
#               DATAZONE          #
###################################

resource "aws_iam_role" "datazone_iam_role" {
  name = "datazone-iam-role-${var.stage}-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["sts:AssumeRole", "sts:TagSession"]
        Effect = "Allow"
        Principal = {
          Service = "datazone.amazonaws.com"
        }
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = "${data.aws_caller_identity.current.account_id}"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "datazone_policy_attachment" {
  role       = aws_iam_role.datazone_iam_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDataZoneDomainExecutionRolePolicy"
}
