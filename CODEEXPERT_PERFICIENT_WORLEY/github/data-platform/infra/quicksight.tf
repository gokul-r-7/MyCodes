module "quicksight_execution_role_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["quicksight", "execution", "role"]
  context    = module.label.context
}

resource "aws_iam_role" "vpc_connection_role" {
  name = module.quicksight_execution_role_label.id
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "quicksight.amazonaws.com"
        }
      }
    ]
  })

  # ref https://repost.aws/questions/QUbx7pdp-qTWWOiUb-WtEhFQ/resource-handler-returned-message-the-provided-execution-role-does-not-have-permissions-to-call-createnetworkinterface-on-ec2-service-lambda-status-code-400
  # Require resource * for Create, Describe and Delete network interfaces
  #tfsec:ignore:aws-iam-no-policy-wildcards
  inline_policy {
    name = "QuickSightVPCConnectionRolePolicy"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "ec2:CreateNetworkInterface",
            "ec2:ModifyNetworkInterfaceAttribute",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeSubnets",
            "ec2:DescribeSecurityGroups"
          ]
          #tfsec:ignore:aws-iam-no-policy-wildcards
          Resource = ["*"]
        }
      ]
    })
  }
}

module "quicksight_subscription_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["quicksight", "subscription"]
  context    = module.label.context
}

resource "aws_quicksight_account_subscription" "subscription" {
  count = var.quicksight_account_name != "WorleyAnalyticsSandbox" ? 1 : 0 # to prevent creating this resource in sandbox

  account_name          = var.quicksight_account_name
  authentication_method = "IAM_IDENTITY_CENTER"
  edition               = "ENTERPRISE"

  admin_group  = var.quicksight_admin_group
  author_group = var.quicksight_author_group
  reader_group = var.quicksight_reader_group

  notification_email = var.quicksight_email
}

module "quicksight_connection_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["quicksight", "connection"]
  context    = module.label.context
}

# Uncomment below only if necessary to be deployed
resource "aws_quicksight_vpc_connection" "default" {
  count = var.create_quicksight_vpc_connection ? 1 : 0 # to prevent creating this resource in sandbox

  vpc_connection_id  = var.vpc_id
  name               = module.quicksight_connection_label.id
  role_arn           = aws_iam_role.vpc_connection_role.arn
  security_group_ids = [module.redshift_security_group.security_group_id]
  subnet_ids         = data.aws_subnets.private_dataware.ids
}

# create a security group resource which is dedicated to quicksight
module "quicksight_security_group_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["quicksight", "security", "group"]
  context    = module.label.context
}

module "quicksight_security_group" {
  source  = "terraform-aws-modules/security-group/aws"
  version = "4.9.0"

  name        = module.quicksight_security_group_label.id
  description = "Security group for QuickSight"
  vpc_id      = var.vpc_id

  ingress_cidr_blocks = [data.aws_vpc.selected.cidr_block]
  ingress_rules       = ["https-443-tcp"]
  egress_with_cidr_blocks = [
    {
      rule        = "all-all"
      cidr_blocks = "10.0.0.0/8"
      description = "Allow all outbound traffic within VPC including vpc endpoints"
    }
  ]

  tags = module.quicksight_security_group_label.tags
}


