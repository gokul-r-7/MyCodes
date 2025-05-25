module "redshift_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  name       = "datawarehouse"
  attributes = ["redshift"]
  context    = module.label.context
}

resource "random_password" "redshift" {
  length  = 16
  special = true
}

module "redshift" {
  source  = "terraform-aws-modules/redshift/aws"
  version = "5.4.0"

  cluster_identifier    = module.redshift_label.id
  allow_version_upgrade = true
  node_type             = "ra3.xlplus"
  number_of_nodes       = 1

  database_name   = var.environment
  master_username = "admin"
  #checkov:skip=CKV_AWS_304:Master Redshift password cannot be rotated
  master_password        = random_password.redshift.result
  create_random_password = false


  encrypted   = true
  kms_key_arn = module.kms_redshift.key_arn

  enhanced_vpc_routing   = false
  vpc_security_group_ids = [module.redshift_security_group.security_group_id]
  subnet_ids             = data.aws_subnets.private_dataware.ids

  availability_zone_relocation_enabled = true

  logging = {
    enable        = true
    bucket_name   = module.bucket_redshift_logs.s3_bucket_id
    s3_key_prefix = "redshift/"
  }

  # Parameter group
  parameter_group_parameters = {

    require_ssl = {
      name  = "require_ssl"
      value = true
    }

    enable_user_activity_logging = {
      name  = "enable_user_activity_logging"
      value = true
    }

  }

  # Subnet group
  subnet_group_name        = "${module.redshift_label.id}-subnet"
  subnet_group_description = "Custom subnet group for ${module.redshift_label.id}"
  subnet_group_tags = {
    Additional = "RedshiftSubnetGroup"
  }

  # Snapshot schedule
  create_snapshot_schedule        = true
  snapshot_schedule_identifier    = module.redshift_label.id
  use_snapshot_identifier_prefix  = true
  snapshot_schedule_description   = "Snapshot schedule"
  snapshot_schedule_definitions   = ["rate(12 hours)"]
  snapshot_schedule_force_destroy = true

  # Scheduled actions
  create_scheduled_action_iam_role = true
  iam_role_name                    = module.redshift_label.id
  iam_role_use_name_prefix         = false
  iam_role_arns = [
    aws_iam_role.redshift.arn,
    module.redshit_idc_integrations_iam_role.arn
  ]
  default_iam_role_arn = module.redshit_idc_integrations_iam_role.arn

  # Endpoint access
  create_endpoint_access          = true
  endpoint_name                   = "redshift-${var.environment}-endpoint" # limited to 30 characters
  endpoint_subnet_group_name      = aws_redshift_subnet_group.endpoint.id
  endpoint_vpc_security_group_ids = [module.redshift_security_group.security_group_id]

  # Usage limits
  usage_limits = {
    currency_scaling = {
      feature_type  = "concurrency-scaling"
      limit_type    = "time"
      amount        = 60
      breach_action = "emit-metric"
    }
    spectrum = {
      feature_type  = "spectrum"
      limit_type    = "data-scanned"
      amount        = 2
      breach_action = "disable"
      tags = {
        Additional = "CustomUsageLimits"
      }
    }
  }

  tags = module.redshift_label.tags
  # depends_on = [null_resource.create_redshift_idc_application] # NOTE: this is enabled/disabled manually intentionally, please see null_resource.create_redshift_idc_application for more details.
}

module "redshift_security_group_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["redshift", "sg"]
}

module "redshift_security_group" {
  source  = "terraform-aws-modules/security-group/aws//modules/redshift"
  version = "~> 5.0"

  name        = module.redshift_security_group_label.id
  description = "Redshift security group"
  vpc_id      = var.vpc_id

  # Allow ingress rules to be accessed only within current VPC
  ingress_rules       = ["redshift-tcp"]
  # Add this block to allow access from 10.0.0.0/8  
  ingress_with_cidr_blocks = [    
    {
      rule        = "redshift-tcp"
      cidr_blocks = "10.0.0.0/8"
      description = "Allow access from 10.0.0.0/8 network"
    }
  ]
  #ingress_cidr_blocks = [data.aws_vpc.selected.cidr_block]
  #ingress_with_cidr_blocks = [
  #  {
  #    cidr_block = "10.0.0.0/8"  # Allow access from 10.0.0.0/8 subnet
  #    from_port  = 5439           # Default Redshift port
  #    to_port    = 5439           # Default Redshift port
  #    protocol   = "tcp"
  #    description = "Allow access from Worley Pvt network to redshift"
  #  }
	#]

  #ingress_with_source_security_group_id = [
   # {
    #  from_port                = 5439
     # to_port                  = 5439
      #protocol                 = "tcp"
      #description              = "Allow VPC"
      #source_security_group_id = module.quicksight_security_group.security_group_id
    #},
    #{
     # from_port                = 5439
      #to_port                  = 5439
      #protocol                 = "tcp"
      #description              = "Allow airflow"
      #source_security_group_id = module.mwaa_sg.security_group_id
    #},
    #{
     # from_port                = 5439
      #to_port                  = 5439
      #protocol                 = "tcp"
      #description              = "Worley pvt"
      #source_security_group_id = module.redshift_worley_pvt_sg.security_group_id
    #}
  #]


  # Allow all rules for all protocols
  egress_cidr_blocks      = [data.aws_vpc.selected.cidr_block]
  egress_ipv6_cidr_blocks = []
}



resource "aws_redshift_subnet_group" "endpoint" {
  name       = "redshift-${var.environment}-endpoint"
  subnet_ids = data.aws_subnets.private_dataware.ids
}

data "aws_iam_policy_document" "redshift" {
  statement {
    effect = "Allow"
    actions = [
      "s3:DeleteObject",
      "s3:GetBucket",
      "s3:GetObject",
      "s3:ListAllMyBuckets",
      "s3:ListBucket",
      "s3:PutObject",
    ]
    resources = [
      "${module.bucket_curated.s3_bucket_arn}",
    ]
  }

  statement {
    sid    = "AllowGlue"
    effect = "Allow"
    actions = [
      "glue:CreateConnection",
      "glue:DeleteConnection",
      "glue:GetConnection",
      "glue:GetConnections",
      "glue:UpdateConnection",

      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",

      "glue:CreateCrawler",
      "glue:DeleteCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlers",
      "glue:UpdateCrawler"
    ]
    resources = [
      module.glue_connection[0].arn,
      module.glue_catalog_database_ecosys_raw.arn,
      module.glue_catalog_database_ecosys_curated.arn,
      module.glue_crawler_raw_ecosys_v2.arn,
      module.glue_catalog_database_p6.arn,
      module.glue_crawler_raw_p6.arn,
    ]
  }

  statement {
    sid    = "RedshiftPolicyForLF"
    effect = "Allow"
    actions = [
      "lakeformation:GetDataAccess"
    ]
    resources = ["*"]
  }
}

module "redshift_iam_role_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["redshift", "iam", "role"]
}

resource "aws_iam_role" "redshift" {
  name = module.redshift_iam_role_label.id

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "AllowRedshiftToAssumeRole"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })

  inline_policy {
    name   = "RedshiftPolicy"
    policy = data.aws_iam_policy_document.redshift.json
  }
}

module "redshift_idc_app_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["redshift", "idc", "app"]
}


module "redshift_worley_pvt_sg" {
  source      = "terraform-aws-modules/security-group/aws"
  version     = "5.1.2"
  name        = "${module.label.id}-ext-resource"
  description = "Security group for Non-AWS resources"
  vpc_id      = var.vpc_id

  # allow ingress amazon mwaa traffic
  ingress_with_self = [
    {
      rule = "all-all"
    }
  ]

  #ingress_with_cidr_blocks = [

   # {
    #  from_port   = 5439
     # to_port     = 5439
      #protocol    = "tcp"
      #description = "Allow Redshift access from private IPs"
      #cidr_blocks = "10.0.0.0/8"
    #}
  #]

  # allow egress for ipv4
  egress_with_cidr_blocks = [
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      description = "Allow All Egress"
      cidr_blocks = "10.0.0.0/8"
    },
  ]

}


########################################################################
# NOTE: THE FOLLOWING CONTENT IS ENABLED/DISABLED INTENTIONALLY
# NOTE: null_resource is known for inconsistency between plans and apply
########################################################################

resource "null_resource" "create_redshift_idc_application" {
  count = var.redshift_idc_app ? 1 : 0
  # triggers = {
  #   # always_run       = true # uncomment to force run
  #   iam_role_arn     = module.redshit_idc_integrations_iam_role.arn
  #   application_name = module.redshift_idc_app_label.id
  #   idc_instance_arn = tolist(data.aws_ssoadmin_instances.idc.arns)[0]
  #   aws_region       = var.aws_region
  # }

  provisioner "local-exec" {
    command = <<-EOT
      aws redshift create-redshift-idc-application \
        --region '${var.aws_region}' \
        --idc-instance-arn  '${tolist(data.aws_ssoadmin_instances.idc.arns)[0]}' \
        --identity-namespace 'AWSIDC' \
        --idc-display-name '${module.redshift_idc_app_label.id}' \
        --iam-role-arn '${module.redshit_idc_integrations_iam_role.arn}' \
        --redshift-idc-application-name '${module.redshift_idc_app_label.id}' \
        --service-integrations '[{"LakeFormation":[{"LakeFormationQuery":{"Authorization": "Enabled"}}]}]'
    EOT
  }
}

resource "null_resource" "update_identity_propagation_config" {
  count = var.redshift_idc_app ? 1 : 0
  # triggers = {
  #   # always_run       = true # uncomment to force run
  #   iam_role_arn     = module.redshit_idc_integrations_iam_role.arn
  #   application_name = module.redshift_idc_app_label.id
  #   idc_instance_arn = tolist(data.aws_ssoadmin_instances.idc.arns)[0]
  #   aws_region       = var.aws_region
  # }
  provisioner "local-exec" {
    command = <<-EOT
      aws --region '${var.aws_region}' quicksight update-identity-propagation-config \
        --aws-account-id '${data.aws_caller_identity.current.account_id}' \
        --service 'REDSHIFT' \
        --authorized-targets "$(aws --region '${var.aws_region}' redshift describe-redshift-idc-applications --query 'RedshiftIdcApplications[?IdcDisplayName==`${module.redshift_idc_app_label.id}`].IdcManagedApplicationArn' --output text)"
    EOT
  }

  depends_on = [null_resource.create_redshift_idc_application]
}
