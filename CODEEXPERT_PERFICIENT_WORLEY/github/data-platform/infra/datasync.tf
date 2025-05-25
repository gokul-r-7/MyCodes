data "aws_ssm_parameter" "datasync_ami" {
  name = "/aws/service/datasync/ami"
}

data "aws_iam_policy_document" "cloudwatch_log_group" {
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutLogEventsBatch",
    ]
    resources = [aws_cloudwatch_log_group.this.arn]
    principals {
      identifiers = ["datasync.amazonaws.com"]
      type        = "Service"
    }
  }
}

module "datasync_instance_policy_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "instance", "policy"]
}

resource "aws_iam_role_policy" "datasync_instance_policy" {
  name   = module.datasync_instance_policy_label.id
  role   = aws_iam_role.datasync_instance_role.name
  policy = data.aws_iam_policy_document.datasync_instance_policy.json
}

# ref https://repost.aws/questions/QUbx7pdp-qTWWOiUb-WtEhFQ/resource-handler-returned-message-the-provided-execution-role-does-not-have-permissions-to-call-createnetworkinterface-on-ec2-service-lambda-status-code-400
# Require resource * for Create, Describe and Delete network interfaces
#tfsec:ignore:aws-iam-no-policy-wildcards
data "aws_iam_policy_document" "datasync_instance_policy" {
  statement {
    effect = "Allow"
    actions = [
      "datasync:*",
      "elasticfilesystem:Describe*"
    ]
    resources = [
      module.datasync_efs_file_system.arn
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeRegions",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcEndpoints",
      "ec2:ModifyNetworkInterfaceAttribute",
    ]
    resources = [
      "*"
    ]
  }

}

module "datasync_s3_access_role_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "s3", "access", "role"]
}

data "aws_iam_policy_document" "datasync_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["datasync.amazonaws.com"]
      type        = "Service"
    }
  }
}

data "aws_iam_policy_document" "bucket_access" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
      "s3:AbortMultipartUpload"
      ]

    resources = [
      "${module.bucket_datasync.s3_bucket_arn}",
      "${module.bucket_datasync.s3_bucket_arn}:/*",
      "${module.bucket_datasync.s3_bucket_arn}:job/*"
    ]
  }
}

resource "aws_iam_role" "datasync_s3_access_role" {
  name               = module.datasync_s3_access_role_label.id
  assume_role_policy = data.aws_iam_policy_document.datasync_assume_role.json
}

resource "aws_iam_role_policy" "datasync-s3-access-policy" {
  name   = module.datasync_s3_access_role_label.id
  role   = aws_iam_role.datasync_s3_access_role.name
  policy = data.aws_iam_policy_document.bucket_access.json
}

module "datasync_instance_role_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "instance", "role"]
}

resource "aws_iam_role" "datasync_instance_role" {
  name               = module.datasync_instance_role_label.id
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}


module "datasync_cw_policy_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "cw", "logs", "policy"]
}

resource "aws_cloudwatch_log_resource_policy" "this" {
  policy_document = data.aws_iam_policy_document.cloudwatch_log_group.json
  policy_name     = module.datasync_cw_policy_label.id
}

module "datasync_cw_logs_group_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "cw", "logs", "group"]
}

resource "aws_cloudwatch_log_group" "this" {
  name              = module.datasync_cw_logs_group_label.id
  retention_in_days = 14
}

module "datasync_instance" {
  source  = "cloudposse/ec2-instance/aws"
  version = "1.4.1"

  #ssh_key_pair = module.ec2_key_pair.key_name
  vpc_id       = var.vpc_id
  subnet       = var.ec2_subnet_id

  ami               = data.aws_ssm_parameter.datasync_ami.value
  # availability_zone = data.aws_availability_zones.available.names[0]
  ebs_volume_type   = "gp3"
  ebs_volume_size   = 160
  root_volume_size  = 160

  instance_profile = aws_iam_instance_profile.ec2_role_profile.name
  instance_type    = "m5.4xlarge"
  security_group_rules = [
    {
      type        = "egress"
      from_port   = 0
      to_port     = 65535
      protocol    = "-1"
      cidr_blocks = ["0.0.0.0/0"]
    },
    {
      type        = "ingress"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
    },
    {
      type        = "ingress"
      from_port   = 80
      to_port     = 80
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
    },
    {
      type        = "ingress"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
    },
    {
      type        = "ingress"
      from_port   = 1024
      to_port     = 1064
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
    },
    # {
    #   type        = "ingress"
    #   from_port   = 3389
    #   to_port     = 3389
    #   protocol    = "tcp"
    #   cidr_blocks = ["10.0.0.0/8"]
    # },
  ]

  depends_on = [aws_iam_instance_profile.ec2_role_profile]

  context    = module.label.context
  attributes = ["ec2", "datasync"]
}


resource "aws_datasync_location_s3" "this" {
  depends_on = [module.datasync_instance]

  s3_bucket_arn = module.bucket_datasync.s3_bucket_arn
  subdirectory  = var.datasync_location_s3_subdirectory

  s3_config {
    bucket_access_role_arn = aws_iam_role.datasync_s3_access_role.arn
  }
}

module "datasync_agent_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "label"]
}

resource "aws_datasync_agent" "this" {
  depends_on = [module.datasync_instance]

  ip_address            = module.datasync_instance.private_ip
  name                  = module.datasync_agent_label.id
  security_group_arns   = [module.datasync_endpoint_security_group.arn]
  subnet_arns           = [data.aws_subnet.private_dataware.arn]
  vpc_endpoint_id       = aws_vpc_endpoint.datasync.id
  private_link_endpoint = var.datasync_vpc_endpoint_ip # The IP address of the VPC endpoint the agent should connect to when retrieving an activation key during resource creation

  timeouts {
    create = "20m"
  }

  lifecycle {
    create_before_destroy = true
  }
}

module "datasync_efs_file_system_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "efs", "file", "system"]
}

module "datasync_efs_file_system" {
  source  = "terraform-aws-modules/efs/aws"
  version = "1.6.3"

  name = module.datasync_efs_file_system_label.id
}

resource "aws_datasync_location_nfs" "this" {
  depends_on = [module.datasync_instance]

  server_hostname = module.datasync_efs_file_system.dns_name
  subdirectory    = var.datasync_location_nfs_subdirectory

  on_prem_config {
    agent_arns = [aws_datasync_agent.this.arn]
  }

}

module "datasync_task_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "task", "role"]
}

resource "aws_datasync_task" "this" {
  name                     = module.datasync_task_label.id
  source_location_arn      = aws_datasync_location_s3.this.arn
  destination_location_arn = aws_datasync_location_nfs.this.arn
  cloudwatch_log_group_arn = join("", split(":*", aws_cloudwatch_log_group.this.arn))

  options {
    bytes_per_second       = -1
    verify_mode            = var.datasync_task_options["verify_mode"]
    posix_permissions      = var.datasync_task_options["posix_permissions"]
    preserve_deleted_files = var.datasync_task_options["preserve_deleted_files"]
    uid                    = var.datasync_task_options["uid"]
    gid                    = var.datasync_task_options["gid"]
    atime                  = var.datasync_task_options["atime"]
    mtime                  = var.datasync_task_options["mtime"]
  }
}


####################################################
#    People Domain Source Location Creation
####################################################


# Get the secret from Secrets Manager
data "aws_secretsmanager_secret" "oic_secrets" {
  name = "worley-datalake-sydney-${var.environment}-oic-aws-credential-keys"
}

data "aws_secretsmanager_secret_version" "current" {
  secret_id = data.aws_secretsmanager_secret.oic_secrets.id
}


resource "aws_datasync_location_object_storage" "people_domain_oci_bucket_source" {

  depends_on = [module.datasync_instance]

  agent_arns      = [aws_datasync_agent.this.arn]
  server_hostname = var.datasync_location_object_storage_server_name
  bucket_name     = var.datasync_location_object_storage_bucket_name
  access_key      = jsondecode(data.aws_secretsmanager_secret_version.current.secret_string)["access_key"]
  secret_key      = jsondecode(data.aws_secretsmanager_secret_version.current.secret_string)["secret_key"]
  server_protocol = "HTTPS"
  server_port     = 443
  subdirectory    = var.datasync_location_object_storage_subdirectory
}


####################################################
#    People Domain Target Location Creation
####################################################

resource "aws_datasync_location_s3" "people_domain_s3_bucket_target" {
  depends_on = [module.datasync_instance]

  s3_bucket_arn = module.bucket_raw_people_domain.s3_bucket_arn
  subdirectory  = var.datasync_location_s3_subdirectory

  s3_config {
    bucket_access_role_arn = module.datasync_s3_iam_role.arn
  }
}


####################################################
#    People Domain DataSync Task Creation
####################################################


module "datasync_task_people_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "oic", "s3", "people", "task"]
}

resource "aws_datasync_task" "people_domain_datasync_task" {
  name                     = module.datasync_task_people_label.id
  source_location_arn      = aws_datasync_location_object_storage.people_domain_oci_bucket_source.arn
  destination_location_arn = aws_datasync_location_s3.people_domain_s3_bucket_target.arn
  cloudwatch_log_group_arn = join("", split(":*", aws_cloudwatch_log_group.people_domain_cw_log_group.arn))

  includes {
    filter_type = "SIMPLE_PATTERN"
    value       = "/file_hcmtopmodelanalyticsglobalam*"
  }

  options {
    bytes_per_second       = -1
    verify_mode            = var.datasync_task_options["verify_mode"]
    posix_permissions      = var.datasync_task_options["posix_permissions"]
    preserve_deleted_files = var.datasync_task_options["preserve_deleted_files"]
    uid                    = var.datasync_task_options["uid"]
    gid                    = var.datasync_task_options["gid"]
    atime                  = var.datasync_task_options["atime"]
    mtime                  = var.datasync_task_options["mtime"]
    log_level              = var.datasync_task_options["log_level"]
    transfer_mode          = var.datasync_task_options["transfer_mode"]
    overwrite_mode         = var.datasync_task_options["overwrite_mode"]
    object_tags            = var.datasync_task_options["object_tags"]
  }
}

############################
# Log Group for data sync people domain
############################

module "datasync_cw_logs_group_people_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["datasync", "people", "cw", "logs", "group"]
}

resource "aws_cloudwatch_log_group" "people_domain_cw_log_group" {
  name              = module.datasync_cw_logs_group_people_label.id
  retention_in_days = 14
}