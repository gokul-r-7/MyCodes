data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}
data "aws_vpc" "selected" {
  id = var.vpc_id
}

locals {
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id
}

data "aws_subnets" "private_application" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
  }

  tags = {
    Tier = "application"
  }
}

data "aws_subnets" "private_dataware" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
  }

  tags = {
    Tier = "dataware"
  }
}

data "aws_subnet" "private_dataware" {
  id = tolist(data.aws_subnets.private_dataware.ids)[0]
}


data "aws_subnets" "private_redshift_serverless" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}", "${data.aws_availability_zones.available.names[2]}"]
  }

  tags = {
    Tier = "redshift"
  }
}

data "aws_subnets" "private_glue" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
  }

  tags = {
    Tier = "glue"
  }
}

data "aws_subnet" "private_glue" {
  id = tolist(data.aws_subnets.private_glue.ids)[0]
}

data "aws_subnets" "private_transit" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
  }

  tags = {
    Tier = "transit"
  }
}

data "aws_ssoadmin_instances" "idc" {}

data "aws_s3_bucket" "external_user_store" {
  bucket = "${var.namespace}-mf-${var.stage}-${var.environment}-external-user-store"
}

data "aws_iam_role" "quicksight_service_role" {
  name = "aws-quicksight-service-role-v0"
}
################################################################################
# Secrets to access Datasync for Aconex SMB share for US
################################################################################

data "aws_secretsmanager_secret" "aconex_smb_secrets" {
  name = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:worley-datalake-sydney-${var.environment}-db-aconex-bot-server-${local.aconex_bot_us_scrt_arn_ext_key_value}"
}

data "aws_secretsmanager_secret_version" "aconex_smb_secret_credentials" {
  secret_id = data.aws_secretsmanager_secret.aconex_smb_secrets.id
}

# Read the account metadata parameter
data "aws_ssm_parameter" "account_metadata" {
  name     = "/worley/aws/account/metadata"
  provider = aws.sydney
}

data "aws_iam_role" "data_platform_engineer_role" {
  name = var.data_platform_engineer_role
}