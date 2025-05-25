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

# data "aws_subnets" "private_dataware" {
#   filter {
#     name   = "vpc-id"
#     values = [var.vpc_id]
#   }

#   filter {
#     name   = "availability-zone"
#     values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
#   }

#   tags = {
#     Tier = "dataware"
#   }
# }

# data "aws_subnet" "private_dataware" {
#   id = tolist(data.aws_subnets.private_dataware.ids)[0]
# }

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

# data "aws_subnets" "private_transit" {
#   filter {
#     name   = "vpc-id"
#     values = [var.vpc_id]
#   }

#   filter {
#     name   = "availability-zone"
#     values = ["${data.aws_availability_zones.available.names[0]}", "${data.aws_availability_zones.available.names[1]}"]
#   }

#   tags = {
#     Tier = "transit"
#   }
# }

data "aws_ssoadmin_instances" "idc" {}

# # Read the account metadata parameter
# data "aws_ssm_parameter" "account_metadata" {
#   name     = "/worley/aws/account/metadata"
#   provider = aws.sydney
# }

data "aws_secretsmanager_secret" "redshift_serverless_secret" {
  name = "redshift!worley-datalake-sydney-${var.environment}-redshift-serverless-namespace-admin"
}