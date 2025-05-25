module "endpoint_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["vpc", "endpoint"]
}

# TODO: need to verify if AirFlow needs this

data "aws_route_tables" "glue_rts" {
  vpc_id = var.vpc_id

  filter {
    name   = "tag:Tier"
    values = ["glue"]
  }
}

# resource "aws_vpc_endpoint" "s3_gateway" {
#   vpc_id            = var.vpc_id
#   service_name      = "com.amazonaws.${var.aws_region}.s3"
#   vpc_endpoint_type = "Gateway"
#   route_table_ids   = data.aws_route_tables.glue_rts.ids

#   tags = {
#     "Name" : "${module.endpoint_label.id}-s3-gateway"
#   }
# }

# resource "aws_vpc_endpoint" "s3_endpoint" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.s3"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-s3-endpoint"
#   }
# }

# resource "aws_vpc_endpoint" "monitoring" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.monitoring"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-monitoring"
#   }
# }

# resource "aws_vpc_endpoint" "airflow_api" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.airflow.api"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-airflow-api"
#   }
# }

# resource "aws_vpc_endpoint" "airflow_env" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.airflow.env"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-airflow-env"
#   }
# }

# resource "aws_vpc_endpoint" "logs" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.logs"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-logs"
#   }
# }

# resource "aws_vpc_endpoint" "sqs" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.sqs"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-sqs"
#   }
# }

# resource "aws_vpc_endpoint" "airflow_ops" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.airflow.ops"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-airflow-ops"
#   }
# }

# resource "aws_vpc_endpoint" "ecr_api" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.ecr.api"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-ecr-api"
#   }
# }

# resource "aws_vpc_endpoint" "ecr_dkr" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.ecr.dkr"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-ecr-dkr"
#   }
# }

# resource "aws_vpc_endpoint" "kms" {
#   vpc_id              = var.vpc_id
#   service_name        = "com.amazonaws.${var.aws_region}.kms"
#   vpc_endpoint_type   = "Interface"
#   subnet_ids          = [data.aws_subnets.private_glue.ids[0], data.aws_subnets.private_glue.ids[1]]
#   security_group_ids  = [module.mwaa.security_group_id, module.mwaa_sg.security_group_id]
#   private_dns_enabled = true

#   tags = {
#     "Name" : "${module.endpoint_label.id}-kms"
#   }
# }

# resource "aws_vpc_endpoint" "dynamodb" {
#   vpc_id            = var.vpc_id
#   service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
#   vpc_endpoint_type = "Gateway"
#   route_table_ids   = data.aws_route_tables.glue_rts.ids

#   tags = {
#     "Name" : "${module.endpoint_label.id}-dynamodb"
#   }
# }

module "datasync_endpoint_security_group" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source     = "cloudposse/security-group/aws"
  version    = "2.2.0"
  attributes = ["datasync", "endpoint", "sg"]

  rules = [
    {
      key         = "ingress-22"
      type        = "ingress"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
      self        = false
      description = "Allow Datasync web browser"
    },
    {
      key         = "ingress-80"
      type        = "ingress"
      from_port   = 80
      to_port     = 80
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
      self        = false
      description = "Allow Datasync web browser"
    },
    {
      key         = "ingress-443"
      type        = "ingress"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
      self        = false
      description = "Allow SMB file server"
    },
    {
      key         = "ingress-1024-1064"
      type        = "ingress"
      from_port   = 1024
      to_port     = 1064
      protocol    = "tcp"
      cidr_blocks = ["10.0.0.0/8"]
      self        = false
      description = "Allow Datasync control traffic"
    },
    {
      key         = "egress"
      type        = "egress"
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = ["10.0.0.0/8"]
      self        = null
      description = "All output traffic"
    }
  ]

  vpc_id  = var.vpc_id
  context = module.label.context
}

resource "aws_vpc_endpoint" "datasync" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.datasync"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = data.aws_subnets.private_dataware.ids
  security_group_ids  = [module.datasync_endpoint_security_group.id]
  private_dns_enabled = true

  tags = {
    "Name" : "${module.endpoint_label.id}-datasync"
  }
}