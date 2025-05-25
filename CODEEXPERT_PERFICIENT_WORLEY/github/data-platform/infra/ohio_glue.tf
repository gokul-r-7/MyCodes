module "ohio_glue_connection_security_group" {
  source  = "cloudposse/security-group/aws"
  version = "2.2.0"

  providers = { aws = aws.us-east-2 }

  vpc_id                = var.ohio_vpc_id
  create_before_destroy = true
  allow_all_egress      = true

  rules = [
    {
      type        = "ingress"
      from_port   = 0
      to_port     = 65535
      protocol    = "tcp"
      cidr_blocks = []
      self        = true
      description = "All all ports from self"
    }
  ]

  attributes = ["glue", "connection", "sg"]
  context    = module.ohio_label.context
}

module "ohio_glue_connection" {
  source  = "cloudposse/glue/aws//modules/glue-connection"
  version = "0.4.0"

  providers = { aws = aws.us-east-2 }

  connection_description = "Glue connection to Postgres database"
  connection_type        = "NETWORK"
  connection_properties  = {}

  physical_connection_requirements = {
    security_group_id_list = [module.ohio_glue_connection_security_group.id]
    availability_zone      = data.aws_subnet.ohio_private_glue.availability_zone
    subnet_id              = data.aws_subnet.ohio_private_glue.id
  }

  attributes = ["glue", "connection"]
  context    = module.ohio_label.context
}

module "ohio_glue_security_config_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  providers = { aws = aws.us-east-2 }

  context    = module.ohio_label.context
  attributes = ["glue", "security", "configuration", "kms"]
  delimiter  = "_"
}

resource "aws_glue_security_configuration" "ohio_buckets_kms_security_config" {
  name = module.ohio_glue_security_config_label.id

  provider = aws.us-east-2

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = module.kms_key_replica.key_arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = module.kms_key_replica.key_arn
    }

    s3_encryption {
      kms_key_arn        = module.kms_key_replica.key_arn
      s3_encryption_mode = "SSE-KMS"
    }
  }
}

