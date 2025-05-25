###########################################
# Aurora PostgreSQL
###########################################
resource "aws_db_subnet_group" "aurora" {
  name       = "auroradbsubnet"
  subnet_ids = data.aws_subnets.private_dataware.ids

  tags = {
    Name = "Aurora DB Subnet"
  }
}

module "aurora_db_label" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["auroradb", "data"]
}

# Create a security group
resource "aws_security_group" "aurora_sg" {
  #checkov:skip=CKV2_AWS_5: The aurora sg is attached to the aurora psql db instance.
  name        = "${module.aurora_db_label.id}-sg"
  description = "Security group for Aurora DB"
  vpc_id      = var.vpc_id
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    self        = true
    description = "Allow self-referencing access for Glue Connections"
  }
    ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "Allow access on port 5432 from 10.0.0.0/8"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/8"]
    description = "Allow egress traffic"
  }
}

variable "auroradbinstances" {
  description = "Configuration for each instance in the cluster."
  type = list(object({
    identifier     = string
    instance_class = string
  }))
  default = [
    {
      identifier     = "aurora-instance-1"
      instance_class = "db.r5.large"
    },
    {
      identifier     = "aurora-instance-2"
      instance_class = "db.r5.large"
    }
  ]
}

module "aurora" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source                 = "terraform-aws-modules/rds-aurora/aws"
  version                = "9.0.1"
  name                   = module.aurora_db_label.id
  engine                 = "aurora-postgresql"
  engine_version         = "15.4"
  instance_class         = "dbr5.large"
  instances              = var.auroradbinstances
  vpc_id                 = var.vpc_id
  subnets                = data.aws_subnets.private_dataware.ids
  #vpc_security_group_ids = [module.genai_glue_connection_security_group.id]
  vpc_security_group_ids = [
    module.genai_glue_connection_security_group.id,
    aws_security_group.aurora_sg.id
  ]
  # Enable delete protection
  # The below option has to set to false before deleting the Aurora DB
  deletion_protection = true
  # Additional configuration options
  apply_immediately    = true
  db_subnet_group_name = aws_db_subnet_group.aurora.id
  #db_parameter_group_name     = var.db_parameter_group_name
  monitoring_interval          = 60
  backup_retention_period      = 7
  preferred_backup_window      = "07:00-09:00"
  preferred_maintenance_window = "sun:05:00-sun:06:00"
  master_username              = "auroraadmin"
  manage_master_user_password  = true
  final_snapshot_identifier    = "${module.aurora_db_label.id}-final-snapshot"
  skip_final_snapshot          = false
  kms_key_id                   = module.aurora_kms_key.key_arn
  #iam_database_authentication_enabled = true
  storage_encrypted = true
  iam_roles = {
    "rds-service-role" = {
      role_arn     = module.rds_aurora_service_iam_role.arn
      feature_name = "s3Export"
    }
  }
}

output "cluster_master_user_secret" {
  value = module.aurora.cluster_master_user_secret
}
