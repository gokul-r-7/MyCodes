module "ec2_instance_profile_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = distinct(compact(concat(module.label.attributes, ["profile"])))

  context = module.label.context
}

resource "aws_iam_role" "ec2" {
  name                = module.ec2_instance_profile_label.id
  assume_role_policy  = data.aws_iam_policy_document.ec2_assume_role.json
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"]
  tags                = module.ec2_instance_profile_label.tags
}

# https://github.com/hashicorp/terraform-guides/tree/master/infrastructure-as-code/terraform-0.13-examples/module-depends-on
resource "aws_iam_instance_profile" "ec2_role_profile" {
  name = module.ec2_instance_profile_label.id
  role = aws_iam_role.ec2.name
}

module "ec2_instance_ssm_parameter_prefix_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  delimiter   = "/"
  namespace   = var.namespace
  environment = var.environment
  stage       = var.stage
  name        = var.name

  attributes = ["ec2", "access", "airflow"]
}

#module "ec2_key_pair" {
#  source  = "cloudposse/key-pair/aws"
#  version = "0.20.0"
#  context = module.label.context

#  generate_ssh_key      = true
#  private_key_extension = "*.pem"
#  public_key_extension  = "*.pub"
#  ssh_public_key_path   = "/secrets"
#}


# data "aws_ami" "windows_2022" {
#   owners      = ["801119661308"]
#   most_recent = true

#   filter {
#     name   = "name"
#     values = ["*WS2022*"]
#   }

#   filter {
#     name   = "architecture"
#     values = ["x86_64"]
#   }

#   filter {
#     name   = "virtualization-type"
#     values = ["hvm"]
#   }
# }

module "instance" {
  source  = "cloudposse/ec2-instance/aws"
  version = "1.4.1"

  #ssh_key_pair = module.ec2_key_pair.key_name
  vpc_id       = var.vpc_id
  # subnet       = data.aws_subnet.private_dataware.id
  subnet       = var.ec2_subnet_id

  ami               = var.ami_id
  availability_zone = data.aws_availability_zones.available.names[0]
  ebs_volume_type   = "gp3"
  ebs_volume_size   = 30
  root_volume_size  = 100

  instance_profile = aws_iam_instance_profile.ec2_role_profile.name
  instance_type    = "t3.large"
  security_group_rules = [
    {
      type        = "egress"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = [data.aws_vpc.selected.cidr_block]
    },
    {
      type        = "ingress"
      from_port   = 3389
      to_port     = 3389
      protocol    = "tcp"
      cidr_blocks = [data.aws_vpc.selected.cidr_block]
    }
  ]

  depends_on = [aws_iam_instance_profile.ec2_role_profile]

  context    = module.label.context
  attributes = ["ec2", "access", "airflow"]
}


# DATASYNC

# data "aws_ami" "datasync_agent" {
#   most_recent = true

#   filter {
#     name   = "name"
#     values = ["aws-datasync-*"]
#   }

#   owners = ["487073908102"] # AMZN
# }
