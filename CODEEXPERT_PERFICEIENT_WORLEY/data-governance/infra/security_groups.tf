module "rbac_redshift_lambda_sg" {
  source      = "terraform-aws-modules/security-group/aws"
  version     = "5.1.2"
  name        = "rbac_redshift_lambda_sg"
  description = "Security group for rbac resources"
  vpc_id      = var.vpc_id

  ingress_with_self = [
    {
      rule = "all-all"
    }
  ]

  ingress_with_cidr_blocks = [

    {
      from_port   = 5439
      to_port     = 5439
      protocol    = "tcp"
      description = "Allow Redshift access from private IPs"
      cidr_blocks = "10.0.0.0/8"
    }
  ]

  # allow egress for ipv4
  egress_with_cidr_blocks = [
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      description = "Allow All Egress"
      cidr_blocks = "10.0.0.0/8"
    },
    {
      #TODO Remove after endpoint(s) 
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      description = "Allow All Egress"
      cidr_blocks = "0.0.0.0/0"
    },
  ]

}