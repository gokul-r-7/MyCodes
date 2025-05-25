data "aws_caller_identity" "current_ohio" { provider = aws.us-east-2 }
data "aws_availability_zones" "available_ohio" { provider = aws.us-east-2 }
data "aws_region" "current_ohio" { provider = aws.us-east-2 }

data "aws_vpc" "ohio_selected" {
  provider = aws.us-east-2
  id       = var.ohio_vpc_id
}

data "aws_subnets" "ohio_private_application" {
  provider = aws.us-east-2
  filter {
    name   = "vpc-id"
    values = [var.ohio_vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available_ohio.names[0]}", "${data.aws_availability_zones.available_ohio.names[1]}"]
  }

  tags = {
    Tier = "application"
  }
}

data "aws_subnets" "ohio_private_dataware" {

  provider = aws.us-east-2

  filter {
    name   = "vpc-id"
    values = [var.ohio_vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available_ohio.names[0]}", "${data.aws_availability_zones.available_ohio.names[1]}"]
  }

  tags = {
    Tier = "dataware"
  }
}

data "aws_subnet" "ohio_private_dataware" {

  provider = aws.us-east-2

  id = tolist(data.aws_subnets.ohio_private_dataware.ids)[0]
}

data "aws_subnets" "ohio_private_glue" {

  provider = aws.us-east-2

  filter {
    name   = "vpc-id"
    values = [var.ohio_vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available_ohio.names[0]}", "${data.aws_availability_zones.available_ohio.names[1]}"]
  }

  tags = {
    Tier = "glue"
  }
}

data "aws_subnet" "ohio_private_glue" {
  provider = aws.us-east-2
  id       = tolist(data.aws_subnets.ohio_private_glue.ids)[0]
}

data "aws_subnets" "ohio_private_transit" {
  provider = aws.us-east-2
  filter {
    name   = "vpc-id"
    values = [var.ohio_vpc_id]
  }

  filter {
    name   = "availability-zone"
    values = ["${data.aws_availability_zones.available_ohio.names[0]}", "${data.aws_availability_zones.available_ohio.names[1]}"]
  }

  tags = {
    Tier = "transit"
  }
}

