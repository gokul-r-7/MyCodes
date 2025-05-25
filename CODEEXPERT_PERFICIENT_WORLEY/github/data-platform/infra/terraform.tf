terraform {
  cloud {
    organization = "W-GlobalDatacenterServices"

    workspaces {
      name = "data-platform-dev"
    }
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 5.53.0"
    }
  }

  required_version = ">= 1.8.1"
}