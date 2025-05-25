provider "aws" {
  region              = var.aws_region
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]
  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = var.environment
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}

provider "aws" {
  region              = "us-east-2"
  alias               = "us-east-2"
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]
  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = var.environment
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}
###
# Providers for Dev, QA, and Prod environments
provider "aws" {
  region              = var.aws_region
  alias               = "dev"
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]

  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = "dev"
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}

provider "aws" {
  region              = var.aws_region
  alias               = "qa"
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]


  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = "qa"
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}

provider "aws" {
  region              = var.aws_region
  alias               = "prd"
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]

  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = "prd"
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}

provider "aws" {
  alias = "sydney"
  region = "ap-southeast-2"
  shared_config_files = [var.tfc_aws_dynamic_credentials.default.shared_config_file]
  default_tags {
    tags = merge(var.tags, {
      Namespace    = var.namespace
      Environment  = var.environment
      Stage        = var.stage
      BusinessUnit = var.business_unit
      Terraform    = "true"
      map-migrated = "mig1UWCZQT1RR"
    })
  }
}