variable "emr_studio_admin_group" {
  type = string
}

variable "aws_region" {
  description = "AWS region to create resources in"
  type        = string
  validation {
    condition     = contains(["ap-southeast-2", "us-east-2"], var.aws_region)
    error_message = "Invalid AWS region value. Allowed values are ap-southeast-2 and us-east-2."
  }
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "kms_key_id" {
  type        = string
  description = "kms_key_id"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs"
}

variable "label_context" {
  type = any
}

variable "environment" {
  description = "ID element. Usually where resources will be deployed 'prd', 'qa', or 'dev'"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prd"], var.environment)
    error_message = "Invalid environment value. Allowed values are dev, qa, or prd."
  }
}

variable "account_id" {
  type        = string
  description = "AWS Account ID"
}

variable "athena_workgroup" {
  type        = string
  description = "Athena Workgroup name"
}

variable "results_bucket" {
  type        = string
  description = "Athena results bucket name"
}

