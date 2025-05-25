variable "aws_region" {
  description = "AWS region to create resources in"
  type        = string
  validation {
    condition     = contains(["ap-southeast-2", "us-east-2"], var.aws_region)
    error_message = "Invalid AWS region value. Allowed values are ap-southeast-2 and us-east-2."
  }
}

variable "stage" {
  description = "ID element. Usually used for AWS Regions e.g. 'ohio' from 'us-east-2' (Ohio), or 'sydney' from 'ap-southeast-2' (Sydney)"
  type        = string
  validation {
    condition     = contains(["ohio", "sydney"], var.stage)
    error_message = "Invalid AWS region value. Allowed values are ohio, sydney."
  }
}

variable "environment" {
  description = "ID element. Usually where resources will be deployed 'prd', 'qa', or 'dev'"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prd", "test"], var.environment)
    error_message = "Invalid environment value. Allowed values are dev, qa, or prd."
  }
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "vpc_subnet_ids" {
  type        = list(string)
  description = "VPC ID"
}

variable "support_email" {
  type        = string
  description = "email address of the production support team"
}

variable "glue_role_name" {
  type        = string
  description = "glue execution role name"
}

variable "mwaa_role_name" {
  type        = string
  description = "mwaa execution role name"
}

variable "tfc_role_name" {
  type        = string
  description = "tfc role name"
}

variable "account_admin_role" {
  type        = string
  description = "human who can administer lakeformation"
}

variable "root_account_id" {
  type        = string
  description = "root account ID where identity center lives"
}

variable "rbac_v2" {
  type        = bool
  description = "enable for RBAC v2"
  default     = false
}

variable "cluster_identifier" {
  type        = string
  description = "redshift cluster ID"
}