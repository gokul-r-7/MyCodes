variable "environment" {
  description = "ID element. Usually where resources will be deployed 'prd', 'qa', or 'dev'"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prd"], var.environment)
    error_message = "Invalid environment value. Allowed values are dev, qa, or prd."
  }
}