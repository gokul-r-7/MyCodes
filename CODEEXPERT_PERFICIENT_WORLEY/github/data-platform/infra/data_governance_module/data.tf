data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# data "yaml" "config" {
#   source = "config/dev.yml"
# }