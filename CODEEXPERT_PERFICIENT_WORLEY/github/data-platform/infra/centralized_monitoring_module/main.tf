resource "aws_cloudformation_stack" "linkaccounts" {
  name = "linkaccounts"
  
  # Read the template from the local file
  template_body = file("${path.module}/cfn/link_to_non_prod.yml")
  
  # Enable stack termination protection (optional but recommended for production)
  capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]
  
  # Add tags if needed
  tags = {
    Environment = var.environment
    Purpose     = "Centralized Monitoring Account"
  }
  
  # If your template requires parameters, you can specify them like this:
  # parameters = {
  #   ParameterKey1 = "ParameterValue1"
  #   ParameterKey2 = "ParameterValue2"
  # }
}

# Optionally, you can add outputs to get stack information
output "stack_id" {
  value = aws_cloudformation_stack.linkaccounts.id
}

output "stack_outputs" {
  value = aws_cloudformation_stack.linkaccounts.outputs
}