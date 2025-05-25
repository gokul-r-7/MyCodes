################################################################################
# API Gateway for Systems integration Ondemand Data fresh.
################################################################################
# API Gateway Features
## - Proxy integration
## - Authorizer = Lambda OAuth2 (Token based Auth)
## - Quotas/Throttling Enforce using API Keys
## - Private Endpoints, CustomDomain
# !!! IMPORTANT NOTE !!! 
# Safe guard on automatic merger on Prod branch is added by defning conditions 
# Using locals, apigw_environments and create_apigw_switch, the solution creates 
# only at defined environments. 
################################################################################

locals {
  apigw_environments = ["dev","qa"]
  create_apigw_switch   = contains(local.apigw_environments, var.environment) ? 1 : 0
}

module "api_gateway_label" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  count   = local.create_apigw_switch
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["apigw", "priv",var.environment,"app-integration"]
}

module "api_gateway_IAM_label" {
  count = local.create_apigw_switch
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["apigw", "app-integration"]
}

################################################################################
# IAM Role for APIGateway CloudWatch Logs
################################################################################
resource "aws_iam_role" "api_gateway_cloudwatch_role" {
  count = local.create_apigw_switch
  name  = "${module.api_gateway_IAM_label[0].id}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "api_gateway_cloudwatch_policy" {
  count      = local.create_apigw_switch
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
  role       = aws_iam_role.api_gateway_cloudwatch_role[0].name
}

resource "aws_cloudwatch_log_group" "rest_api_access_log_group" {
  count             = local.create_apigw_switch
  name_prefix       = "apigw_${var.environment}_priv_access_log_group"
  retention_in_days = local.retention_in_days
  kms_key_id        = module.kms_key.key_arn
}

resource "aws_api_gateway_account" "main" {
  count               = local.create_apigw_switch
  cloudwatch_role_arn = aws_iam_role.api_gateway_cloudwatch_role[0].arn
}

################################################################################
# APIGateway Resource configuration
################################################################################
resource "aws_api_gateway_rest_api" "rest_api" {
  count             = local.create_apigw_switch
  name              = module.api_gateway_label[0].id
  put_rest_api_mode = "merge"

  endpoint_configuration {
    types = ["PRIVATE"]
  }

  lifecycle {
    create_before_destroy = true
  }
}


# Create API KEY
resource "aws_api_gateway_api_key" "api_key" {
  count       = local.create_apigw_switch
  name        = "${module.api_gateway_label[0].id}-api-key"
  description = "API Key for ${module.api_gateway_label[0].id}"
}

# Create a Usage Plan 
resource "aws_api_gateway_usage_plan" "usage_plan" {
  count       = local.create_apigw_switch
  name        = "${module.api_gateway_label[0].id}-usage-plan"
  description = "Usage plan for API Gateway"

  api_stages {
    api_id = aws_api_gateway_rest_api.rest_api[0].id
    stage  = aws_api_gateway_stage.rest_api_stage[0].stage_name
  }

  quota_settings {
    limit  = 1000
    offset = 0
    period = "MONTH"
  }

  throttle_settings {
    burst_limit = 5
    rate_limit  = 10
  }

  depends_on = [aws_api_gateway_stage.rest_api_stage]
}

resource "aws_api_gateway_usage_plan_key" "usage_plan_key" {
  count         = local.create_apigw_switch
  key_id        = aws_api_gateway_api_key.api_key[0].id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.usage_plan[0].id
}

# Create API Gateway VPC Endpoint for Private API Gateway
resource "aws_vpc_endpoint" "api_gateway" {
  count             = local.create_apigw_switch
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.execute-api"
  vpc_endpoint_type = "Interface"
  subnet_ids = data.aws_subnets.private_application.ids
  security_group_ids = [aws_security_group.api_gateway_endpoint[0].id]
  private_dns_enabled = true
}

# Create a security group for the VPC Endpoint
resource "aws_security_group" "api_gateway_endpoint" {
  count       = local.create_apigw_switch
  name        = "${var.aws_region}-apigw-endpoint-sg-${var.name}-${var.environment}"
  description = "Security group for API Gateway VPC Endpoint"
  vpc_id      = var.vpc_id

  ingress {
    description = "Allow API Gateway traffic from VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    description = "Allow outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Create API Gateway Resource Policy
resource "aws_api_gateway_rest_api_policy" "private_api_policy" {
  count       = local.create_apigw_switch
  rest_api_id = aws_api_gateway_rest_api.rest_api[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "execute-api:Invoke"
        Resource  = "${aws_api_gateway_rest_api.rest_api[0].execution_arn}/*"
        Condition = {
          StringEquals = {
            "aws:SourceVpce" = aws_vpc_endpoint.api_gateway[0].id
          }
        }
      }
    ]
  })
}

################################################################################
# On demand Data refresh Processor Backend Lambda config - PROXY
################################################################################

resource "aws_api_gateway_resource" "rest_api_resource" {
  count       = local.create_apigw_switch
  parent_id   = aws_api_gateway_rest_api.rest_api[0].root_resource_id
  path_part   = "{proxy+}"
  rest_api_id = aws_api_gateway_rest_api.rest_api[0].id
}

# Start of Method with Lambda Authorizer
resource "aws_api_gateway_method" "rest_api_method" {
  count            = local.create_apigw_switch
  authorization    = "CUSTOM"
  authorizer_id    = aws_api_gateway_authorizer.lambda_authorizer[0].id
  http_method      = "ANY"
  resource_id      = aws_api_gateway_resource.rest_api_resource[0].id
  rest_api_id      = aws_api_gateway_rest_api.rest_api[0].id
  api_key_required = true
  request_validator_id = aws_api_gateway_request_validator.token_validator[0].id
}

###################################################################################
# Dac app integration api resource 
###################################################################################
resource "aws_api_gateway_resource" "rest_api_dac_resource" {
  count       = local.create_apigw_switch
  parent_id   = aws_api_gateway_rest_api.rest_api[0].root_resource_id
  path_part   = "dac"
  rest_api_id = aws_api_gateway_rest_api.rest_api[0].id
}

# Start of Method with Lambda Authorizer
resource "aws_api_gateway_method" "rest_api_dac_method" {
  count            = local.create_apigw_switch
  authorization    = "CUSTOM"
  authorizer_id    = aws_api_gateway_authorizer.lambda_authorizer[0].id
  http_method      = "ANY"
  resource_id      = aws_api_gateway_resource.rest_api_dac_resource[0].id
  rest_api_id      = aws_api_gateway_rest_api.rest_api[0].id
  api_key_required = true
  request_validator_id = aws_api_gateway_request_validator.token_validator[0].id
}

resource "aws_api_gateway_integration" "rest_api_dac_integration" {
  count                   = local.create_apigw_switch
  http_method             = aws_api_gateway_method.rest_api_method[0].http_method
  resource_id             = aws_api_gateway_resource.rest_api_dac_resource[0].id
  rest_api_id             = aws_api_gateway_rest_api.rest_api[0].id
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = "arn:aws:apigateway:${var.aws_region}:lambda:path/2015-03-31/functions/arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.aws_region}-apigw-dac-int-func-datalake-${var.environment}/invocations"


}





# Validate the token
resource "aws_api_gateway_request_validator" "token_validator" {
  count                       = local.create_apigw_switch
  name                        = "token-validator"
  rest_api_id                 = aws_api_gateway_rest_api.rest_api[0].id
  validate_request_parameters = true
}

resource "aws_api_gateway_model" "token_model" {
  count        = local.create_apigw_switch
  rest_api_id  = aws_api_gateway_rest_api.rest_api[0].id
  name         = "TokenModel"
  description  = "Validates the Authorization token"
  content_type = "application/json"

  schema = jsonencode({
    "$schema" = "http://json-schema.org/draft-04/schema#"
    type      = "object"
    properties = {
      Authorization = {
        type    = "string"
        pattern = "^Bearer [-0-9a-zA-Z\\._]*$"
      }
    }
    required = ["Authorization"]
  })
}


resource "aws_api_gateway_integration" "rest_api_integration" {
  count                   = local.create_apigw_switch
  http_method             = aws_api_gateway_method.rest_api_method[0].http_method
  resource_id             = aws_api_gateway_resource.rest_api_resource[0].id
  rest_api_id             = aws_api_gateway_rest_api.rest_api[0].id
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = aws_lambda_function.apigw_req_proc_func[0].invoke_arn
}

resource "aws_api_gateway_method_response" "rest_api_method_response_200" {
  count       = local.create_apigw_switch
  rest_api_id = aws_api_gateway_rest_api.rest_api[0].id
  resource_id = aws_api_gateway_resource.rest_api_resource[0].id
  http_method = aws_api_gateway_method.rest_api_method[0].http_method
  status_code = "200"

  response_models = {
    "application/json" = "Empty"  
  }

}

# End of Method configuration with Lambda Authorizer

resource "aws_lambda_permission" "rest_api_lambda" {
  count         = local.create_apigw_switch
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.apigw_req_proc_func[0].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "arn:aws:execute-api:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${aws_api_gateway_rest_api.rest_api[0].id}/*/*/*"
}

################################################################################
# The Common Deployment and stages for all resources.
################################################################################
resource "aws_api_gateway_deployment" "rest_api_deployment" {
  count       = local.create_apigw_switch
  rest_api_id = aws_api_gateway_rest_api.rest_api[0].id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.rest_api_resource[0].id,
      aws_api_gateway_method.rest_api_method[0].id,
      aws_api_gateway_integration.rest_api_integration[0].id,
      aws_api_gateway_method_response.rest_api_method_response_200[0].id,
      aws_api_gateway_rest_api_policy.private_api_policy[0].id,
      formatdate("YYYYMMDDhhmmss", timestamp()),
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_method.rest_api_method,
    aws_api_gateway_integration.rest_api_integration,
    aws_api_gateway_method_response.rest_api_method_response_200,
    aws_api_gateway_rest_api_policy.private_api_policy,
    aws_api_gateway_resource.rest_api_resource
  ]
}

resource "aws_api_gateway_stage" "rest_api_stage" {
  count                = local.create_apigw_switch
  depends_on           = [aws_vpc_endpoint.api_gateway, aws_cloudwatch_log_group.rest_api_access_log_group]
  deployment_id        = aws_api_gateway_deployment.rest_api_deployment[0].id
  rest_api_id          = aws_api_gateway_rest_api.rest_api[0].id
  stage_name           = var.environment
  xray_tracing_enabled = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.rest_api_access_log_group[0].arn
    format          = "$context.identity.sourceIp $context.identity.caller $context.identity.user [$context.requestTime] $context.httpMethod $context.resourcePath $context.protocol $context.status $context.responseLength $context.requestId"
  }
}

###############################################################################
# Lambda Authorizer for Azure AD Integration - JWT Auth Token
###############################################################################
resource "aws_api_gateway_authorizer" "lambda_authorizer" {
  count                  = local.create_apigw_switch
  name                   = "lambda-authorizer"
  rest_api_id            = aws_api_gateway_rest_api.rest_api[0].id
  authorizer_uri         = aws_lambda_function.apigw_lambda_authorizer[0].invoke_arn
  authorizer_credentials = aws_iam_role.api_gateway_authorizer_role[0].arn
  type                   = "TOKEN"
  identity_source        = "method.request.header.Authorization"
  authorizer_result_ttl_in_seconds = 10
  identity_validation_expression = "^Bearer [-0-9a-zA-Z\\._]*$"
}

resource "aws_iam_role" "api_gateway_authorizer_role" {
  count = local.create_apigw_switch
  name  = "${var.aws_region}-apigw-authorizer-role-${var.name}-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "api_gateway_authorizer_policy" {
  count = local.create_apigw_switch
  name  = "${var.aws_region}-apigw-authorizer-policy-${var.name}-${var.environment}"
  role  = aws_iam_role.api_gateway_authorizer_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = aws_lambda_function.apigw_lambda_authorizer[0].arn
      }
    ]
  })
}

################################################################################
# API Gateway Lambda Authorizer - Lambda function.
################################################################################

data "archive_file" "apigw_lambda_authorizer_zip" {
  count       = local.create_apigw_switch
  type        = "zip"
  source_file = "src/lambda/apigw_authorizer/apigw_lambda_authorizer.py"
  output_path = "src/lambda/apigw_authorizer/apigw_lambda_authorizer.zip"
}

# Lambda authorizer Layer
resource "aws_lambda_layer_version" "lambda_auth_layer" {
  filename   = "${path.module}/src/layers/lambdaAuthorizerLayer.zip"
  layer_name = "lambda-auth-layer"
  compatible_runtimes = [local.python_version] 
  source_code_hash  = filebase64sha256("src/layers/lambdaAuthorizerLayer.zip")
  description = "Lambda Authorizer Layer Base modules"
}

locals {
  dac_entra_client_id = contains(["dev","qa"], var.environment) ? "5abd6514-8c2b-4010-b7ea-e56e6b49be96" : ""
 }

resource "aws_cloudwatch_log_group" "apigw_lambda_authorizer" {
  count         = local.create_apigw_switch
  name              = "/aws/lambda/${var.aws_region}-apigw-lambda-authorizer-${var.name}-${var.environment}"
  retention_in_days = local.retention_in_days
}
# Create a lambda function
resource "aws_lambda_function" "apigw_lambda_authorizer" {
  count         = local.create_apigw_switch
  depends_on    = [aws_cloudwatch_log_group.apigw_lambda_authorizer]
  function_name = "${var.aws_region}-apigw-lambda-authorizer-${var.name}-${var.environment}"
  description   = "APIGateway Lambda Authorizer, integrated with Azure"
  handler       = "apigw_lambda_authorizer.handler"
  architectures = ["x86_64"]
  memory_size   = 128
  timeout       = 600
  role          = module.lambda_iam_role.arn
  runtime       = local.python_version
  layers      = [aws_lambda_layer_version.lambda_auth_layer.arn]
  tracing_config {
    mode = "PassThrough"
  }

  vpc_config {
    subnet_ids         = data.aws_subnets.private_glue.ids
    security_group_ids = [module.lambda_security_group.id]
  }

  kms_key_arn = module.kms_key.key_arn

  environment {
    variables = {
      ENTRA_CLIENT_ID = local.dac_entra_client_id
      ENTRA_TENANT_ID = var.entra_tenant_id
    }
  }

  filename         = data.archive_file.apigw_lambda_authorizer_zip[0].output_path
  source_code_hash = filebase64sha256("src/lambda/apigw_authorizer/apigw_lambda_authorizer.zip")
}


# ################################################################################
# # Private Custom Domain Name using CFN
# # Terraform providers support for Private Custom domain is unavailable at the 
# # time of writing this module.
# # Ref: https://github.com/hashicorp/terraform-provider-aws/issues/40274
# # Below configuration uses CloudFormation stack.
# ################################################################################
## Using CFN
resource "aws_cloudformation_stack" "api_gateway_custom_domain" {
  count = local.create_apigw_switch

  name = "apigw-custom-domain-${var.environment}-stack"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "API Gateway Custom Domain Configuration"

    Parameters = {
      CustDomainName = { Type = "String" }
      CertificateArn = { Type = "String" }
      VpceId         = { Type = "String" }
      RestApiId      = { Type = "String" }
      StageName      = { Type = "String" }
      AccountId      = { Type = "String" }
    }

    Resources = {
      DomainName = {
        Type = "AWS::ApiGateway::DomainNameV2"
        Properties = {
          CertificateArn = { Ref = "CertificateArn" }
          DomainName     = { Ref = "CustDomainName" }
          EndpointConfiguration = {
            Types = ["PRIVATE"]
          }
          Policy = { "Fn::Sub" = jsonencode({
            Statement = [
              {
                Effect    = "Deny"
                Principal = "*"
                Action    = "execute-api:Invoke"
                Resource  = ["execute-api:/*"]
                Condition = {
                  StringNotEquals = {
                    "aws:SourceVpce" = "$${VpceId}"
                  }
                }
              },
              {
                Effect    = "Allow"
                Principal = "*"
                Action    = "execute-api:Invoke"
                Resource  = ["execute-api:/*"]
                Condition = {
                  StringEquals = {
                    "aws:SourceVpce" = "$${VpceId}"
                  }
                }
              }
            ]
          }) }
          SecurityPolicy = "TLS_1_2"
        }
      }
      Mapping = {
        DependsOn = "DomainName"
        Type      = "AWS::ApiGateway::BasePathMappingV2"
        Properties = {
          DomainNameArn = { "Fn::GetAtt" = ["DomainName", "DomainNameArn"] }
          RestApiId     = { Ref = "RestApiId" }
          Stage         = { Ref = "StageName" }
        }
      }

      Association = {
        DependsOn = "DomainName"
        Type      = "AWS::ApiGateway::DomainNameAccessAssociation"
        Properties = {
          AccessAssociationSource     = { Ref = "VpceId" }
          AccessAssociationSourceType = "VPCE"
          DomainNameArn               = { "Fn::GetAtt" = ["DomainName", "DomainNameArn"] }
        }
      }
    }

    Outputs = {
      DomainNameId = {
        Description = "The ID of the created domain name"
        Value       = { "Fn::GetAtt" = ["DomainName", "DomainNameId"] }
      }
      DomainNameArn = {
        Description = "The ARN of the created domain name"
        Value       = { "Fn::GetAtt" = ["DomainName", "DomainNameArn"] }
      }
    }
  })

  parameters = {
    CustDomainName = var.apigw_domain_name
    CertificateArn = var.apigw_certificate_arn
    VpceId         = aws_vpc_endpoint.api_gateway[0].id
    RestApiId      = aws_api_gateway_rest_api.rest_api[0].id
    StageName      = aws_api_gateway_stage.rest_api_stage[0].stage_name
    AccountId      = data.aws_caller_identity.current.account_id
  }

  capabilities = ["CAPABILITY_IAM"]

  timeout_in_minutes = 30
}

output "domain_name_id" {
  value = var.environment == "dev" ? aws_cloudformation_stack.api_gateway_custom_domain[0].outputs["DomainNameId"] : null
}

output "domain_name_arn" {
  value = var.environment == "dev" ? aws_cloudformation_stack.api_gateway_custom_domain[0].outputs["DomainNameArn"] : null
}

###############################################################################
# Output 
###############################################################################

output "api_gateway_endpoint_network_interface_ids" {
  value = local.create_apigw_switch == 0 ? null : aws_vpc_endpoint.api_gateway[0].network_interface_ids
}

output "api_gateway_vpcendpoint_id" {
  value = local.create_apigw_switch == 0 ? null : aws_vpc_endpoint.api_gateway[0].id
}

################################################################################
# Backend Lambda function to handle data refresh requests
################################################################################

data "archive_file" "apigw_req_proc_zip" {
  count       = local.create_apigw_switch
  type        = "zip"
  source_dir  = "src/lambda/apigw_req_proc/"
  output_path = "src/lambda/apigw_req_proc/apigw_req_proc_function.zip"
}

# data "archive_file" "apigw_dac_int_zip" {
#   count       = local.create_apigw_switch
#   type        = "zip"
#   source_dir  = "${path.module}/src/lambda/apigw_dac_app_integration/"
#   output_path = "${path.module}/src/lambda/apigw_dac_app_integration/apigw_dac_app_integration.zip"
# }

resource "aws_cloudwatch_log_group" "apigw_req_proc_func" {
  count         = local.create_apigw_switch
  name              = "/aws/lambda/${var.aws_region}-apigw-req-proc-func-${var.name}-${var.environment}"
  retention_in_days = local.retention_in_days
}

# Create a lambda function
resource "aws_lambda_function" "apigw_req_proc_func" {
  depends_on    = [ aws_cloudwatch_log_group.apigw_req_proc_func ]
  count         = local.create_apigw_switch
  function_name = "${var.aws_region}-apigw-req-proc-func-${var.name}-${var.environment}"
  description   = "API Gateway On demand data refresh request processor Function"
  handler       = "apigw_lambda_dac_app_integration.lambda_handler"
  architectures = ["x86_64"]
  memory_size   = 128
  timeout       = 600
  role          = module.api_dac_lambda_iam_role[0].arn
  runtime       = local.python_version

  tracing_config {
    mode = "PassThrough"
  }

  vpc_config {
    subnet_ids         = data.aws_subnets.private_glue.ids
    security_group_ids = [module.lambda_security_group.id]
  }

  kms_key_arn = module.kms_key.key_arn

  environment {
    variables = {
      ENV = var.environment
    }
  }

  filename         = data.archive_file.apigw_req_proc_zip[0].output_path
  source_code_hash = filebase64sha256("src/lambda/apigw_req_proc/apigw_req_proc_function.zip")
}


module "apigw_dac_int_func" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"
  count         = local.create_apigw_switch
  function_name = "${var.aws_region}-apigw-dac-int-func-${var.name}-${var.environment}"
  description   = "Lambda function for dac integration"
  handler       = "apigw_lambda_dac_app_integration.lambda_handler"
  runtime       = local.python_version
  publish       = true
  memory_size   = 128
  architectures = ["x86_64"]

  cloudwatch_logs_retention_in_days  = local.retention_in_days

  source_path = "src/lambda/apigw_dac_app_integration"
  timeout     = 600

  create_role = false
  lambda_role   = module.api_dac_lambda_iam_role[0].arn

  vpc_security_group_ids = [module.lambda_security_group.id]
  vpc_subnet_ids = data.aws_subnets.private_glue.ids

  tracing_mode = "PassThrough"

  environment_variables = {
    ENV = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }
}


resource "aws_lambda_permission" "apigw_dac_int_func_policy" {
  count         = local.create_apigw_switch
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = module.apigw_dac_int_func[0].lambda_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "arn:aws:execute-api:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${aws_api_gateway_rest_api.rest_api[0].id}/*/*/*"
}
