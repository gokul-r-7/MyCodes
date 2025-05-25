# modules/event-bridge/main.tf
# Source Event Bus (Data Platform Account)
resource "aws_cloudwatch_event_bus" "data_platform" {
  name =  "${module.label.id}-data-platform-event-bus"
}

# Event Rule in Source Account
resource "aws_cloudwatch_event_rule" "forward_to_system_integration" {
  name           = "forward-to-system-integration"
  event_bus_name = aws_cloudwatch_event_bus.data_platform.name
  event_pattern  = jsonencode(var.source_event_pattern)
}

# Dead Letter Queue
resource "aws_sqs_queue" "dlq" {
  name                      = "${module.label.id}-data-platform-dlq"
  message_retention_seconds = var.dlq_retention_days * 24 * 60 * 60
}

# IAM Role for Event Bridge to send events to target account
resource "aws_iam_role" "event_bridge_cross_account" {
  name = "${module.label.id}-event-bridge-cross-account"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for cross-account event publishing
resource "aws_iam_role_policy" "event_bridge_cross_account" {
  name = "${module.label.id}-event-bridge-cross-account-policy"
  role = aws_iam_role.event_bridge_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "events:PutEvents"
        ]
        Resource = [
          "arn:aws:events:${data.aws_region.current.name}:${var.sys_int_account_id}:event-bus/apse2-eip-${var.environment}-system-integration-event-bus"
        ]
      }
    ]
  })
}

# Target configuration
resource "aws_cloudwatch_event_target" "cross_account" {
  rule           = aws_cloudwatch_event_rule.forward_to_system_integration.name
  event_bus_name = aws_cloudwatch_event_bus.data_platform.name
  target_id      = "SendToSystemIntegration"
  arn           = "arn:aws:events:${data.aws_region.current.name}:${var.sys_int_account_id}:event-bus/apse2-eip-${var.environment}-system-integration-event-bus"
  role_arn      = aws_iam_role.event_bridge_cross_account.arn

  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

# DLQ Policy
resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sqs:SendMessage"
        Resource = aws_sqs_queue.dlq.arn
      }
    ]
  })
}

