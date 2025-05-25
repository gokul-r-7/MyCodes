resource "aws_sns_topic" "rbac-notifications" {
  name = "worley-mf-${var.stage}-${var.environment}-rbac-notifications"
}

resource "aws_sns_topic_subscription" "example_subscription" {
  topic_arn = aws_sns_topic.rbac-notifications.arn
  protocol  = "email"
  endpoint  = var.support_email
}

resource "aws_sns_topic" "modelling-dag-notifications" {
  name = "worley-${var.stage}-${var.environment}-modelling-dag-notifications"
}

resource "aws_sns_topic_subscription" "support_team_subscription" {
  topic_arn = aws_sns_topic.modelling-dag-notifications.arn
  protocol  = "email"
  endpoint  = var.support_email
}