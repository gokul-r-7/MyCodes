module "sns_topic_data_platform_notification" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/sns-topic/aws"
  version = "0.21.0"

  name                  = "data-platform-notification-topic"
  fifo_queue_enabled    = false
  sns_topic_policy_json = data.aws_iam_policy_document.sns_policy.json

  attributes = ["sns", "topic", "data", "platform", "notification"]
  context    = module.label.context

  kms_master_key_id = module.kms_key.key_id

}

#checkov:skip=CKV_AWS_283:Principals are restricted by source account
data "aws_iam_policy_document" "sns_policy" {
  statement {
    sid = "sns_policy_1"
    actions = [
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:Subscribe"
    ]
    resources = [module.sns_topic_data_platform_notification.sns_topic_arn]
    condition {
      test     = "StringEquals"
      values   = ["AWS:SourceAccount"]
      variable = data.aws_caller_identity.current.account_id
    }
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }

  statement {
    sid       = "sns_policy_2"
    actions   = ["SNS:Subscribe"]
    resources = [module.sns_topic_data_platform_notification.sns_topic_arn]
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }
}


