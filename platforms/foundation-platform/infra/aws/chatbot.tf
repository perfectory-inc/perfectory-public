# Urgent alarms reach Slack (owner, 2026-11-20) through AWS Chatbot, now Amazon Q Developer in chat
# applications (owner, 2026-11-26): the alarm topic of dagster.tf is bound to one Slack channel. No token or
# webhook is stored anywhere. The owner authorizes the Slack workspace once in the console
# (docs/runbooks/aws-platform.md); which alarms reach Slack is which alarms publish to the topic, in code.
# The service has no control plane in this account's Region, so the configuration is managed through the
# contract's control Region; the topic stays here (config/aws-platform.contract.json, alarms.chatbot).

variable "slack_team_id" {
  description = "The Slack workspace the owner authorized for AWS Chatbot. Given at apply time (TF_VAR_slack_team_id), not committed: the runbook reads it back from AWS."
  type        = string
  sensitive   = true
  validation {
    condition     = can(regex("^T[A-Z0-9]{8,}$", var.slack_team_id))
    error_message = "A Slack workspace id starts with T."
  }
}

variable "slack_channel_id" {
  description = "The one Slack channel the alarms go to. Given at apply time (TF_VAR_slack_channel_id), not committed: the runbook reads it back from AWS."
  type        = string
  sensitive   = true
  validation {
    condition     = can(regex("^[CG][A-Z0-9]{8,}$", var.slack_channel_id))
    error_message = "A Slack channel id starts with C (G for an old private channel)."
  }
}

locals {
  chatbot = local.contract.alarms.chatbot
}

data "aws_iam_policy_document" "chatbot_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["chatbot.amazonaws.com"]
    }
  }
}

# The channel may do what an alarm notification needs: read the alarm and its metric to draw it (the
# service's "Notification permissions" template). The same policy is the channel's guardrail, so no role can
# do more from the channel; with no guardrail the service applies AdministratorAccess.
data "aws_iam_policy_document" "chatbot_notifications" {
  statement {
    sid       = "ReadTheAlarmItShows"
    actions   = ["cloudwatch:Describe*", "cloudwatch:Get*", "cloudwatch:List*"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "chatbot_notifications" {
  name   = "${local.chatbot.configuration_name}-notifications"
  policy = data.aws_iam_policy_document.chatbot_notifications.json
}

resource "aws_iam_role" "chatbot" {
  name               = "${local.chatbot.configuration_name}-chatbot"
  assume_role_policy = data.aws_iam_policy_document.chatbot_assume.json
}

resource "aws_iam_role_policy_attachment" "chatbot" {
  role       = aws_iam_role.chatbot.name
  policy_arn = aws_iam_policy.chatbot_notifications.arn
}

# The rule for what goes through it (coordinator, 2026-11-26): the service may store and process a
# notification in any of its four Regions, outside ap-northeast-2. So only alarms whose message carries no
# personal or business data publish to the topic: metadata such as "Dagster not running" (an alarm's name,
# state, metric and the platform's own resource names). An alarm that would carry data does not use this topic.
resource "aws_chatbot_slack_channel_configuration" "alarms" {
  region                = local.chatbot.control_region
  configuration_name    = local.chatbot.configuration_name
  iam_role_arn          = aws_iam_role.chatbot.arn
  slack_team_id         = var.slack_team_id
  slack_channel_id      = var.slack_channel_id
  sns_topic_arns        = [aws_sns_topic.alarms.arn]
  guardrail_policy_arns = [aws_iam_policy.chatbot_notifications.arn]
  # A command from the channel would need the member's own role, and none is given: the channel only receives.
  user_authorization_required = true
  # Its logs would go to us-east-1; the runbook checks delivery with one forced alarm instead.
  logging_level = "NONE"
}
