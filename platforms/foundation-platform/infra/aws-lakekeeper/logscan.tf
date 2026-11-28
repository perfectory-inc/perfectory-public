# T22's scanner: a task that reads the rig's log group and counts lines holding a credential (probes/log-scan.sh).
# Its execution role injects every secret of the rig but the warehouse's R2 credential, whose slot keeps one reader
# (warehouse.tf); its task role may read this log group and nothing else. It runs as probe.sh logscan <minutes>.

locals {
  logscan_image = replace("public.ecr.aws/aws-cli/aws-cli:2.42.5@sha256:4e569d37c0b93893f660946f9b00a442b1172b1c4ba86338a3a1af3937ed62b5", "/:[^:/@]+@/", "@")
  logscan_secrets = merge(
    {
      SECRET_CATALOG_DATABASE = "${aws_secretsmanager_secret.database["catalog"].arn}:password::"
      SECRET_AUTHZ_DATABASE   = "${aws_secretsmanager_secret.database["authz"].arn}:password::"
      SECRET_CATALOG_KEY      = "${aws_secretsmanager_secret.catalog_encryption_key.arn}:key::"
      SECRET_AUTHZ_KEY        = "${aws_secretsmanager_secret.authz_key.arn}:key::"
    },
    { for name, secret in aws_secretsmanager_secret.principal : "SECRET_CLIENT_${upper(replace(name, "-", "_"))}" => "${secret.arn}:client_secret::" },
  )
}

resource "aws_iam_role" "logscan_execution" {
  count              = local.env.probes ? 1 : 0
  name               = "${local.name}-probe-logscan-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "logscan_execution" {
  count = local.env.probes ? 1 : 0
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:probe/logscan/*"]
  }
  statement {
    sid       = "InjectTheSecretsItLooksFor"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = concat([for secret in aws_secretsmanager_secret.database : secret.arn], [for secret in aws_secretsmanager_secret.principal : secret.arn], [aws_secretsmanager_secret.catalog_encryption_key.arn, aws_secretsmanager_secret.authz_key.arn])
  }
}

resource "aws_iam_role_policy" "logscan_execution" {
  count  = local.env.probes ? 1 : 0
  name   = "logscan"
  role   = aws_iam_role.logscan_execution[0].id
  policy = data.aws_iam_policy_document.logscan_execution[0].json
}

resource "aws_iam_role" "logscan" {
  count              = local.env.probes ? 1 : 0
  name               = "${local.name}-probe-logscan"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "logscan" {
  count = local.env.probes ? 1 : 0
  statement {
    sid       = "ReadTheRigsLogs"
    actions   = ["logs:FilterLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:*"]
  }
}

resource "aws_iam_role_policy" "logscan" {
  count  = local.env.probes ? 1 : 0
  name   = "logscan"
  role   = aws_iam_role.logscan[0].id
  policy = data.aws_iam_policy_document.logscan[0].json
}

resource "aws_ecs_task_definition" "logscan" {
  count                    = local.env.probes ? 1 : 0
  family                   = "${local.name}-probe-logscan"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.logscan_execution[0].arn
  task_role_arn            = aws_iam_role.logscan[0].arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "logscan"
    image            = local.logscan_image
    essential        = true
    entryPoint       = ["bash", "-c", file("${path.module}/probes/log-scan.sh"), "log-scan"]
    command          = ["180"]
    environment      = [{ name = "LOG_GROUP", value = aws_cloudwatch_log_group.lakekeeper.name }, { name = "AWS_REGION", value = local.region }]
    secrets          = [for name in sort(keys(local.logscan_secrets)) : { name = name, valueFrom = local.logscan_secrets[name] }]
    logConfiguration = local.logs["probe"]
  }])
}

moved {
  from = aws_iam_role.logscan_execution
  to   = aws_iam_role.logscan_execution[0]
}

moved {
  from = aws_iam_role_policy.logscan_execution
  to   = aws_iam_role_policy.logscan_execution[0]
}

moved {
  from = aws_iam_role.logscan
  to   = aws_iam_role.logscan[0]
}

moved {
  from = aws_iam_role_policy.logscan
  to   = aws_iam_role_policy.logscan[0]
}

moved {
  from = aws_ecs_task_definition.logscan
  to   = aws_ecs_task_definition.logscan[0]
}
