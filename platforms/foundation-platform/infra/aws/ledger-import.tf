# The ledger's move with its lanes (root ADR-0616 A1b, A4; coordinator 2026-11-28): a source's Bronze ledger rows go
# with its collector. The host exports the rows of the sources whose lanes moved and hold a Lakekeeper grant
# (config/lakekeeper-declaration.v1.json ledger_sources) once, to control/evidence/ledger_move/ (the host job
# bronze_ledger_export); this step, the rollout's third before the services move, imports every such export into the
# AWS ledger with the publisher's import-bronze-ledger-rows, which keeps what is there (matched by source id and
# object key, timestamps as they were), so it imports each row once and a later run imports nothing. A load on AWS
# plans from this ledger (plan-silver-load-objects), so its first run after the switch sees the history.
#
# Its role reads the foundation database's runtime secret and invokes only its own alias of the credential vendor,
# whose grant is read-only on the exports (collect.tf vendor_lanes). It runs the code location's image.

locals {
  ledger_import = "bronze_ledger_import"
}

resource "aws_lambda_alias" "ledger_import" {
  name             = local.ledger_import
  function_name    = aws_lambda_function.r2_vend.function_name
  function_version = aws_lambda_function.r2_vend.version
}

resource "aws_iam_role" "ledger_import" {
  name               = "${local.project}-ledger-import"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "ledger_import" {
  statement {
    sid       = "AskTheVendorForTheExports"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_alias.ledger_import.arn]
  }
  statement {
    sid       = "ReadTheDatabaseRole"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.foundation_runtime.arn]
  }
}

resource "aws_iam_role_policy" "ledger_import" {
  name   = "ledger-import"
  role   = aws_iam_role.ledger_import.id
  policy = data.aws_iam_policy_document.ledger_import.json
}

# It injects nothing: it writes its log.
resource "aws_iam_role" "ledger_import_execution" {
  name               = "${local.project}-ledger-import-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "ledger_import_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dagster.arn}:log-stream:ledger-import/*"]
  }
}

resource "aws_iam_role_policy" "ledger_import_execution" {
  name   = "ledger-import-execution"
  role   = aws_iam_role.ledger_import_execution.id
  policy = data.aws_iam_policy_document.ledger_import_execution.json
}

resource "aws_ecs_task_definition" "ledger_import" {
  family                   = "${local.project}-ledger-import"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  task_role_arn            = aws_iam_role.ledger_import.arn
  execution_role_arn       = aws_iam_role.ledger_import_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "ledger-import"
    image            = var.dagster_image
    essential        = true
    command          = ["python", "orchestration/dagster/aws/collect_lane.py", "import-ledger"]
    environment      = [{ name = "AWS_REGION", value = local.contract.region }]
    logConfiguration = merge(local.one_off_logs, { options = merge(local.one_off_logs.options, { awslogs-stream-prefix = "ledger-import" }) })
  }])
}

# After the migrations (the ledger's tables are there) and before the services move (definitions.py's lanes plan from
# this ledger). A new image or a new export set runs it; a failed one stays tainted and fails the apply.
resource "terraform_data" "ledger_import" {
  triggers_replace = {
    definition = aws_ecs_task_definition.ledger_import.revision
    sources    = jsonencode(jsondecode(file("${path.module}/../../config/lakekeeper-declaration.v1.json")).ledger_sources)
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = "${path.module}/pre-deploy.sh"
    environment = {
      REGION          = local.contract.region
      CLUSTER         = aws_ecs_cluster.platform.name
      SUBNETS         = join(",", [for subnet in aws_subnet.public : subnet.id])
      SECURITY_GROUP  = aws_security_group.tasks.id
      TASK_DEFINITION = "${aws_ecs_task_definition.ledger_import.family}:${aws_ecs_task_definition.ledger_import.revision}"
      WAIT_MINUTES    = tostring(local.database.one_off_wait_minutes)
    }
  }
  depends_on = [terraform_data.database_migrate, aws_iam_role_policy.ledger_import, aws_iam_role_policy.ledger_import_execution]
}

output "ledger_import_task_definition" {
  value = aws_ecs_task_definition.ledger_import.family
}
