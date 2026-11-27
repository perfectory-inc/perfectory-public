# Prod's acceptance task (root ADR-0624 A1, decisions 4-4 and 4-7; the contract's acceptance): the A1 tests that
# need prod, T13 at 50 concurrent on prod's own database and T17 (the migration method), as one-off runs of
# probes/acceptance.py (with probes/common.py). probe.sh runs one step (LAKEKEEPER_ENVIRONMENT=prod ./probe.sh operator <step> ...).
#
# It is the release's own code-location image by digest (var.code_location_image), so it exists only when an apply
# gives one, like the reconciler. Its Python takes the pinned duckdb wheel by hash and loads the DuckDB extension
# files that image carries, by path: nothing is downloaded at run time, and its group has no plain-HTTP way out
# (root ADR-0559 §6). It runs under the reconciler's execution role, the one role that reads the warehouse
# credential (coordinator 2026-11-28, option b): it adds no reader to that secret and no secret. T17 attaches Basin
# READ_ONLY with that credential (an Admin Read & Write token of the account, which Basin also accepts); this read
# path goes with Basin at A9. Its tables live in the contract's namespace (probe) only, and its drop step removes
# that namespace with every table and file in it right after.

locals {
  acceptance = local.env.acceptance != null && var.code_location_image != null
}

data "aws_iam_policy_document" "acceptance_logs" {
  count = local.acceptance ? 1 : 0
  statement {
    sid       = "WriteTheAcceptanceLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:probe/operator/*"]
  }
}

resource "aws_iam_role_policy" "acceptance_logs" {
  count  = local.acceptance ? 1 : 0
  name   = "acceptance-logs"
  role   = aws_iam_role.reconciler_execution.id
  policy = data.aws_iam_policy_document.acceptance_logs[0].json
}

resource "aws_ecs_task_definition" "acceptance" {
  count                    = local.acceptance ? 1 : 0
  family                   = "${local.name}-probe-operator"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = local.env.acceptance.cpu
  memory                   = local.env.acceptance.memory
  execution_role_arn       = aws_iam_role.reconciler_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name       = "operator"
    image      = var.code_location_image
    essential  = true
    entryPoint = ["python", "-I", "-c", local.probe_program["acceptance.py"]]
    command    = ["health"]
    environment = [for key, value in merge(local.probe_environment, {
      PROBE_STEPS          = base64gzip(file("${path.module}/probes/acceptance.py"))
      PRINCIPAL            = local.operator
      DECLARED             = "true"
      ACCEPTANCE_NAMESPACE = local.env.acceptance.namespace
      WORK                 = "/tmp/acceptance"
    }) : { name = key, value = value }]
    secrets = [
      { name = "CLIENT_ID", valueFrom = "${aws_secretsmanager_secret.principal[local.operator].arn}:client_id::" },
      { name = "CLIENT_SECRET", valueFrom = "${aws_secretsmanager_secret.principal[local.operator].arn}:client_secret::" },
      { name = "R2_CREDENTIAL", valueFrom = aws_secretsmanager_secret.r2_warehouse_credential.arn },
    ]
    # The image's user writes only its own scratch (/tmp): pip's target, DuckDB's home and spill.
    readonlyRootFilesystem = false
    logConfiguration       = local.logs["probe"]
  }])
  depends_on = [aws_secretsmanager_secret_version.principal]
}
