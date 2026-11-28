# The reconciler (root ADR-0624 decision 10-4, P8, A4, A6): the publisher's reconcile-lakehouse-catalog, run as the
# operator, makes what the contract declares and checks that nothing else is there: it bootstraps Lakekeeper once,
# makes the warehouse (handing it the R2 credential, warehouse.tf), the namespaces, the declared tables (from their
# table contracts) and the lanes' grants, and fails (prod) or reports (dev, where probes make tables of their own)
# on drift. Lakekeeper's /management/v1 is called from here and nowhere else of the lanes.
#
# It is the release's own code-location image by digest (var.code_location_image), so it exists only when an apply
# gives one; the apply then runs it once per new definition, the way the migrations run, and a failed run fails the
# apply. It injects the warehouse credential, so until the owner puts that value the task cannot start: prod gets
# its image only after the owner's paste (docs/runbooks/lakekeeper.md).

locals {
  reconciler = local.env.reconciler
  # What the catalog holds: prod's is generated from the pipeline graph (scripts/catalog/declared_definitions.py,
  # root ADR-0626 §9), never a hand list; dev's contract declares its own (none: its probes make theirs).
  declared = try(local.reconciler.declaration, null) == null ? {
    tables = local.reconciler.tables
    lanes  = local.reconciler.lanes
  } : jsondecode(file("${path.module}/../../${local.reconciler.declaration}"))
  # Lakekeeper names an OIDC principal oidc~<subject>, and a Cognito machine token's subject is its client id.
  user_id = { for name, client in aws_cognito_user_pool_client.principal : name => "oidc~${client.id}" }
  reconciler_declaration = {
    warehouse = local.env.warehouse
    tables    = local.declared.tables
    lanes     = { for lane, grants in local.declared.lanes : local.user_id[lane] => grants }
    operator  = local.user_id[local.operator]
    drift     = local.reconciler.drift
  }
}

resource "aws_security_group" "reconciler" {
  name        = "${local.name}-reconciler"
  description = "${local.env.title} reconciler: no inbound"
  vpc_id      = data.aws_vpc.platform.id
}

# Out: the token endpoint and image pulls over TLS, the catalog's port.
resource "aws_vpc_security_group_egress_rule" "reconciler" {
  for_each = {
    https   = { port = 443, cidr = "0.0.0.0/0", group = null }
    catalog = { port = local.catalog.port, cidr = null, group = aws_security_group.catalog.id }
  }
  security_group_id            = aws_security_group.reconciler.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

resource "aws_iam_role" "reconciler_execution" {
  name               = "${local.name}-reconciler-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "reconciler_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:reconciler/*"]
  }
  statement {
    sid       = "InjectTheOperatorAndTheWarehouseCredential"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.principal[local.operator].arn, aws_secretsmanager_secret.r2_warehouse_credential.arn]
  }
}

resource "aws_iam_role_policy" "reconciler_execution" {
  name   = "reconciler"
  role   = aws_iam_role.reconciler_execution.id
  policy = data.aws_iam_policy_document.reconciler_execution.json
}

# The publisher's own posture on AWS: its code-location runs keep a writable root file system (infra/aws).
resource "aws_ecs_task_definition" "reconciler" {
  count                    = var.code_location_image == null ? 0 : 1
  family                   = "${local.name}-reconciler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.reconciler_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name       = "reconciler"
    image      = var.code_location_image
    essential  = true
    entryPoint = ["/opt/foundation-platform/release/foundation-outbox-publisher"]
    command    = ["reconcile-lakehouse-catalog"]
    environment = [for key, value in {
      LAKEHOUSE_RECONCILE_CATALOG_URL = local.catalog_url
      LAKEHOUSE_RECONCILE_TOKEN_URI   = local.token_url
      LAKEHOUSE_RECONCILE_SCOPE       = local.catalog_scope
      LAKEHOUSE_RECONCILE_DECLARATION = jsonencode(local.reconciler_declaration)
    } : { name = key, value = value }]
    secrets = [
      { name = "LAKEHOUSE_RECONCILE_CLIENT_ID", valueFrom = "${aws_secretsmanager_secret.principal[local.operator].arn}:client_id::" },
      { name = "LAKEHOUSE_RECONCILE_CLIENT_SECRET", valueFrom = "${aws_secretsmanager_secret.principal[local.operator].arn}:client_secret::" },
      { name = "LAKEHOUSE_RECONCILE_R2_CREDENTIAL", valueFrom = aws_secretsmanager_secret.r2_warehouse_credential.arn },
    ]
    readonlyRootFilesystem = false
    logConfiguration       = local.logs["reconciler"]
  }])
  depends_on = [aws_secretsmanager_secret_version.principal]
}

# Runs when its definition changes: a new image, or a new declaration. A failed run (drift in fail mode, a refused
# call) fails the apply and stays tainted, so the next apply runs it again.
resource "terraform_data" "reconcile" {
  count = var.code_location_image == null ? 0 : 1
  triggers_replace = {
    definition = aws_ecs_task_definition.reconciler[0].revision
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = local.one_off_script
    environment = {
      REGION          = local.region
      CLUSTER         = aws_ecs_cluster.lakekeeper.name
      SUBNETS         = join(",", local.subnets)
      SECURITY_GROUP  = aws_security_group.reconciler.id
      TASK_DEFINITION = "${aws_ecs_task_definition.reconciler[0].family}:${aws_ecs_task_definition.reconciler[0].revision}"
      WAIT_MINUTES    = "15"
    }
  }
  depends_on = [aws_ecs_service.catalog, aws_vpc_security_group_egress_rule.reconciler, aws_vpc_security_group_ingress_rule.catalog]
}
