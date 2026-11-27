# The probes: one task definition per principal (and one with no identity at all), each running probes/probe.py
# from inside the VPC as that principal. A principal's client secret is injected by its own execution role only,
# so a probe can act as no one but itself: the shape a lane's run would have. The probe's task has no role: it
# calls nothing of AWS. probe.sh runs one step and prints its result lines (they never hold a token or a secret).
# Probes and their names live only in dev.

locals {
  probe_image = replace("public.ecr.aws/docker/library/python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b", "/:[^:/@]+@/", "@")
  # A probe acts as one principal. anonymous has no client at all (T6: a call with no token). warehouse is the
  # operator with the warehouse's R2 credential too: the one task that hands it to Lakekeeper (warehouse.tf).
  probes = merge(
    { for name, principal in local.dev.identity.principals : name => {
      principal = name, secret = aws_secretsmanager_secret.principal[name].arn, declared = principal.declared, r2 = false
    } },
    { anonymous = { principal = "anonymous", secret = null, declared = false, r2 = false } },
    { warehouse = { principal = "bootstrap", secret = aws_secretsmanager_secret.principal["bootstrap"].arn, declared = true, r2 = true } },
  )
  probe_environment = {
    CATALOG_URL = local.catalog_url
    TOKEN_URL   = local.token_url
    SCOPE       = local.catalog_scope
    OTHER_SCOPE = "${aws_cognito_resource_server.catalog.identifier}/${local.dev.identity.other_scope}"
    PRINCIPALS  = jsonencode({ for name, client in aws_cognito_user_pool_client.principal : name => client.id })
    WAREHOUSE   = jsonencode(local.dev.warehouse)
  }
}

resource "aws_security_group" "probe" {
  name        = "${local.name}-probe"
  description = "Lakekeeper dev rig probes: no inbound"
  vpc_id      = data.aws_vpc.platform.id
}

# Out: the token endpoint, image pulls, PyPI and R2 over TLS; DuckDB's extension repository, which DuckDB fetches
# over plain HTTP (dev probes only: prod ships the extensions in the image, root ADR-0559 §6); the catalog's port.
resource "aws_vpc_security_group_egress_rule" "probe" {
  for_each = {
    https   = { port = 443, cidr = "0.0.0.0/0", group = null }
    http    = { port = 80, cidr = "0.0.0.0/0", group = null }
    catalog = { port = local.catalog.port, cidr = null, group = aws_security_group.catalog.id }
  }
  security_group_id            = aws_security_group.probe.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

resource "aws_iam_role" "probe_execution" {
  for_each           = local.probes
  name               = "${local.name}-probe-${each.key}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "probe_execution" {
  for_each = local.probes
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dev.arn}:log-stream:probe/${each.key}/*"]
  }
  dynamic "statement" {
    for_each = each.value.secret == null ? [] : [each.value.secret]
    content {
      sid       = "InjectItsOwnClientSecret"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [statement.value]
    }
  }
  dynamic "statement" {
    for_each = each.value.r2 ? [aws_secretsmanager_secret.r2_warehouse_credential.arn] : []
    content {
      sid       = "InjectTheWarehouseCredential"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_role_policy" "probe_execution" {
  for_each = local.probes
  name     = "probe"
  role     = aws_iam_role.probe_execution[each.key].id
  policy   = data.aws_iam_policy_document.probe_execution[each.key].json
}

resource "aws_ecs_task_definition" "probe" {
  for_each                 = local.probes
  family                   = "${local.name}-probe-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.probe_execution[each.key].arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  # Scratch space for the warehouse steps' pinned clients (the root file system stays read-only).
  volume {
    name = "work"
  }
  container_definitions = jsonencode([{
    name       = each.key
    image      = local.probe_image
    essential  = true
    entryPoint = ["python", "-I", "-c", file("${path.module}/probes/probe.py")]
    command    = ["health"]
    environment = [for key, value in merge(local.probe_environment, {
      PRINCIPAL = each.value.principal
      DECLARED  = tostring(each.value.declared)
    }) : { name = key, value = value }]
    secrets = concat(each.value.secret == null ? [] : [
      { name = "CLIENT_ID", valueFrom = "${each.value.secret}:client_id::" },
      { name = "CLIENT_SECRET", valueFrom = "${each.value.secret}:client_secret::" },
      ], each.value.r2 ? [
      { name = "R2_CREDENTIAL", valueFrom = aws_secretsmanager_secret.r2_warehouse_credential.arn },
    ] : [])
    readonlyRootFilesystem = true
    mountPoints            = [{ sourceVolume = "work", containerPath = "/work", readOnly = false }]
    logConfiguration       = local.logs["probe"]
  }])
  depends_on = [aws_secretsmanager_secret_version.principal]
}

output "probe_security_group" {
  value = aws_security_group.probe.id
}
