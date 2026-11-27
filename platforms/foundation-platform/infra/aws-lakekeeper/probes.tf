# The probes: one task definition per principal (and one with no identity at all), each running probes/probe.py
# from inside the VPC as that principal. A principal's client secret is injected by its own execution role only,
# so a probe can act as no one but itself: the shape a lane's run would have. The probe's task has no role: it
# calls nothing of AWS. probe.sh runs one step and prints its result lines (they never hold a token or a secret).
# Probes and their names live only where the contract says so (probes: dev).
#
# The rust probe (T8, and a step of the upgrade canary) is the release's own publisher: the AWS code-location
# image, by digest, given at apply time from the commit's verified record (scripts/deploy/aws-release-images.py
# writes it as dagster_image). It runs check-lakehouse-catalog-writes as the operator, with no storage key: the
# writer path of that very binary against this catalog. With no image given there is no rust probe.


locals {
  probe_image = replace("public.ecr.aws/docker/library/python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b", "/:[^:/@]+@/", "@")
  # A probe acts as one principal. anonymous has no client at all (T6: a call with no token). warehouse is the
  # operator with the warehouse's R2 credential too: the one task that hands it to Lakekeeper (warehouse.tf).
  probes = !local.env.probes ? {} : merge(
    { for name, principal in local.env.identity.principals : name => {
      principal = name, secret = aws_secretsmanager_secret.principal[name].arn, declared = principal.declared, r2 = false
    } },
    { anonymous = { principal = "anonymous", secret = null, declared = false, r2 = false } },
    { warehouse = { principal = local.operator, secret = aws_secretsmanager_secret.principal[local.operator].arn, declared = true, r2 = true } },
    { for name in var.code_location_image == null ? [] : ["rust"] : name => {
      principal = local.operator, secret = aws_secretsmanager_secret.principal[local.operator].arn, declared = true, r2 = false
    } },
  )
  # A probe's program comes in its environment, gzip-compressed and base64-encoded (OpenTofu's base64gzip): the shared
  # part PROBE_COMMON (probes/common.py), made the module `common`, and the task's steps PROBE_STEPS (dev:
  # probes/probe.py, prod: probes/acceptance.py), which import from it and run. One ECS container definition holds
  # at most 64 KiB: dev's steps inline already took 63 KB of it, and with prod's steps added 77 KB was refused
  # (RegisterTaskDefinition, 2026-11-28). The loader is the entryPoint, which a run's overrides cannot replace, and it
  # pins each file's SHA-256: an environment overridden at run time (ecs:RunTask containerOverrides) is refused, so
  # the code a task runs is always its definition's.
  probe_program = { for steps in ["probe.py", "acceptance.py"] : steps => join("\n", [
    "import base64, gzip, hashlib, os, sys, types",
    "def source(name, pinned):",
    "    text = gzip.decompress(base64.b64decode(os.environ.pop(name)))",
    "    if hashlib.sha256(text).hexdigest() != pinned:",
    "        sys.exit(f'{name} is not the program this task definition pins')",
    "    return text.decode()",
    "common = types.ModuleType('common')",
    "exec(compile(source('PROBE_COMMON', '${sha256(file("${path.module}/probes/common.py"))}'), 'common.py', 'exec'), common.__dict__)",
    "sys.modules['common'] = common",
    "exec(compile(source('PROBE_STEPS', '${sha256(file("${path.module}/probes/${steps}"))}'), '${steps}', 'exec'), {'__name__': '__main__'})",
  ]) }
  probe_environment = {
    PROBE_COMMON = base64gzip(file("${path.module}/probes/common.py"))
    CATALOG_URL  = local.catalog_url
    TOKEN_URL    = local.token_url
    SCOPE        = local.catalog_scope
    OTHER_SCOPE  = local.env.identity.other_scope == null ? "" : "${aws_cognito_resource_server.catalog.identifier}/${local.env.identity.other_scope}"
    PRINCIPALS   = jsonencode({ for name, client in aws_cognito_user_pool_client.principal : name => client.id })
    WAREHOUSE    = jsonencode(local.env.warehouse)
  }
}

# dev's probes, or prod's acceptance task: the group the catalog lets in besides the reconciler.
resource "aws_security_group" "probe" {
  count       = local.env.probes || local.acceptance ? 1 : 0
  name        = "${local.name}-probe"
  description = local.env.probes ? "Lakekeeper dev rig probes: no inbound" : "${local.env.title} acceptance task: no inbound"
  vpc_id      = data.aws_vpc.platform.id
}

# Out: the token endpoint, image pulls, PyPI, Basin and R2 over TLS; the catalog's port. DuckDB's extension
# repository, which DuckDB fetches over plain HTTP, only for dev's probes: prod's acceptance task loads the
# extension files its image carries (root ADR-0559 §6).
resource "aws_vpc_security_group_egress_rule" "probe" {
  for_each = !(local.env.probes || local.acceptance) ? {} : merge(
    {
      https   = { port = 443, cidr = "0.0.0.0/0", group = null }
      catalog = { port = local.catalog.port, cidr = null, group = aws_security_group.catalog.id }
    },
    local.env.probes ? { http = { port = 80, cidr = "0.0.0.0/0", group = null } } : {},
  )
  security_group_id            = aws_security_group.probe[0].id
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
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:probe/${each.key}/*"]
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
  for_each                 = { for name, probe in local.probes : name => probe if name != "rust" }
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
    entryPoint = ["python", "-I", "-c", local.probe_program["probe.py"]]
    command    = ["health"]
    environment = [for key, value in merge(local.probe_environment, {
      PROBE_STEPS = base64gzip(file("${path.module}/probes/probe.py"))
      PRINCIPAL   = each.value.principal
      DECLARED    = tostring(each.value.declared)
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

# The publisher's own posture on AWS: its code-location runs keep a writable root file system (infra/aws).
resource "aws_ecs_task_definition" "rust_probe" {
  for_each                 = { for name, probe in local.probes : name => probe if name == "rust" }
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
  container_definitions = jsonencode([{
    name       = each.key
    image      = var.code_location_image
    essential  = true
    entryPoint = ["/opt/foundation-platform/release/foundation-outbox-publisher"]
    command    = ["check-lakehouse-catalog-writes"]
    environment = [for key, value in {
      LAKEHOUSE_CHECK_CATALOG_URI = "${local.catalog_url}/catalog"
      LAKEHOUSE_CHECK_WAREHOUSE   = local.env.warehouse.bucket
      LAKEHOUSE_CHECK_TOKEN_URI   = local.token_url
      LAKEHOUSE_CHECK_SCOPE       = local.catalog_scope
      LAKEHOUSE_CHECK_TABLE       = "silver.w109_rust_smoke"
    } : { name = key, value = value }]
    secrets = [
      { name = "LAKEHOUSE_CHECK_CLIENT_ID", valueFrom = "${each.value.secret}:client_id::" },
      { name = "LAKEHOUSE_CHECK_CLIENT_SECRET", valueFrom = "${each.value.secret}:client_secret::" },
    ]
    readonlyRootFilesystem = false
    logConfiguration       = local.logs["probe"]
  }])
  depends_on = [aws_secretsmanager_secret_version.principal]
}

moved {
  from = aws_security_group.probe
  to   = aws_security_group.probe[0]
}

output "probe_security_group" {
  value = one(aws_security_group.probe[*].id)
}
