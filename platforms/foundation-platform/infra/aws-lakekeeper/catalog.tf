# Lakekeeper, the catalog (upstream v0.15.1 docs: configuration, production, authentication, authorization-openfga).
# Two tasks behind catalog.<namespace>:<port>, the internal endpoint; `lakekeeper migrate` runs as a one-off task
# before the service moves (it also writes Lakekeeper's model into the OpenFGA store, so authz runs first), and
# `serve` refuses a database that is not migrated to its own version. Only its declared callers may call it (below).
#
# What fails closed here (the deploy refuses otherwise, T35): authorization is OpenFGA, never the default
# `allowall`; authentication is the Cognito pool, and a token is admitted only with the catalog scope,
# token_use=access and the client_id of a declared principal. No Console login is configured (no human issuer at
# launch, root ADR-0624 decision 10-7: Dawneer is the one console), and nothing outside the VPC reaches the port.

locals {
  catalog = local.env.catalog
  # The version contract's pin, as compose files spell it; the task runs it by the index digest.
  catalog_image = replace("quay.io/lakekeeper/catalog:v0.15.1@sha256:22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec", "/:[^:/@]+@/", "@")
  catalog_url   = "http://catalog.${aws_service_discovery_private_dns_namespace.lakekeeper.name}:${local.catalog.port}"
  # The issuer's own URI (Cognito: https://cognito-idp.<region>.amazonaws.com/<pool id>), whose
  # .well-known/openid-configuration Lakekeeper reads.
  issuer          = "https://${aws_cognito_user_pool.machines.endpoint}"
  catalog_scope   = "${aws_cognito_resource_server.catalog.identifier}/${local.env.identity.scope}"
  declared_client = [for name, principal in local.principals : aws_cognito_user_pool_client.principal[name].id if principal.declared]
  catalog_settings = {
    LAKEKEEPER__PG_HOST_W                 = local.database.address
    LAKEKEEPER__PG_PORT                   = tostring(local.database.port)
    LAKEKEEPER__PG_USER                   = local.databases.catalog.username
    LAKEKEEPER__PG_DATABASE               = local.databases.catalog.name
    LAKEKEEPER__PG_SSL_MODE               = "require"
    LAKEKEEPER__PG_READ_POOL_CONNECTIONS  = tostring(local.catalog.read_pool_connections)
    LAKEKEEPER__PG_WRITE_POOL_CONNECTIONS = tostring(local.catalog.write_pool_connections)
    LAKEKEEPER__LISTEN_PORT               = tostring(local.catalog.port)
    LAKEKEEPER__BASE_URI                  = local.catalog_url
    LAKEKEEPER__AUTHZ_BACKEND             = "openfga"
    LAKEKEEPER__OPENFGA__ENDPOINT         = "http://authz.${aws_service_discovery_private_dns_namespace.lakekeeper.name}:${local.authz.grpc_port}"
    LAKEKEEPER__OPENID_PROVIDER_URI       = local.issuer
    # Cognito's machine tokens name the app client in sub (and client_id); upstream asks for the claim to be set.
    LAKEKEEPER__OPENID_SUBJECT_CLAIM = "sub"
    LAKEKEEPER__OPENID_SCOPE         = local.catalog_scope
    # A Cognito access token has no aud: the required-claim rules bind it to the pool's access tokens of the
    # declared principals (upstream configuration "Required Claims").
    LAKEKEEPER__OPENID_REQUIRED_CLAIMS__TOKENUSE__CLAIM  = "token_use"
    LAKEKEEPER__OPENID_REQUIRED_CLAIMS__TOKENUSE__ANY_OF = "[access]"
    LAKEKEEPER__OPENID_REQUIRED_CLAIMS__CLIENT__CLAIM    = "client_id"
    LAKEKEEPER__OPENID_REQUIRED_CLAIMS__CLIENT__ANY_OF   = "[${join(", ", local.declared_client)}]"
    LAKEKEEPER__OPENID_DISPLAY_NAME_TEMPLATE             = "machine {client_id}"
    LAKEKEEPER__SERVE_SWAGGER_UI                         = "false"
    LAKEKEEPER__UI__ENABLE_SURVEYS                       = "false"
    RUST_LOG                                             = var.catalog_rust_log
  }
  catalog_secrets = [
    { name = "LAKEKEEPER__PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.database["catalog"].arn}:password::" },
    { name = "LAKEKEEPER__PG_ENCRYPTION_KEY", valueFrom = "${aws_secretsmanager_secret.catalog_encryption_key.arn}:key::" },
    { name = "LAKEKEEPER__OPENFGA__API_KEY", valueFrom = "${aws_secretsmanager_secret.authz_key.arn}:key::" },
  ]
  catalog_environment = [for name in sort(keys(local.catalog_settings)) : { name = name, value = local.catalog_settings[name] }]
  # What T35 holds: a deploy of an open catalog is refused before any task definition is registered.
  catalog_fails_closed = (
    local.catalog_settings.LAKEKEEPER__AUTHZ_BACKEND == "openfga"
    && startswith(local.catalog_settings.LAKEKEEPER__OPENID_PROVIDER_URI, "https://")
    && length(local.declared_client) > 0
    && !contains(keys(local.catalog_settings), "LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED")
  )
}

variable "catalog_rust_log" {
  description = "Lakekeeper's RUST_LOG. info by default; T22 applies it once at debug to look for credential values in the log, then back."
  type        = string
  default     = "info"
  validation {
    condition     = contains(["info", "debug"], var.catalog_rust_log)
    error_message = "info, or debug for T22 only."
  }
}

# Lakekeeper encrypts the warehouses' storage credentials in its database with this key (upstream: set it to a
# long random string; the default backend is Postgres).
resource "aws_secretsmanager_secret" "catalog_encryption_key" {
  name                    = "${local.name}/catalog-encryption-key"
  description             = "LAKEKEEPER__PG_ENCRYPTION_KEY of the ${local.env.title}."
  recovery_window_in_days = local.env.secret_recovery_days
}

ephemeral "aws_secretsmanager_random_password" "catalog_encryption_key" {
  password_length     = 64
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "catalog_encryption_key" {
  secret_id                = aws_secretsmanager_secret.catalog_encryption_key.id
  secret_string_wo         = jsonencode({ key = ephemeral.aws_secretsmanager_random_password.catalog_encryption_key.random_password })
  secret_string_wo_version = 1
}

resource "aws_security_group" "catalog" {
  name        = "${local.name}-catalog"
  description = "${local.env.title} catalog: its port from its declared callers only"
  vpc_id      = data.aws_vpc.platform.id
  # dev's group was made with older wording, and a new description replaces a group the services use.
  lifecycle {
    ignore_changes = [description]
  }
}

# Who may call the catalog: the reconciler, and in dev the probes. Lanes come with their component (root ADR-0624 A1).
# The platform's Dagster service and run tasks (infra/aws dagster.tf), whose lanes call the catalog: only while the
# declaration grants a lane a table.
data "aws_security_group" "platform_tasks" {
  count  = length(local.lane_principals) > 0 ? 1 : 0
  vpc_id = data.aws_vpc.platform.id
  name   = "perfectory-platform-tasks"
}

resource "aws_vpc_security_group_ingress_rule" "catalog" {
  for_each = merge(
    { reconciler = aws_security_group.reconciler.id },
    { for group in aws_security_group.probe : "probes" => group.id },
    { for group in data.aws_security_group.platform_tasks : "lanes" => group.id },
  )
  security_group_id            = aws_security_group.catalog.id
  ip_protocol                  = "tcp"
  from_port                    = local.catalog.port
  to_port                      = local.catalog.port
  referenced_security_group_id = each.value
}

# Out of the platform's tasks to the catalog's port: that group allows only TLS and its database otherwise.
resource "aws_vpc_security_group_egress_rule" "lanes_to_catalog" {
  count                        = length(data.aws_security_group.platform_tasks)
  security_group_id            = data.aws_security_group.platform_tasks[0].id
  ip_protocol                  = "tcp"
  from_port                    = local.catalog.port
  to_port                      = local.catalog.port
  referenced_security_group_id = aws_security_group.catalog.id
}

moved {
  from = aws_vpc_security_group_ingress_rule.catalog_from_probes
  to   = aws_vpc_security_group_ingress_rule.catalog["probes"]
}

# Out: the issuer's keys and R2 over TLS, the database, OpenFGA.
resource "aws_vpc_security_group_egress_rule" "catalog" {
  for_each = {
    https    = { port = 443, cidr = "0.0.0.0/0", group = null }
    database = { port = 5432, cidr = null, group = local.database.security_group }
    authz    = { port = local.authz.grpc_port, cidr = null, group = aws_security_group.authz.id }
  }
  security_group_id            = aws_security_group.catalog.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

resource "aws_iam_role" "catalog_execution" {
  name               = "${local.name}-catalog-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "catalog_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:catalog*"]
  }
  statement {
    sid     = "InjectItsSecrets"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.database["catalog"].arn,
      aws_secretsmanager_secret.catalog_encryption_key.arn,
      aws_secretsmanager_secret.authz_key.arn,
    ]
  }
}

resource "aws_iam_role_policy" "catalog_execution" {
  name   = "catalog"
  role   = aws_iam_role.catalog_execution.id
  policy = data.aws_iam_policy_document.catalog_execution.json
}

# The catalog's own AWS identity: none. Lakekeeper reaches R2 with the warehouse's storage credential, not with
# AWS system credentials (LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS stays off), so the task has no role.

resource "aws_ecs_task_definition" "catalog_migrate" {
  family                   = "${local.name}-catalog-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.catalog_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "migrate"
    image            = local.catalog_image
    essential        = true
    command          = ["migrate"]
    environment      = local.catalog_environment
    secrets          = local.catalog_secrets
    logConfiguration = local.logs["catalog-migrate"]
  }])
  depends_on = [aws_secretsmanager_secret_version.database, aws_secretsmanager_secret_version.catalog_encryption_key]
  lifecycle {
    precondition {
      condition     = local.catalog_fails_closed
      error_message = "The catalog must fail closed (T35): LAKEKEEPER__AUTHZ_BACKEND=openfga, an https OIDC issuer, at least one declared principal, and no LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED."
    }
  }
}

# Runs when its definition changes: a new image, or new settings. The service moves only after it exited 0.
resource "terraform_data" "catalog_migrate" {
  triggers_replace = {
    definition = aws_ecs_task_definition.catalog_migrate.revision
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = local.one_off_script
    environment = {
      REGION          = local.region
      CLUSTER         = aws_ecs_cluster.lakekeeper.name
      SUBNETS         = join(",", local.subnets)
      SECURITY_GROUP  = aws_security_group.catalog.id
      TASK_DEFINITION = "${aws_ecs_task_definition.catalog_migrate.family}:${aws_ecs_task_definition.catalog_migrate.revision}"
      WAIT_MINUTES    = "15"
    }
  }
  depends_on = [terraform_data.database_bootstrap, aws_ecs_service.authz, aws_vpc_security_group_egress_rule.catalog]
}

resource "aws_ecs_task_definition" "catalog" {
  family                   = "${local.name}-catalog"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = local.catalog.cpu
  memory                   = local.catalog.memory
  execution_role_arn       = aws_iam_role.catalog_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name                   = "lakekeeper"
    image                  = local.catalog_image
    essential              = true
    command                = ["serve"]
    portMappings           = [{ containerPort = local.catalog.port, protocol = "tcp" }]
    environment            = local.catalog_environment
    secrets                = local.catalog_secrets
    readonlyRootFilesystem = true
    # Upstream's own probe: /health answers 200 when the database is reachable, 503 otherwise.
    healthCheck = {
      command     = ["CMD", "/home/nonroot/lakekeeper", "healthcheck"]
      interval    = 15
      timeout     = 10
      retries     = 3
      startPeriod = 30
    }
    logConfiguration = local.logs["catalog"]
  }])
  depends_on = [aws_secretsmanager_secret_version.database, aws_secretsmanager_secret_version.catalog_encryption_key]
  lifecycle {
    precondition {
      condition     = local.catalog_fails_closed
      error_message = "The catalog must fail closed (T35): LAKEKEEPER__AUTHZ_BACKEND=openfga, an https OIDC issuer, at least one declared principal, and no LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED."
    }
  }
}

resource "aws_service_discovery_service" "catalog" {
  name = "catalog"
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.lakekeeper.id
    routing_policy = "MULTIVALUE"
    dns_records {
      type = "A"
      ttl  = 10
    }
  }
}

resource "aws_ecs_service" "catalog" {
  name             = "${local.name}-catalog"
  cluster          = aws_ecs_cluster.lakekeeper.id
  task_definition  = aws_ecs_task_definition.catalog.arn
  desired_count    = local.catalog.desired_count
  launch_type      = "FARGATE"
  platform_version = "LATEST"
  propagate_tags   = "SERVICE"
  # A rolling deploy: new tasks start and register before old ones stop (T16 watches it with calls running).
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = local.subnets
    security_groups  = [aws_security_group.catalog.id]
    assign_public_ip = true
  }
  service_registries {
    registry_arn = aws_service_discovery_service.catalog.arn
  }
  wait_for_steady_state = true
  depends_on            = [terraform_data.catalog_migrate]
}

output "catalog_url" {
  value = local.catalog_url
}
