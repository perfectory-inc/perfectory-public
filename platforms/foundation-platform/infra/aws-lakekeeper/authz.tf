# OpenFGA, Lakekeeper's open-source authorizer (upstream docs authorization-openfga, v0.15.1: OpenFGA 1.11 or
# later, tested against 1.14). Its own service, so it can be stopped alone (T7). Its store lives in its own
# database (database.tf); `openfga migrate` makes the schema as a one-off task before the service starts, and
# Lakekeeper's own migrate (catalog.tf) then writes Lakekeeper's model into the store. Lakekeeper reaches it by
# gRPC at authz.<namespace> with a pre-shared key; nothing else can reach it.

locals {
  authz = local.env.authz
  # The version contract's pin, as compose files spell it; the task runs it by the index digest.
  authz_image = replace("ghcr.io/openfga/openfga:v1.14.2@sha256:38857bdc425316ebe6dabd1cde52e05c48e6e8bff781713b7c36da8d5ed68205", "/:[^:/@]+@/", "@")
  authz_database_environment = [
    { name = "OPENFGA_DATASTORE_ENGINE", value = "postgres" },
    # Host and database only; the login and its password come apart (they overwrite the URI's).
    { name = "OPENFGA_DATASTORE_URI", value = "postgres://${local.database.address}:${local.database.port}/${local.databases.authz.name}?sslmode=require" },
    { name = "OPENFGA_DATASTORE_USERNAME", value = local.databases.authz.username },
    { name = "OPENFGA_LOG_FORMAT", value = "json" },
  ]
  authz_database_secrets = [
    { name = "OPENFGA_DATASTORE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.database["authz"].arn}:password::" },
  ]
}

resource "aws_secretsmanager_secret" "authz_key" {
  name                    = "${local.name}/authz-preshared-key"
  description             = "The pre-shared key the ${local.env.title} catalog presents to OpenFGA."
  recovery_window_in_days = local.env.secret_recovery_days
}

ephemeral "aws_secretsmanager_random_password" "authz_key" {
  password_length     = 48
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "authz_key" {
  secret_id                = aws_secretsmanager_secret.authz_key.id
  secret_string_wo         = jsonencode({ key = ephemeral.aws_secretsmanager_random_password.authz_key.random_password })
  secret_string_wo_version = 1
}

resource "aws_security_group" "authz" {
  name        = "${local.name}-authz"
  description = "${local.env.title} OpenFGA: gRPC from the catalog only"
  vpc_id      = data.aws_vpc.platform.id
}

resource "aws_vpc_security_group_ingress_rule" "authz_from_catalog" {
  security_group_id            = aws_security_group.authz.id
  ip_protocol                  = "tcp"
  from_port                    = local.authz.grpc_port
  to_port                      = local.authz.grpc_port
  referenced_security_group_id = aws_security_group.catalog.id
}

resource "aws_vpc_security_group_egress_rule" "authz" {
  for_each = {
    https    = { port = 443, cidr = "0.0.0.0/0", group = null }
    database = { port = 5432, cidr = null, group = local.database.security_group }
  }
  security_group_id            = aws_security_group.authz.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

resource "aws_iam_role" "authz_execution" {
  name               = "${local.name}-authz-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "authz_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:authz*"]
  }
  statement {
    sid       = "InjectItsSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.database["authz"].arn, aws_secretsmanager_secret.authz_key.arn]
  }
}

resource "aws_iam_role_policy" "authz_execution" {
  name   = "authz"
  role   = aws_iam_role.authz_execution.id
  policy = data.aws_iam_policy_document.authz_execution.json
}

resource "aws_ecs_task_definition" "authz_migrate" {
  family                   = "${local.name}-authz-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.authz_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "migrate"
    image            = local.authz_image
    essential        = true
    command          = ["migrate"]
    environment      = local.authz_database_environment
    secrets          = local.authz_database_secrets
    logConfiguration = local.logs["authz-migrate"]
  }])
  depends_on = [aws_secretsmanager_secret_version.database]
}

resource "terraform_data" "authz_migrate" {
  triggers_replace = {
    definition = aws_ecs_task_definition.authz_migrate.revision
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = local.one_off_script
    environment = {
      REGION          = local.region
      CLUSTER         = aws_ecs_cluster.lakekeeper.name
      SUBNETS         = join(",", local.subnets)
      SECURITY_GROUP  = aws_security_group.authz.id
      TASK_DEFINITION = "${aws_ecs_task_definition.authz_migrate.family}:${aws_ecs_task_definition.authz_migrate.revision}"
      WAIT_MINUTES    = "15"
    }
  }
  depends_on = [terraform_data.database_bootstrap, aws_vpc_security_group_egress_rule.authz]
}

resource "aws_ecs_task_definition" "authz" {
  family                   = "${local.name}-authz"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = local.authz.cpu
  memory                   = local.authz.memory
  execution_role_arn       = aws_iam_role.authz_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name         = "openfga"
    image        = local.authz_image
    essential    = true
    command      = ["run"]
    portMappings = [{ containerPort = local.authz.grpc_port, protocol = "tcp" }]
    environment = concat(local.authz_database_environment, [
      { name = "OPENFGA_GRPC_ADDR", value = "0.0.0.0:${local.authz.grpc_port}" },
      # Only gRPC is served: no HTTP API, no playground.
      { name = "OPENFGA_HTTP_ENABLED", value = "false" },
      { name = "OPENFGA_PLAYGROUND_ENABLED", value = "false" },
      { name = "OPENFGA_AUTHN_METHOD", value = "preshared" },
      { name = "OPENFGA_DATASTORE_MAX_OPEN_CONNS", value = tostring(local.authz.max_open_connections) },
      { name = "OPENFGA_DATASTORE_MAX_IDLE_CONNS", value = tostring(local.authz.max_idle_connections) },
      # Opened at start and kept (PostgreSQL only): a burst never waits for new connections (T13).
      { name = "OPENFGA_DATASTORE_MIN_OPEN_CONNS", value = tostring(local.authz.min_open_connections) },
      # Lakekeeper's production advice for OpenFGA (upstream authorization-openfga, "OpenFGA in Production").
      { name = "OPENFGA_CACHE_CONTROLLER_ENABLED", value = "true" },
      { name = "OPENFGA_CHECK_QUERY_CACHE_ENABLED", value = "true" },
      { name = "OPENFGA_CHECK_ITERATOR_CACHE_ENABLED", value = "true" },
    ])
    secrets = concat(local.authz_database_secrets, [
      { name = "OPENFGA_AUTHN_PRESHARED_KEYS", valueFrom = "${aws_secretsmanager_secret.authz_key.arn}:key::" },
    ])
    readonlyRootFilesystem = true
    # The image's own health probe (its Dockerfile's HEALTHCHECK); the gRPC health service needs no key.
    healthCheck = {
      command     = ["CMD", "/usr/local/bin/grpc_health_probe", "-addr=127.0.0.1:${local.authz.grpc_port}"]
      interval    = 15
      timeout     = 10
      retries     = 3
      startPeriod = 30
    }
    logConfiguration = local.logs["authz"]
  }])
  depends_on = [aws_secretsmanager_secret_version.database, aws_secretsmanager_secret_version.authz_key]
}

resource "aws_service_discovery_service" "authz" {
  name = "authz"
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.lakekeeper.id
    routing_policy = "MULTIVALUE"
    dns_records {
      type = "A"
      ttl  = 10
    }
  }
}

resource "aws_ecs_service" "authz" {
  name                               = "${local.name}-authz"
  cluster                            = aws_ecs_cluster.lakekeeper.id
  task_definition                    = aws_ecs_task_definition.authz.arn
  desired_count                      = local.authz.desired_count
  launch_type                        = "FARGATE"
  platform_version                   = "LATEST"
  propagate_tags                     = "SERVICE"
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = local.subnets
    security_groups  = [aws_security_group.authz.id]
    assign_public_ip = true
  }
  service_registries {
    registry_arn = aws_service_discovery_service.authz.arn
  }
  wait_for_steady_state = true
  depends_on            = [terraform_data.authz_migrate]
  # T7 stops this service by hand (desired count 0) and starts it again; an apply puts the declared count back.
}
