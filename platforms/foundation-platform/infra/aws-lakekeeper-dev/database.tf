# The rig's two databases on the platform's RDS instance (coordinator 2026-11-27: their own databases and logins,
# never the Dagster or ledger ones): lakekeeper_dev for the catalog, openfga_dev for authz. Each login owns its
# database, so its own migrations create its schema (Lakekeeper's five extensions are trusted on PostgreSQL 17,
# and upstream's migrate creates them when the role has CREATE on the database). The bootstrap task makes the
# logins and databases as the RDS master user, the way infra/aws/database-bootstrap.sql makes Dagster's.
# Passwords are generated at apply time and written write-only: the state holds only the secrets' ARNs.

locals {
  databases = local.dev.databases
  # psql comes from the platform's mirror of the version contract's postgres image (root ADR-0619 §7).
  postgres_image = "${local.platform.images.repository}@${local.versions.container_images["postgres:17-alpine"]}"
  database_address = [
    { name = "PGHOST", value = data.aws_db_instance.platform.address },
    { name = "PGPORT", value = tostring(data.aws_db_instance.platform.port) },
    { name = "PGSSLMODE", value = "require" },
  ]
  master_secret = data.aws_db_instance.platform.master_user_secret[0].secret_arn
}

resource "aws_secretsmanager_secret" "database" {
  for_each    = { catalog = local.databases.catalog, authz = local.databases.authz }
  name        = "${local.name}/${each.key}-database"
  description = "The ${each.value.username} login of the Lakekeeper dev rig (owner of database ${each.value.name})."
  # A dev secret goes at once on destroy, so the rig can be made again under the same names.
  recovery_window_in_days = 0
}

ephemeral "aws_secretsmanager_random_password" "database" {
  for_each            = aws_secretsmanager_secret.database
  password_length     = 40
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "database" {
  for_each  = aws_secretsmanager_secret.database
  secret_id = each.value.id
  secret_string_wo = jsonencode({
    engine   = "postgres"
    host     = data.aws_db_instance.platform.address
    port     = data.aws_db_instance.platform.port
    dbname   = local.databases[each.key].name
    username = local.databases[each.key].username
    password = ephemeral.aws_secretsmanager_random_password.database[each.key].random_password
  })
  secret_string_wo_version = 1
}

# The bootstrap task reaches the database and pulls its image; nothing comes in.
resource "aws_security_group" "database_bootstrap" {
  name        = "${local.name}-database-bootstrap"
  description = "Lakekeeper dev rig database bootstrap: no inbound"
  vpc_id      = data.aws_vpc.platform.id
}

resource "aws_vpc_security_group_egress_rule" "database_bootstrap" {
  for_each = {
    https    = { port = 443, cidr = "0.0.0.0/0", group = null }
    database = { port = 5432, cidr = null, group = data.aws_security_group.database.id }
  }
  security_group_id            = aws_security_group.database_bootstrap.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

# The platform database lets in the rig's bootstrap, authz and catalog tasks, and nothing else of the rig.
resource "aws_vpc_security_group_ingress_rule" "database" {
  for_each = {
    bootstrap = aws_security_group.database_bootstrap.id
    authz     = aws_security_group.authz.id
    catalog   = aws_security_group.catalog.id
  }
  security_group_id            = data.aws_security_group.database.id
  description                  = "lakekeeper-dev ${each.key}"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = each.value
}

resource "aws_iam_role" "database_bootstrap_execution" {
  name               = "${local.name}-database-bootstrap-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "database_bootstrap_execution" {
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dev.arn}:log-stream:database-bootstrap/*"]
  }
  statement {
    sid       = "InjectItsPasswords"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = concat([local.master_secret], [for secret in aws_secretsmanager_secret.database : secret.arn])
  }
}

resource "aws_iam_role_policy" "database_bootstrap_execution" {
  name   = "database-bootstrap"
  role   = aws_iam_role.database_bootstrap_execution.id
  policy = data.aws_iam_policy_document.database_bootstrap_execution.json
}

resource "aws_ecs_task_definition" "database_bootstrap" {
  family                   = "${local.name}-database-bootstrap"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.database_bootstrap_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name       = "bootstrap"
    image      = local.postgres_image
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = ["printf '%s' \"$BOOTSTRAP_SQL\" | psql --no-psqlrc --file -"]
    environment = concat(local.database_address, [
      { name = "PGUSER", value = local.platform.database.master_username },
      { name = "PGDATABASE", value = "postgres" },
      { name = "CATALOG_USER", value = local.databases.catalog.username },
      { name = "CATALOG_DB", value = local.databases.catalog.name },
      { name = "CATALOG_LIMIT", value = tostring(local.databases.catalog.connection_limit) },
      { name = "AUTHZ_USER", value = local.databases.authz.username },
      { name = "AUTHZ_DB", value = local.databases.authz.name },
      { name = "AUTHZ_LIMIT", value = tostring(local.databases.authz.connection_limit) },
      { name = "BOOTSTRAP_SQL", value = file("${path.module}/database-bootstrap.sql") },
    ])
    secrets = [
      { name = "PGPASSWORD", valueFrom = "${local.master_secret}:password::" },
      { name = "CATALOG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.database["catalog"].arn}:password::" },
      { name = "AUTHZ_PASSWORD", valueFrom = "${aws_secretsmanager_secret.database["authz"].arn}:password::" },
    ]
    logConfiguration = local.logs["database-bootstrap"]
  }])
  depends_on = [aws_secretsmanager_secret_version.database]
}

# Runs when its definition (its SQL is in it) or a password's version changes. A failed run stays tainted and
# the next apply runs it again.
resource "terraform_data" "database_bootstrap" {
  triggers_replace = {
    definition = aws_ecs_task_definition.database_bootstrap.revision
    passwords  = join(",", [for version in aws_secretsmanager_secret_version.database : version.secret_string_wo_version])
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = local.one_off_script
    environment = {
      REGION          = local.region
      CLUSTER         = aws_ecs_cluster.dev.name
      SUBNETS         = join(",", local.subnets)
      SECURITY_GROUP  = aws_security_group.database_bootstrap.id
      TASK_DEFINITION = "${aws_ecs_task_definition.database_bootstrap.family}:${aws_ecs_task_definition.database_bootstrap.revision}"
      WAIT_MINUTES    = "15"
    }
  }
  depends_on = [aws_vpc_security_group_ingress_rule.database, aws_vpc_security_group_egress_rule.database_bootstrap]
}
