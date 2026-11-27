# The catalog's two databases (coordinator 2026-11-27: their own databases and logins, never the Dagster or ledger
# ones): the catalog's (Lakekeeper) and authz's (OpenFGA). Each login owns its database, so its own migrations
# create its schema (Lakekeeper's five extensions are trusted on PostgreSQL 17, and upstream's migrate creates them
# when the role has CREATE on the database). The bootstrap task makes the logins and databases as the instance's
# master user, the way infra/aws/database-bootstrap.sql makes Dagster's. Passwords are generated at apply time and
# written write-only: the state holds only the secrets' ARNs.
#
# Where they live is the contract's databases.instance: none (dev) is the platform's shared RDS instance; an object
# (prod) is an instance of this stack's own (root ADR-0624 decision 4 and §7): Single-AZ with automated backups and
# point-in-time restore now, Multi-AZ at launch (owner 2026-11-27), which is that one contract value, done by RDS in
# place with no data move.

locals {
  databases = local.env.databases
  own       = local.databases.instance
  # psql comes from the platform's mirror of the version contract's postgres image (root ADR-0619 §7).
  postgres_image = "${local.platform.images.repository}@${local.versions.container_images["postgres:17-alpine"]}"
  database = local.own == null ? {
    address         = data.aws_db_instance.platform[0].address
    port            = data.aws_db_instance.platform[0].port
    master_username = local.platform.database.master_username
    master_secret   = data.aws_db_instance.platform[0].master_user_secret[0].secret_arn
    security_group  = data.aws_security_group.platform_database[0].id
    } : {
    address         = aws_db_instance.catalog[0].address
    port            = aws_db_instance.catalog[0].port
    master_username = local.own.master_username
    master_secret   = aws_db_instance.catalog[0].master_user_secret[0].secret_arn
    security_group  = aws_security_group.database[0].id
  }
  database_address = [
    { name = "PGHOST", value = local.database.address },
    { name = "PGPORT", value = tostring(local.database.port) },
    { name = "PGSSLMODE", value = "require" },
  ]
  master_secret = local.database.master_secret
}

# The own instance (prod): in the platform's database subnets (infra/aws), reached only by this stack's tasks.
resource "aws_security_group" "database" {
  count       = local.own == null ? 0 : 1
  name        = "${local.name}-database"
  description = "${local.env.title} database: PostgreSQL from the catalog, OpenFGA and the bootstrap only"
  vpc_id      = data.aws_vpc.platform.id
}

# max_connections is set, so the alarm's share of it is a number (alarms.tf) and the logins' limits fit under it.
resource "aws_db_parameter_group" "catalog" {
  count  = local.own == null ? 0 : 1
  name   = local.own.identifier
  family = "postgres${local.own.engine_version}"
  parameter {
    name         = "max_connections"
    value        = tostring(local.own.max_connections)
    apply_method = "pending-reboot"
  }
}

resource "aws_db_instance" "catalog" {
  count                       = local.own == null ? 0 : 1
  identifier                  = local.own.identifier
  engine                      = "postgres"
  engine_version              = local.own.engine_version
  instance_class              = local.own.instance_class
  allocated_storage           = local.own.allocated_storage_gb
  max_allocated_storage       = local.own.max_allocated_storage_gb
  storage_type                = "gp3"
  storage_encrypted           = true
  username                    = local.own.master_username
  manage_master_user_password = true
  parameter_group_name        = aws_db_parameter_group.catalog[0].name
  db_subnet_group_name        = local.platform.database.identifier
  vpc_security_group_ids      = [aws_security_group.database[0].id]
  publicly_accessible         = false
  multi_az                    = local.own.multi_az
  # Automated backups are what point-in-time restore replays (to any second of the retention window).
  backup_retention_period    = local.own.backup_retention_days
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = true
  deletion_protection        = true
  skip_final_snapshot        = false
  final_snapshot_identifier  = "${local.own.identifier}-final"
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_secretsmanager_secret" "database" {
  for_each    = { catalog = local.databases.catalog, authz = local.databases.authz }
  name        = "${local.name}/${each.key}-database"
  description = "The ${each.value.username} login of the ${local.env.title} (owner of database ${each.value.name})."
  # A dev secret goes at once on destroy, so the rig can be made again under the same names.
  recovery_window_in_days = local.env.secret_recovery_days
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
    host     = local.database.address
    port     = local.database.port
    dbname   = local.databases[each.key].name
    username = local.databases[each.key].username
    password = ephemeral.aws_secretsmanager_random_password.database[each.key].random_password
  })
  secret_string_wo_version = 1
}

# The bootstrap task reaches the database and pulls its image; nothing comes in.
resource "aws_security_group" "database_bootstrap" {
  name        = "${local.name}-database-bootstrap"
  description = "${local.env.title} database bootstrap: no inbound"
  vpc_id      = data.aws_vpc.platform.id
}

resource "aws_vpc_security_group_egress_rule" "database_bootstrap" {
  for_each = {
    https    = { port = 443, cidr = "0.0.0.0/0", group = null }
    database = { port = 5432, cidr = null, group = local.database.security_group }
  }
  security_group_id            = aws_security_group.database_bootstrap.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.group
}

# The database lets in the bootstrap, authz and catalog tasks of this stack, and nothing else of it.
resource "aws_vpc_security_group_ingress_rule" "database" {
  for_each = {
    bootstrap = aws_security_group.database_bootstrap.id
    authz     = aws_security_group.authz.id
    catalog   = aws_security_group.catalog.id
  }
  security_group_id            = local.database.security_group
  description                  = "${trimprefix(local.name, "perfectory-")} ${each.key}"
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
    resources = ["${aws_cloudwatch_log_group.lakekeeper.arn}:log-stream:database-bootstrap/*"]
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
      { name = "PGUSER", value = local.database.master_username },
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
      CLUSTER         = aws_ecs_cluster.lakekeeper.name
      SUBNETS         = join(",", local.subnets)
      SECURITY_GROUP  = aws_security_group.database_bootstrap.id
      TASK_DEFINITION = "${aws_ecs_task_definition.database_bootstrap.family}:${aws_ecs_task_definition.database_bootstrap.revision}"
      WAIT_MINUTES    = "15"
    }
  }
  depends_on = [aws_vpc_security_group_ingress_rule.database, aws_vpc_security_group_egress_rule.database_bootstrap]
}
