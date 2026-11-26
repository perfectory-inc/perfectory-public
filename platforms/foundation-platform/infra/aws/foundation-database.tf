# The Bronze ledger's place on RDS (root ADR-0616 A1b, coordinator 2026-11-26): a database `foundation` on
# the Dagster instance with the data host's schema, made by the same sqlx migrations (foundation-migrate,
# from the release's arm64 runtime image), empty until a collector moves in (A2). Its migrator and its
# runtime role have their own secrets, written write-only like Dagster's.
#
# The rollout migrates first: every apply that moves the platform to a new image digest runs the
# one-off tasks (bootstrap, migrate, grants and finalize) in order before the service moves, and stops
# at the first that does not exit 0 (pre-deploy.sh). Values are the contract's (database.foundation).

variable "runtime_image" {
  description = "The release's AWS runtime image by digest, from a verified record (scripts/deploy/aws-release-images.py writes it): foundation-migrate runs from it. A rollout takes it from the same commit as dagster_image; a rollback keeps the newer one (--runtime-commit), since migrations only go forward."
  type        = string
  validation {
    condition     = startswith(var.runtime_image, "${local.contract.images.repository}@") && can(regex("^sha256:[0-9a-f]{64}$", trimprefix(var.runtime_image, "${local.contract.images.repository}@")))
    error_message = "The image is the contract's release repository (images.repository) by digest: never a tag, never another repository."
  }
}

locals {
  foundation = local.database.foundation
  # The one-off tasks log beside Dagster, under their own stream prefixes.
  one_off_logs = {
    logDriver = "awslogs"
    options = {
      awslogs-group  = aws_cloudwatch_log_group.dagster.name
      awslogs-region = local.contract.region
    }
  }
}

# The migrator owns the schema (DDL in every rollout); the runtime role reads and writes rows, which is
# what a collector does from A2 (infra/compose/grant-foundation-runtime.sql). A new password: raise the
# version and apply; the rollout's bootstrap sets it.
resource "aws_secretsmanager_secret" "foundation_migrator" {
  name        = local.foundation.migrator_secret
  description = "The foundation database's migrator (schema owner; not the RDS master user)."
}

ephemeral "aws_secretsmanager_random_password" "foundation_migrator" {
  password_length     = 40
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "foundation_migrator" {
  secret_id = aws_secretsmanager_secret.foundation_migrator.id
  secret_string_wo = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.platform.address
    port     = aws_db_instance.platform.port
    dbname   = local.foundation.name
    username = local.foundation.migrator_username
    password = ephemeral.aws_secretsmanager_random_password.foundation_migrator.random_password
  })
  secret_string_wo_version = 1
}

resource "aws_secretsmanager_secret" "foundation_runtime" {
  name        = local.foundation.runtime_secret
  description = "The foundation database's runtime role (rows only)."
}

ephemeral "aws_secretsmanager_random_password" "foundation_runtime" {
  password_length     = 40
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "foundation_runtime" {
  secret_id = aws_secretsmanager_secret.foundation_runtime.id
  secret_string_wo = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.platform.address
    port     = aws_db_instance.platform.port
    dbname   = local.foundation.name
    username = local.foundation.runtime_username
    password = ephemeral.aws_secretsmanager_random_password.foundation_runtime.random_password
  })
  secret_string_wo_version = 1
}

# The migrate task's own execution role: it injects the migrator's password and nothing else.
resource "aws_iam_role" "foundation_migrate_execution" {
  name               = "perfectory-platform-foundation-migrate-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "foundation_migrate_execution" {
  role       = aws_iam_role.foundation_migrate_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "foundation_migrate_execution" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.foundation_migrator.arn]
  }
}

resource "aws_iam_role_policy" "foundation_migrate_execution" {
  name   = "migrator-password"
  role   = aws_iam_role.foundation_migrate_execution.id
  policy = data.aws_iam_policy_document.foundation_migrate_execution.json
}

# The data host's migrator (services/foundation-api/src/bin/foundation-migrate.rs: every migration of
# migrations/, under sqlx's lock), from the release's own runtime image. It reads one database URL; the
# password is alphanumeric, so it stands in the URL as it is.
resource "aws_ecs_task_definition" "foundation_migrate" {
  family                   = "perfectory-platform-foundation-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.foundation_migrate_execution.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name       = "migrate"
    image      = var.runtime_image
    essential  = true
    entryPoint = ["/bin/sh", "-c"]
    command    = ["export FOUNDATION_MIGRATOR_DATABASE_URL=\"postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE?sslmode=require\" && exec /usr/local/bin/foundation-migrate"]
    environment = [
      { name = "PGHOST", value = aws_db_instance.platform.address },
      { name = "PGPORT", value = tostring(aws_db_instance.platform.port) },
      { name = "PGUSER", value = local.foundation.migrator_username },
      { name = "PGDATABASE", value = local.foundation.name },
    ]
    secrets = [
      { name = "PGPASSWORD", valueFrom = "${aws_secretsmanager_secret.foundation_migrator.arn}:password::" },
    ]
    logConfiguration = merge(local.one_off_logs, {
      options = merge(local.one_off_logs.options, { awslogs-stream-prefix = "foundation-migrate" })
    })
  }])
  depends_on = [aws_secretsmanager_secret_version.foundation_migrator]
}

# After the migrations, as on the data host: the runtime role's grants, as the migrator that owns the
# tables (infra/compose/grant-foundation-runtime.sql), then finalize as the master (CREATE taken back from
# the migrator, the roles' hardening checked: infra/compose/finalize-foundation.sql). The same files the
# data host runs. Under the bootstrap's execution role, which already reads both passwords.
resource "aws_ecs_task_definition" "database_finalize" {
  family                   = "perfectory-platform-database-finalize"
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
    name       = "finalize"
    image      = "postgres:17-alpine@${local.versions.container_images["postgres:17-alpine"]}"
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = ["printf '%s' \"$GRANTS_SQL\" | PGUSER=\"$MIGRATOR_USER\" PGPASSWORD=\"$MIGRATOR_PASSWORD\" psql --no-psqlrc --set ON_ERROR_STOP=1 --file - && printf '%s' \"$FINALIZE_SQL\" | psql --no-psqlrc --set ON_ERROR_STOP=1 --file -"]
    environment = [
      { name = "PGHOST", value = aws_db_instance.platform.address },
      { name = "PGPORT", value = tostring(aws_db_instance.platform.port) },
      { name = "PGUSER", value = local.database.master_username },
      { name = "PGDATABASE", value = local.foundation.name },
      { name = "PGSSLMODE", value = "require" },
      { name = "MIGRATOR_USER", value = local.foundation.migrator_username },
      { name = "GRANTS_SQL", value = file("${path.module}/../compose/grant-foundation-runtime.sql") },
      { name = "FINALIZE_SQL", value = file("${path.module}/../compose/finalize-foundation.sql") },
    ]
    secrets = [
      { name = "PGPASSWORD", valueFrom = "${aws_db_instance.platform.master_user_secret[0].secret_arn}:password::" },
      { name = "MIGRATOR_PASSWORD", valueFrom = "${aws_secretsmanager_secret.foundation_migrator.arn}:password::" },
    ]
    logConfiguration = merge(local.one_off_logs, {
      options = merge(local.one_off_logs.options, { awslogs-stream-prefix = "database-finalize" })
    })
  }])
  depends_on = [aws_secretsmanager_secret_version.foundation_migrator]
}

# The rollout's pre-deploy step: a new image digest, a task definition's new revision or a new password
# replaces this, and replacing it runs the chain. The service depends on it, so a chain that fails
# leaves the service where it was and the apply failed (this resource is then tainted, and the next
# apply runs the chain again).
resource "terraform_data" "pre_deploy" {
  triggers_replace = {
    dagster_image     = var.dagster_image
    runtime_image     = var.runtime_image
    bootstrap         = aws_ecs_task_definition.database_bootstrap.revision
    migrate           = aws_ecs_task_definition.foundation_migrate.revision
    finalize          = aws_ecs_task_definition.database_finalize.revision
    dagster_password  = aws_secretsmanager_secret_version.dagster_database.secret_string_wo_version
    migrator_password = aws_secretsmanager_secret_version.foundation_migrator.secret_string_wo_version
    runtime_password  = aws_secretsmanager_secret_version.foundation_runtime.secret_string_wo_version
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = "${path.module}/pre-deploy.sh"
    environment = {
      ACCOUNT        = local.contract.account.id
      REGION         = local.contract.region
      CLUSTER        = aws_ecs_cluster.platform.name
      SUBNET         = [for subnet in aws_subnet.public : subnet.id][0]
      SECURITY_GROUP = aws_security_group.tasks.id
      # In order: bootstrap, migrate, grants and finalize (docker-compose.yml's chain).
      TASK_DEFINITIONS = join(" ", [for task in [
        aws_ecs_task_definition.database_bootstrap,
        aws_ecs_task_definition.foundation_migrate,
        aws_ecs_task_definition.database_finalize,
      ] : "${task.family}:${task.revision}"])
    }
  }
}

output "foundation_database" {
  value = local.foundation.name
}
