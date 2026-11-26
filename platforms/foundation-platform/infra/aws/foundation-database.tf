# The Bronze ledger's place on RDS (root ADR-0616 A1b, coordinator 2026-11-26): a database `foundation` on
# the Dagster instance with the data host's schema, made by the same sqlx migrations (foundation-migrate,
# from the release's arm64 runtime image), empty until a collector moves in (A2). Its migrator and its
# runtime role have their own secrets, written write-only like Dagster's. The one-off tasks that make and
# migrate it, and the rollout steps that run them before the service moves, are below.

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

# The one-off tasks (root ADR-0616 §2-4, coordinator 2026-11-26): two, each a task of its own outside the
# service, started by the rollout before the service moves, never by Dagster.
#
# - bootstrap (master): the databases' roles, their passwords, the foundation database and its schemas.
#   It runs when its own definition changes (its SQL is in it) or a password's version does, never on an
#   image change.
# - migrate: one task whose containers run in order, each only after the one before exited 0 (ECS
#   `dependsOn` SUCCESS; no start timeout, so a long migration is waited for): open (the master lets the
#   migrator create, as the data host's bootstrap does on every deploy), migrate (foundation-migrate from the
#   release's runtime image), grants (as the migrator), finalize (the master takes CREATE back and checks the
#   roles). It runs when its definition changes, which a new runtime image does. Every step is idempotent.
#
# psql comes from the version contract's postgres image, mirrored by digest into the release package on GHCR
# (scripts/deploy/aws-images-build.py), never from Docker Hub (root ADR-0619 §7). Passwords reach psql as
# environment variables it reads with \getenv, never on its command line.
locals {
  postgres_image = "${local.contract.images.repository}@${local.versions.container_images["postgres:17-alpine"]}"
  one_off_logs = {
    logDriver = "awslogs"
    options = {
      awslogs-group  = aws_cloudwatch_log_group.dagster.name
      awslogs-region = local.contract.region
    }
  }
  # What each one-off task's execution role may inject: the bootstrap sets every role's password, the
  # migrate task's steps run as the master (open, finalize) or the migrator (migrate, grants).
  one_off_secrets = {
    bootstrap = [
      aws_db_instance.platform.master_user_secret[0].secret_arn,
      aws_secretsmanager_secret.dagster_database.arn,
      aws_secretsmanager_secret.foundation_migrator.arn,
      aws_secretsmanager_secret.foundation_runtime.arn,
    ]
    migrate = [
      aws_db_instance.platform.master_user_secret[0].secret_arn,
      aws_secretsmanager_secret.foundation_migrator.arn,
    ]
  }
  master_password   = "${aws_db_instance.platform.master_user_secret[0].secret_arn}:password::"
  migrator_password = "${aws_secretsmanager_secret.foundation_migrator.arn}:password::"
  # Every step reaches the database the same way; a container adds its user and database.
  database_address = [
    { name = "PGHOST", value = aws_db_instance.platform.address },
    { name = "PGPORT", value = tostring(aws_db_instance.platform.port) },
    { name = "PGSSLMODE", value = "require" },
  ]
}

resource "aws_iam_role" "one_off_execution" {
  for_each           = local.one_off_secrets
  name               = "perfectory-platform-database-${each.key}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "one_off_execution" {
  for_each = local.one_off_secrets
  statement {
    sid       = "WriteItsLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dagster.arn}:log-stream:database-${each.key}/*"]
  }
  statement {
    sid       = "InjectItsPasswords"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = each.value
  }
}

resource "aws_iam_role_policy" "one_off_execution" {
  for_each = local.one_off_secrets
  name     = "database-${each.key}"
  role     = aws_iam_role.one_off_execution[each.key].id
  policy   = data.aws_iam_policy_document.one_off_execution[each.key].json
}

resource "aws_ecs_task_definition" "database_bootstrap" {
  family                   = "perfectory-platform-database-bootstrap"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.one_off_execution["bootstrap"].arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name       = "bootstrap"
    image      = local.postgres_image
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = ["printf '%s' \"$BOOTSTRAP_SQL\" | psql --no-psqlrc --set app_user=\"$APP_USER\" --set db_name=\"$DB_NAME\" --file - && printf '%s' \"$FOUNDATION_BOOTSTRAP_SQL\" | psql --no-psqlrc --set db_name=\"$FOUNDATION_DB\" --file -"]
    environment = concat(local.database_address, [
      { name = "PGUSER", value = local.database.master_username },
      { name = "PGDATABASE", value = "postgres" },
      { name = "APP_USER", value = local.database.app_username },
      { name = "DB_NAME", value = local.database.name },
      { name = "BOOTSTRAP_SQL", value = file("${path.module}/database-bootstrap.sql") },
      { name = "FOUNDATION_DB", value = local.foundation.name },
      { name = "FOUNDATION_BOOTSTRAP_SQL", value = file("${path.module}/foundation-bootstrap.sql") },
    ])
    secrets = [
      { name = "PGPASSWORD", valueFrom = local.master_password },
      { name = "APP_PASSWORD", valueFrom = "${aws_secretsmanager_secret.dagster_database.arn}:password::" },
      { name = "MIGRATOR_PASSWORD", valueFrom = local.migrator_password },
      { name = "RUNTIME_PASSWORD", valueFrom = "${aws_secretsmanager_secret.foundation_runtime.arn}:password::" },
    ]
    logConfiguration = merge(local.one_off_logs, {
      options = merge(local.one_off_logs.options, { awslogs-stream-prefix = "database-bootstrap" })
    })
  }])
  depends_on = [
    aws_secretsmanager_secret_version.dagster_database,
    aws_secretsmanager_secret_version.foundation_migrator,
    aws_secretsmanager_secret_version.foundation_runtime,
  ]
}

locals {
  # The migrate task's steps in order: each after the one before exited 0; only the last is essential
  # (ECS: SUCCESS cannot be the condition on an essential container).
  migrate_steps = [
    {
      name  = "open"
      image = local.postgres_image
      user  = local.database.master_username
      sql   = file("${path.module}/foundation-migrate-open.sql")
    },
    {
      name  = "migrate"
      image = var.runtime_image
      user  = local.foundation.migrator_username
      sql   = null
    },
    {
      name  = "grants"
      image = local.postgres_image
      user  = local.foundation.migrator_username
      sql   = file("${path.module}/../compose/grant-foundation-runtime.sql")
    },
    {
      name  = "finalize"
      image = local.postgres_image
      user  = local.database.master_username
      sql   = file("${path.module}/../compose/finalize-foundation.sql")
    },
  ]
}

resource "aws_ecs_task_definition" "database_migrate" {
  family                   = "perfectory-platform-database-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.one_off_execution["migrate"].arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([for index, step in local.migrate_steps : merge({
    name       = step.name
    image      = step.image
    essential  = index == length(local.migrate_steps) - 1
    entryPoint = ["/bin/sh", "-c"]
    # The migrator reads one database URL (services/foundation-api/src/bin/foundation-migrate.rs, every
    # migration of migrations/ under sqlx's lock); its password is alphanumeric, so it stands in the URL as it is.
    command = [step.sql == null
      ? "export FOUNDATION_MIGRATOR_DATABASE_URL=\"postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE?sslmode=require\" && exec /usr/local/bin/foundation-migrate"
    : "printf '%s' \"$STEP_SQL\" | psql --no-psqlrc --set ON_ERROR_STOP=1 --file -"]
    environment = concat(local.database_address, [
      { name = "PGUSER", value = step.user },
      { name = "PGDATABASE", value = local.foundation.name },
    ], step.sql == null ? [] : [{ name = "STEP_SQL", value = step.sql }])
    secrets = [
      { name = "PGPASSWORD", valueFrom = step.user == local.database.master_username ? local.master_password : local.migrator_password },
    ]
    logConfiguration = merge(local.one_off_logs, {
      options = merge(local.one_off_logs.options, { awslogs-stream-prefix = "database-migrate" })
    })
  }, index == 0 ? {} : { dependsOn = [{ containerName = local.migrate_steps[index - 1].name, condition = "SUCCESS" }] })])
  depends_on = [aws_secretsmanager_secret_version.foundation_migrator]
}

# The rollout's two steps before the service moves (pre-deploy.sh runs one task and waits for every container
# of it to exit 0). Replacing one runs it; a failed one stays tainted and the next apply runs it again. The
# service depends on both, so a step that fails leaves the service where it was and fails the apply.
resource "terraform_data" "database_bootstrap" {
  triggers_replace = {
    definition        = aws_ecs_task_definition.database_bootstrap.revision
    dagster_password  = aws_secretsmanager_secret_version.dagster_database.secret_string_wo_version
    migrator_password = aws_secretsmanager_secret_version.foundation_migrator.secret_string_wo_version
    runtime_password  = aws_secretsmanager_secret_version.foundation_runtime.secret_string_wo_version
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = "${path.module}/pre-deploy.sh"
    environment = local.one_off_run_environment.database_bootstrap
  }
}

resource "terraform_data" "database_migrate" {
  triggers_replace = {
    definition = aws_ecs_task_definition.database_migrate.revision
  }
  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = "${path.module}/pre-deploy.sh"
    environment = local.one_off_run_environment.database_migrate
  }
  depends_on = [terraform_data.database_bootstrap]
}

locals {
  one_off_run_environment = {
    for name, task in {
      database_bootstrap = aws_ecs_task_definition.database_bootstrap
      database_migrate   = aws_ecs_task_definition.database_migrate
      } : name => {
      REGION          = local.contract.region
      CLUSTER         = aws_ecs_cluster.platform.name
      SUBNETS         = join(",", [for subnet in aws_subnet.public : subnet.id])
      SECURITY_GROUP  = aws_security_group.tasks.id
      TASK_DEFINITION = "${task.family}:${task.revision}"
      WAIT_MINUTES    = tostring(local.database.one_off_wait_minutes)
    }
  }
}

output "one_off_task_definitions" {
  value = [aws_ecs_task_definition.database_bootstrap.family, aws_ecs_task_definition.database_migrate.family]
}

output "foundation_database" {
  value = local.foundation.name
}
