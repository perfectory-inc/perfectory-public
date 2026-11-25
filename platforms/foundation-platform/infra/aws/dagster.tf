# Dagster on AWS with no job yet (root ADR-0616 A1): its cluster, its database and the role it connects
# as, its always-on service (webserver and daemon) and the alarm that says it is not running. Every value is the contract's
# (config/aws-platform.contract.json: ecs, dagster, database, alarms); the image is the release's AWS
# code location by digest (root ADR-0620), given at apply time. Collectors move in from A2, one at a time.

variable "dagster_image" {
  description = "The release's AWS code-location image by digest (ghcr.io/<owner>/foundation-release@sha256:...), the digest of that release's <commit>-aws-code-location (aws-images.json, signed by the release workflow). The service runs it, and every run task Dagster launches runs it too."
  type        = string
  validation {
    condition     = can(regex("^ghcr\\.io/[a-z0-9._-]+/[a-z0-9._-]+@sha256:[0-9a-f]{64}$", var.dagster_image))
    error_message = "The image is a GHCR reference by digest, never a tag."
  }
}

locals {
  dagster  = local.contract.dagster
  database = local.contract.database
  # Image pins are the repository's one contract (the bootstrap's postgres client comes from there).
  versions = jsondecode(file("${path.module}/../../../../tools/technology-versions.contract.json"))
}

resource "aws_ecs_cluster" "platform" {
  name = local.contract.ecs.cluster
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_cloudwatch_log_group" "dagster" {
  name              = local.dagster.log_group
  retention_in_days = local.dagster.log_retention_days
}

# The service and its run tasks: nothing comes in; out to the internet over TLS and to the database.
resource "aws_security_group" "tasks" {
  name        = "perfectory-platform-tasks"
  description = "Dagster service and run tasks: no inbound"
  vpc_id      = aws_vpc.platform.id
}

resource "aws_vpc_security_group_egress_rule" "tasks_https" {
  security_group_id = aws_security_group.tasks.id
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "tasks_database" {
  security_group_id            = aws_security_group.tasks.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.database.id
}

resource "aws_security_group" "database" {
  name        = "perfectory-platform-database"
  description = "Postgres: from the tasks only"
  vpc_id      = aws_vpc.platform.id
}

resource "aws_vpc_security_group_ingress_rule" "database_from_tasks" {
  security_group_id            = aws_security_group.database.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.tasks.id
}

resource "aws_db_subnet_group" "platform" {
  name       = local.database.identifier
  subnet_ids = [for subnet in aws_subnet.private : subnet.id]
}

resource "aws_db_instance" "platform" {
  identifier                  = local.database.identifier
  engine                      = "postgres"
  engine_version              = local.database.engine_version
  instance_class              = local.database.instance_class
  allocated_storage           = local.database.allocated_storage_gb
  storage_type                = "gp3"
  storage_encrypted           = true
  db_name                     = local.database.name
  username                    = local.database.master_username
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.platform.name
  vpc_security_group_ids      = [aws_security_group.database.id]
  publicly_accessible         = false
  multi_az                    = false
  backup_retention_period     = local.database.backup_retention_days
  copy_tags_to_snapshot       = true
  auto_minor_version_upgrade  = true
  deletion_protection         = true
  skip_final_snapshot         = false
  final_snapshot_identifier   = "${local.database.identifier}-final"
  lifecycle {
    prevent_destroy = true
  }
}

# The database's own role (database-bootstrap.sql): Dagster and its runs connect as it, never as the
# master user. Its secret has the shape Secrets Manager's RDS rotation reads, so rotation can be turned
# on later. The password is generated at apply time and written write-only: it is never in the state.
# A new password: raise secret_string_wo_version, apply, then run the bootstrap task again.
resource "aws_secretsmanager_secret" "dagster_database" {
  name        = local.database.app_secret
  description = "The Dagster database's own role (not the RDS master user)."
}

ephemeral "aws_secretsmanager_random_password" "dagster_database" {
  password_length     = 40
  exclude_punctuation = true
}

resource "aws_secretsmanager_secret_version" "dagster_database" {
  secret_id = aws_secretsmanager_secret.dagster_database.id
  secret_string_wo = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.platform.address
    port     = aws_db_instance.platform.port
    dbname   = local.database.name
    username = local.database.app_username
    password = ephemeral.aws_secretsmanager_random_password.dagster_database.random_password
  })
  secret_string_wo_version = 1
}

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.contract.account.id]
    }
  }
}

# What ECS itself needs to start the task: write its logs and read the role's password it injects (only
# the role's: orchestration/tests/test_aws_dagster_secrets.py holds every block but the bootstrap's to that).
resource "aws_iam_role" "dagster_execution" {
  name               = "perfectory-platform-dagster-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "dagster_execution" {
  role       = aws_iam_role.dagster_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "dagster_execution" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.dagster_database.arn]
  }
}

resource "aws_iam_role_policy" "dagster_execution" {
  name   = "database-password"
  role   = aws_iam_role.dagster_execution.id
  policy = data.aws_iam_policy_document.dagster_execution.json
}

# What Dagster does (root ADR-0616 §2-3): launch and watch its run tasks in this cluster, register the run
# task definitions EcsRunLauncher derives from its own, pass its two roles to them, and let an operator in
# by ECS Exec. It reads no job's secret.
resource "aws_iam_role" "dagster" {
  name               = "perfectory-platform-dagster"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "dagster" {
  statement {
    sid       = "LaunchRunsInThisCluster"
    actions   = ["ecs:RunTask"]
    resources = ["arn:aws:ecs:${local.contract.region}:${local.contract.account.id}:task-definition/*"]
    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.platform.arn]
    }
  }
  statement {
    sid       = "WatchAndStopRuns"
    actions   = ["ecs:DescribeTasks", "ecs:StopTask"]
    resources = ["arn:aws:ecs:${local.contract.region}:${local.contract.account.id}:task/${aws_ecs_cluster.platform.name}/*"]
  }
  # RegisterTaskDefinition, DescribeTaskDefinition and the tagging of a task at launch take no resource
  # narrower than "*"; DescribeNetworkInterfaces is how the launcher reads the service task's subnets.
  statement {
    sid = "DeriveRunTaskDefinitions"
    actions = [
      "ecs:RegisterTaskDefinition",
      "ecs:DescribeTaskDefinition",
      "ecs:TagResource",
      "ec2:DescribeNetworkInterfaces",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "PassItsRolesToRuns"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.dagster.arn, aws_iam_role.dagster_execution.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
  statement {
    sid       = "ReadRunLogs"
    actions   = ["logs:GetLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dagster.arn}:*"]
  }
  statement {
    sid = "EcsExec"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "dagster" {
  name   = "dagster"
  role   = aws_iam_role.dagster.id
  policy = data.aws_iam_policy_document.dagster.json
}

locals {
  # The run tasks EcsRunLauncher starts copy the daemon's container definition, so they inherit these.
  dagster_environment = [
    { name = "DAGSTER_PG_HOST", value = aws_db_instance.platform.address },
    { name = "DAGSTER_PG_PORT", value = tostring(aws_db_instance.platform.port) },
    { name = "DAGSTER_PG_DB", value = local.database.name },
    { name = "DAGSTER_PG_USERNAME", value = local.database.app_username },
    # The image a run task runs (EcsRunLauncher refuses to launch without it for a location in this image).
    { name = "DAGSTER_CURRENT_IMAGE", value = var.dagster_image },
  ]
  # The database role's password, by reference: the task definition holds the secret's ARN, never the
  # value, and never the master user's (the run tasks inherit this list).
  dagster_secrets = [
    { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.dagster_database.arn}:password::" },
  ]
  dagster_container = {
    image                  = var.dagster_image
    essential              = true
    environment            = local.dagster_environment
    secrets                = local.dagster_secrets
    readonlyRootFilesystem = false
    linuxParameters        = { initProcessEnabled = true }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.dagster.name
        awslogs-region        = local.contract.region
        awslogs-stream-prefix = "dagster"
      }
    }
  }
}

resource "aws_ecs_task_definition" "dagster" {
  family                   = "perfectory-platform-dagster"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = local.dagster.cpu
  memory                   = local.dagster.memory
  execution_role_arn       = aws_iam_role.dagster_execution.arn
  task_role_arn            = aws_iam_role.dagster.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([
    merge(local.dagster_container, {
      name    = "webserver"
      command = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", local.dagster.workspace]
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/server_info', timeout=5)"]
        interval    = 30
        timeout     = 10
        retries     = 3
        startPeriod = 120
      }
    }),
    merge(local.dagster_container, {
      name    = "daemon"
      command = ["dagster-daemon", "run", "-w", local.dagster.workspace]
      healthCheck = {
        command     = ["CMD", "dagster-daemon", "liveness-check"]
        interval    = 60
        timeout     = 30
        retries     = 3
        startPeriod = 180
      }
    }),
  ])
  # The role's secret has its value before a task reads it.
  depends_on = [aws_secretsmanager_secret_version.dagster_database]
}

# The one-off bootstrap (database-bootstrap.sql) and the only thing that reads the master user's secret:
# its own execution role injects that and the role's password into one psql run. Started by the runbook,
# never by Dagster.
resource "aws_iam_role" "database_bootstrap_execution" {
  name               = "perfectory-platform-database-bootstrap-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "database_bootstrap_execution" {
  role       = aws_iam_role.database_bootstrap_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "database_bootstrap_execution" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_db_instance.platform.master_user_secret[0].secret_arn,
      aws_secretsmanager_secret.dagster_database.arn,
    ]
  }
}

resource "aws_iam_role_policy" "database_bootstrap_execution" {
  name   = "database-passwords"
  role   = aws_iam_role.database_bootstrap_execution.id
  policy = data.aws_iam_policy_document.database_bootstrap_execution.json
}

resource "aws_ecs_task_definition" "database_bootstrap" {
  family                   = "perfectory-platform-database-bootstrap"
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
    image      = "postgres:17-alpine@${local.versions.container_images["postgres:17-alpine"]}"
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = ["printf '%s' \"$BOOTSTRAP_SQL\" | psql --no-psqlrc --set app_user=\"$APP_USER\" --set db_name=\"$DB_NAME\" --set app_password=\"$APP_PASSWORD\" --file -"]
    environment = [
      { name = "PGHOST", value = aws_db_instance.platform.address },
      { name = "PGPORT", value = tostring(aws_db_instance.platform.port) },
      { name = "PGUSER", value = local.database.master_username },
      { name = "PGDATABASE", value = "postgres" },
      { name = "PGSSLMODE", value = "require" },
      { name = "APP_USER", value = local.database.app_username },
      { name = "DB_NAME", value = local.database.name },
      { name = "BOOTSTRAP_SQL", value = file("${path.module}/database-bootstrap.sql") },
    ]
    secrets = [
      { name = "PGPASSWORD", valueFrom = "${aws_db_instance.platform.master_user_secret[0].secret_arn}:password::" },
      { name = "APP_PASSWORD", valueFrom = "${aws_secretsmanager_secret.dagster_database.arn}:password::" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.dagster.name
        awslogs-region        = local.contract.region
        awslogs-stream-prefix = "database-bootstrap"
      }
    }
  }])
  depends_on = [aws_secretsmanager_secret_version.dagster_database]
}

resource "aws_ecs_service" "dagster" {
  name                   = local.dagster.service
  cluster                = aws_ecs_cluster.platform.id
  task_definition        = aws_ecs_task_definition.dagster.arn
  desired_count          = 1
  launch_type            = "FARGATE"
  platform_version       = "LATEST"
  enable_execute_command = true
  propagate_tags         = "SERVICE"
  # One daemon at a time: a deploy stops the old task before it starts the new one.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = [for subnet in aws_subnet.public : subnet.id]
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = true
  }
}

resource "aws_sns_topic" "alarms" {
  name = local.contract.alarms.topic
}

# ECS publishes a running service's CPU every minute; with no task there is no datapoint, which counts
# as breaching. So the alarm fires when Dagster has not run for ten minutes, whatever stopped it.
resource "aws_cloudwatch_metric_alarm" "dagster_not_running" {
  alarm_name          = "perfectory-platform-dagster-not-running"
  alarm_description   = "The Dagster service (webserver and daemon) has had no running task for 10 minutes."
  namespace           = "AWS/ECS"
  metric_name         = "CPUUtilization"
  dimensions          = { ClusterName = aws_ecs_cluster.platform.name, ServiceName = aws_ecs_service.dagster.name }
  statistic           = "SampleCount"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

output "ecs_cluster" {
  value = aws_ecs_cluster.platform.name
}

output "dagster_service" {
  value = aws_ecs_service.dagster.name
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarms.arn
}

output "database_bootstrap_task_definition" {
  value = aws_ecs_task_definition.database_bootstrap.family
}
