# Dagster on AWS (root ADR-0616 A1): its cluster, its database and the role it connects as, its always-on
# services (the webserver and the daemon, each its own), the roles its run tasks may carry, and the alarms
# that say one is not running or the disk is filling. Names and sizes are the contract's
# (config/aws-platform.contract.json: ecs, dagster, database, alarms); the image is the release's Dagster
# image by digest (root ADR-0620), given at apply time. Collectors move in from A2, one at a time.

variable "dagster_image" {
  description = "The release's AWS code-location image by digest, from the commit's verified record (scripts/deploy/aws-release-images.py writes it). The service runs it, and every run task Dagster launches runs it too."
  type        = string
  validation {
    condition     = startswith(var.dagster_image, "${local.contract.images.repository}@") && can(regex("^sha256:[0-9a-f]{64}$", trimprefix(var.dagster_image, "${local.contract.images.repository}@")))
    error_message = "The image is the contract's release repository (images.repository) by digest: never a tag, never another repository."
  }
}

variable "dagster_service_image" {
  description = "The release's AWS Dagster image by digest (the webserver and the daemon: Dagster, its instance and workspace files, no location's code), from the commit's verified record (scripts/deploy/aws-release-images.py writes it). It changes only when its inputs do, so a code release leaves the service running."
  type        = string
  validation {
    condition     = startswith(var.dagster_service_image, "${local.contract.images.repository}@") && can(regex("^sha256:[0-9a-f]{64}$", trimprefix(var.dagster_service_image, "${local.contract.images.repository}@")))
    error_message = "The image is the contract's release repository (images.repository) by digest: never a tag, never another repository."
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
  max_allocated_storage       = local.database.max_allocated_storage_gb
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
# A new password: raise secret_string_wo_version and apply. The rollout's bootstrap sets it, then the
# service starts again with it (the service's triggers); a run task reads it when it starts.
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
# the role's: orchestration/tests/test_aws_dagster_secrets.py holds every block but the one-off tasks' to that).
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

# What Dagster does (root ADR-0616 §2-3): launch and watch its run tasks in this cluster, register their task
# definitions, pass them a role under the run path and nothing else, read a failed run's last log lines, and
# let an operator in by ECS Exec. It reads no job's secret, and no run carries its role.
resource "aws_iam_role" "dagster" {
  name               = "perfectory-platform-dagster"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "dagster" {
  statement {
    sid     = "LaunchRunsInThisCluster"
    actions = ["ecs:RunTask"]
    # Only the family EcsRunLauncher registers runs in (every revision): never the database bootstrap,
    # which carries the master user's secret (orchestration/tests/test_aws_dagster_secrets.py).
    resources = ["arn:aws:ecs:${local.contract.region}:${local.contract.account.id}:task-definition/${local.dagster.run_task_definition_prefix}_*"]
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
  # A run's task definition names the run execution role and the default run role; a lane's run overrides
  # the task role with its own (ecs/task_overrides). All of them are under the run path, Dagster's own are not.
  statement {
    sid       = "PassRunRoles"
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${local.contract.account.id}:role${local.contract.ecs.run_role_path}*"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
  # Run monitoring quotes a failed run's last lines (dagster-aws get_task_logs): the run family's streams only,
  # never the one-off tasks' or the service's own.
  statement {
    sid       = "ReadRunLogs"
    actions   = ["logs:GetLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dagster.arn}:log-stream:${local.dagster.run_task_definition_prefix}_*"]
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

# The roles a run task carries (root ADR-0616 §2-3, §2-8), under the contract's run path. The default task
# role has no policy: a run whose lane names no role of its own reaches nothing. Fail closed, so no lane
# has to remember to narrow its run. A lane's own role lives under the same path (collect.tf).
resource "aws_iam_role" "run_default" {
  name               = "perfectory-platform-run-default"
  path               = local.contract.ecs.run_role_path
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

# What ECS needs to start a run task: write its log stream and inject the database role's password, which
# the run's worker writes its events with. Nothing else, no registry: the images are public on GHCR.
resource "aws_iam_role" "run_execution" {
  name               = "perfectory-platform-run-execution"
  path               = local.contract.ecs.run_role_path
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "run_execution" {
  statement {
    sid       = "WriteRunLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dagster.arn}:log-stream:${local.dagster.run_task_definition_prefix}_*"]
  }
  statement {
    sid       = "InjectTheDatabaseRolePassword"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.dagster_database.arn]
  }
}

resource "aws_iam_role_policy" "run_execution" {
  name   = "run-execution"
  role   = aws_iam_role.run_execution.id
  policy = data.aws_iam_policy_document.run_execution.json
}

locals {
  # The database role's password, by reference: a task definition holds the secret's ARN, never the value,
  # and never the master user's. The service and every run task (dagster.yaml's run_launcher) name this one.
  dagster_password = "${aws_secretsmanager_secret.dagster_database.arn}:password::"
  # Where Dagster's storage is: the service, the code servers (code-servers.tf) and every run task.
  dagster_database_environment = [
    { name = "DAGSTER_PG_HOST", value = aws_db_instance.platform.address },
    { name = "DAGSTER_PG_PORT", value = tostring(aws_db_instance.platform.port) },
    { name = "DAGSTER_PG_DB", value = local.database.name },
    { name = "DAGSTER_PG_USERNAME", value = local.database.app_username },
  ]
  # What the service adds: dagster.yaml's run_launcher registers each run's task definition from the
  # DAGSTER_RUN_* names, the log group and the password's reference, and hands the four DAGSTER_PG_* on. A
  # run's image is its location's: the code server reports it (code-servers.tf).
  dagster_environment = [
    { name = "DAGSTER_PG_PASSWORD_SECRET", value = local.dagster_password },
    # The family the launcher registers runs in; the role may start that family only.
    { name = "DAGSTER_RUN_TASK_DEFINITION_PREFIX", value = local.dagster.run_task_definition_prefix },
    { name = "DAGSTER_RUN_TASK_ROLE_ARN", value = aws_iam_role.run_default.arn },
    { name = "DAGSTER_RUN_EXECUTION_ROLE_ARN", value = aws_iam_role.run_execution.arn },
    { name = "DAGSTER_LOG_GROUP", value = aws_cloudwatch_log_group.dagster.name },
  ]
  dagster_secrets = [
    { name = "DAGSTER_PG_PASSWORD", valueFrom = local.dagster_password },
  ]
  dagster_container = {
    image                  = var.dagster_service_image
    essential              = true
    environment            = concat(local.dagster_database_environment, local.dagster_environment)
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

locals {
  # What each always-on process runs and how its service deploys; the contract's dagster.services names each
  # one and its size, under the same key. Both run the same container settings (local.dagster_container) and
  # roles: the webserver too stops a run's task when an operator terminates it (EcsRunLauncher.terminate).
  dagster_processes = {
    webserver = {
      command = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", local.dagster.workspace]
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/server_info', timeout=5)"]
        interval    = 30
        timeout     = 10
        retries     = 3
        startPeriod = 120
      }
      # Any number of webservers may run: a deploy starts the new task before it stops the old one.
      minimum_healthy_percent = 100
      maximum_percent         = 200
    }
    daemon = {
      command = ["dagster-daemon", "run", "-w", local.dagster.workspace]
      healthCheck = {
        command     = ["CMD", "dagster-daemon", "liveness-check"]
        interval    = 60
        timeout     = 30
        retries     = 3
        startPeriod = 180
      }
      # One daemon at a time (a deployment has a single daemon): a deploy stops the old task before it
      # starts the new one.
      minimum_healthy_percent = 0
      maximum_percent         = 100
    }
  }
}

resource "aws_ecs_task_definition" "dagster" {
  for_each                 = local.dagster.services
  family                   = "perfectory-platform-dagster-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.dagster_execution.arn
  task_role_arn            = aws_iam_role.dagster.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([
    merge(local.dagster_container, {
      name        = each.key
      command     = local.dagster_processes[each.key].command
      healthCheck = local.dagster_processes[each.key].healthCheck
    }),
  ])
  # The role's secret has its value before a task reads it.
  depends_on = [aws_secretsmanager_secret_version.dagster_database]
}

# Until 2026-11-27 one service (`dagster`) ran both processes in one task. It moves to the daemon's address,
# so the plan replaces it, and a replacement destroys before it creates (no create_before_destroy): AWS
# provider 6.72.0 sets its desired count to 0, deletes it and waits until it is INACTIVE (its task stopping
# or stopped) before the daemon's new service is created, as a deploy of the one daemon does.
moved {
  from = aws_ecs_task_definition.dagster
  to   = aws_ecs_task_definition.dagster["daemon"]
}

moved {
  from = aws_ecs_service.dagster
  to   = aws_ecs_service.dagster["daemon"]
}

moved {
  from = aws_cloudwatch_metric_alarm.dagster_not_running
  to   = aws_cloudwatch_metric_alarm.dagster_not_running["daemon"]
}

resource "aws_ecs_service" "dagster" {
  for_each                           = local.dagster.services
  name                               = each.value.name
  cluster                            = aws_ecs_cluster.platform.id
  task_definition                    = aws_ecs_task_definition.dagster[each.key].arn
  desired_count                      = 1
  launch_type                        = "FARGATE"
  platform_version                   = "LATEST"
  enable_execute_command             = true
  propagate_tags                     = "SERVICE"
  deployment_minimum_healthy_percent = local.dagster_processes[each.key].minimum_healthy_percent
  deployment_maximum_percent         = local.dagster_processes[each.key].maximum_percent
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = [for subnet in aws_subnet.public : subnet.id]
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = true
  }
  # The apply waits until the new task runs and fails if ECS rolled the deployment back (AWS provider
  # 6.72.0 waitServiceStable: ROLLBACK_SUCCESSFUL is an error), so a green apply means the new task runs.
  wait_for_steady_state = true
  # ECS injects the password when a task starts: a new password starts the service again, after the
  # bootstrap set it (depends_on below).
  force_new_deployment = true
  triggers = {
    database_password = aws_secretsmanager_secret_version.dagster_database.secret_string_wo_version
  }
  # The rollout migrates first: the service moves to a new task definition only after the bootstrap and the
  # migrate task exited 0 (foundation-database.tf).
  depends_on = [terraform_data.database_migrate]
}

resource "aws_sns_topic" "alarms" {
  name = local.contract.alarms.topic
}

# ECS publishes a running service's CPU every minute; with no task there is no datapoint, which counts
# as breaching. So an alarm fires when its process has not run for ten minutes, whatever stopped it.
resource "aws_cloudwatch_metric_alarm" "dagster_not_running" {
  for_each            = local.dagster.services
  alarm_name          = "perfectory-platform-dagster-${each.key}-not-running"
  alarm_description   = "The Dagster ${each.key} (service ${each.value.name}) has had no running task for 10 minutes."
  namespace           = "AWS/ECS"
  metric_name         = "CPUUtilization"
  dimensions          = { ClusterName = aws_ecs_cluster.platform.name, ServiceName = aws_ecs_service.dagster[each.key].name }
  statistic           = "SampleCount"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

# The disk grows by itself (max_allocated_storage): RDS adds storage when less than 10 % is free. This alarm
# is below that, so it fires only when the disk could not grow: at its ceiling, or within the six hours RDS
# waits between two growths. A full disk stops Dagster and every collector at once.
resource "aws_cloudwatch_metric_alarm" "database_storage_low" {
  alarm_name          = "perfectory-platform-database-storage-low"
  alarm_description   = "The platform database has less than 1 GiB free and its disk did not grow."
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  dimensions          = { DBInstanceIdentifier = aws_db_instance.platform.identifier }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1073741824
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

output "ecs_cluster" {
  value = aws_ecs_cluster.platform.name
}

output "dagster_services" {
  value = { for process, service in aws_ecs_service.dagster : process => service.name }
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarms.arn
}
