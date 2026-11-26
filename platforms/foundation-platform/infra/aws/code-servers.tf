# Dagster's code locations on AWS (root ADR-0616 §1, §2-2, coordinator 2026-11-26): one ECS service per
# location, each a gRPC code server (`dagster code-server start`) from that location's image, the layout of
# Dagster's own ECS deployment guide. The webserver and the daemon (dagster.tf) reach a location by its name in
# a private DNS namespace (Cloud Map) and run an image of their own that changes only with Dagster's version
# and instance files, so a code release restarts the code server alone and the daemon's schedules keep their
# ticks. The code server reports its image to the run launcher (DAGSTER_CURRENT_IMAGE), so a run task runs its
# location's image. Locations are the contract's (dagster.code_locations); no location is named here.

locals {
  code_locations = local.dagster.code_locations
}

resource "aws_service_discovery_private_dns_namespace" "platform" {
  name = local.dagster.namespace
  vpc  = aws_vpc.platform.id
}

resource "aws_service_discovery_service" "code_location" {
  for_each = local.code_locations
  name     = each.key
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.platform.id
    routing_policy = "MULTIVALUE"
    dns_records {
      type = "A"
      ttl  = 10
    }
  }
  # ECS registers a task once it runs and deregisters it when it stops; the container's own health check
  # decides when it runs.
  health_check_custom_config {}
}

# The webserver and the daemon call the code servers; both are in the tasks' security group, which still
# lets nothing in from outside it. The run tasks are in it too (EcsRunLauncher copies the daemon's subnet and
# groups; its run_task_kwargs take only literal ids, which the image cannot know), so a run can reach a code
# server's port. What that gives it: the code server's role, which may only publish to the alarm topic; a run
# already holds the Dagster database role it shares with the code server. A group of the runs' own needs the
# run network named at deploy time (W107 report, not decided here).
resource "aws_vpc_security_group_ingress_rule" "code_servers" {
  for_each                     = local.code_locations
  security_group_id            = aws_security_group.tasks.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  referenced_security_group_id = aws_security_group.tasks.id
}

resource "aws_vpc_security_group_egress_rule" "code_servers" {
  for_each                     = local.code_locations
  security_group_id            = aws_security_group.tasks.id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  referenced_security_group_id = aws_security_group.tasks.id
}

# What a code server does with AWS: its sensors publish metadata-only notifications to the alarm topic
# (orchestration/dagster/aws/alerts.py). Nothing else; no run carries this role.
resource "aws_iam_role" "code_server" {
  name               = "perfectory-platform-code-server"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "code_server" {
  statement {
    sid       = "PublishAlarms"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alarms.arn]
  }
}

resource "aws_iam_role_policy" "code_server" {
  name   = "code-server"
  role   = aws_iam_role.code_server.id
  policy = data.aws_iam_policy_document.code_server.json
}

resource "aws_ecs_task_definition" "code_server" {
  for_each                 = local.code_locations
  family                   = "perfectory-platform-code-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  # The database role's password, which its sensors read the instance with (dagster.tf).
  execution_role_arn = aws_iam_role.dagster_execution.arn
  task_role_arn      = aws_iam_role.code_server.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name      = "code-server"
    image     = var.dagster_image
    essential = true
    command = ["dagster", "code-server", "start", "--host", "0.0.0.0", "--port", tostring(each.value.port),
    "--python-file", each.value.python_file, "--location-name", each.key]
    environment = concat(local.dagster_database_environment, [
      { name = "DAGSTER_CURRENT_IMAGE", value = var.dagster_image },
      { name = "DAGSTER_ALARM_TOPIC_ARN", value = aws_sns_topic.alarms.arn },
    ])
    secrets                = local.dagster_secrets
    readonlyRootFilesystem = false
    linuxParameters        = { initProcessEnabled = true }
    healthCheck = {
      command     = ["CMD", "dagster", "api", "grpc-health-check", "--port", tostring(each.value.port)]
      interval    = 30
      timeout     = 10
      retries     = 3
      startPeriod = 120
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.dagster.name
        awslogs-region        = local.contract.region
        awslogs-stream-prefix = "code-${each.key}"
      }
    }
  }])
  depends_on = [aws_secretsmanager_secret_version.dagster_database]
}

resource "aws_ecs_service" "code_server" {
  for_each               = local.code_locations
  name                   = "code-${each.key}"
  cluster                = aws_ecs_cluster.platform.id
  task_definition        = aws_ecs_task_definition.code_server[each.key].arn
  desired_count          = 1
  launch_type            = "FARGATE"
  platform_version       = "LATEST"
  enable_execute_command = false
  propagate_tags         = "SERVICE"
  # A new code server starts and registers before the old one stops: the location is never without one.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = [for subnet in aws_subnet.public : subnet.id]
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = true
  }
  service_registries {
    registry_arn = aws_service_discovery_service.code_location[each.key].arn
  }
  wait_for_steady_state = true
  force_new_deployment  = true
  triggers = {
    database_password = aws_secretsmanager_secret_version.dagster_database.secret_string_wo_version
  }
  # A location's code reads the schema the release migrated (foundation-database.tf).
  depends_on = [terraform_data.database_migrate]
}

output "code_servers" {
  value = { for name, service in aws_ecs_service.code_server : name => service.name }
}
