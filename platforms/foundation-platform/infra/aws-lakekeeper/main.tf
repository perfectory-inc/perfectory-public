# The lakehouse catalog, self-hosted open-source Lakekeeper (root ADR-0624, accepted by the owner 2026-11-27), in two
# environments from one module: dev, the test rig and upgrade canary (W109), and prod, the catalog Silver and Gold
# commit to (step A1). var.environment names the contract, config/lakekeeper-<environment>.contract.json, which
# holds every name, size and difference (the state key too, so a plan can never mix the two); the account, the
# region, the tags, the state bucket and the platform's network are config/aws-platform.contract.json's.
#
# What it makes (every name is the contract's name): its own ECS cluster; OpenFGA (authz.tf) and Lakekeeper
# (catalog.tf) on Fargate ARM, each with its own database and login, made by a one-off bootstrap task, and its
# migration as a one-off task (database.tf); dev's databases are on the platform's RDS instance, prod's on an
# instance of its own (database.tf, root ADR-0624 decision 4); a Cognito user pool whose app clients are the
# machine identities (identity.tf); a private DNS name as the internal endpoint; the reconciler task that makes the
# warehouse, namespaces, declared tables and grants and checks drift (reconciler.tf); the warehouse's R2
# credential slot (warehouse.tf); prod's alarms (alarms.tf); and dev's probes (probes.tf, logscan.tf,
# maintenance.tf). It reads the platform stack by name and changes nothing of it but, in dev, ingress rules on the
# platform database's security group.
#
# Applied from the laptop with the SSO profile (docs/runbooks/lakekeeper.md).

variable "environment" {
  description = "dev or prod: names the contract config/lakekeeper-<environment>.contract.json and so the state key. Give it at init and at plan."
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "dev or prod."
  }
}

variable "code_location_image" {
  description = "The release's AWS code-location image by digest (aws-release-images.py's dagster_image), whose publisher the reconciler (and in dev the rust probe) runs; null: neither exists."
  type        = string
  default     = null
  validation {
    condition     = var.code_location_image == null || (startswith(coalesce(var.code_location_image, "-"), "${local.platform.images.repository}@") && can(regex("^sha256:[0-9a-f]{64}$", trimprefix(coalesce(var.code_location_image, "-"), "${local.platform.images.repository}@"))))
    error_message = "The image is the platform contract's release repository (images.repository) by digest: never a tag, never another repository."
  }
}

locals {
  platform = jsondecode(file("${path.module}/../../config/aws-platform.contract.json"))
  env      = jsondecode(file("${path.module}/../../config/lakekeeper-${var.environment}.contract.json"))
  versions = jsondecode(file("${path.module}/../../../../tools/technology-versions.contract.json"))
  name     = local.env.name
  # The operator: the principal the reconciler (and dev's warehouse probe) runs as; it bootstraps Lakekeeper.
  operator = local.env.identity.operator
  region   = local.platform.region
  account  = local.platform.account.id
}

terraform {
  required_version = "1.13.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.72.0"
    }
  }
  # The platform's state bucket, under a key of its own: this stack's state never mixes with the platform's.
  backend "s3" {
    bucket              = local.platform.state.bucket
    key                 = local.env.state_key
    region              = local.platform.region
    allowed_account_ids = [local.platform.account.id]
    use_lockfile        = true
    encrypt             = true
  }
}

provider "aws" {
  region              = local.region
  allowed_account_ids = [local.account]
  default_tags {
    tags = merge(local.platform.tags, { environment = var.environment }, local.env.tags)
  }
}

# The platform stack's network and database, by the names it gives them (infra/aws).
data "aws_vpc" "platform" {
  tags = { Name = "perfectory-platform" }
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.platform.id]
  }
  tags = { tier = "public" }
}

# The platform's database, for an environment whose databases live on it (dev).
data "aws_db_instance" "platform" {
  count                  = local.env.databases.instance == null ? 1 : 0
  db_instance_identifier = local.platform.database.identifier
}

data "aws_security_group" "platform_database" {
  count  = local.env.databases.instance == null ? 1 : 0
  vpc_id = data.aws_vpc.platform.id
  name   = "perfectory-platform-database"
}

moved {
  from = aws_ecs_cluster.dev
  to   = aws_ecs_cluster.lakekeeper
}

moved {
  from = aws_cloudwatch_log_group.dev
  to   = aws_cloudwatch_log_group.lakekeeper
}

moved {
  from = aws_service_discovery_private_dns_namespace.dev
  to   = aws_service_discovery_private_dns_namespace.lakekeeper
}

resource "aws_ecs_cluster" "lakekeeper" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_cloudwatch_log_group" "lakekeeper" {
  name              = "/${local.name}"
  retention_in_days = local.env.log_retention_days
}

# The internal endpoint: catalog.<namespace> and authz.<namespace>, a private hosted zone of the platform's VPC.
resource "aws_service_discovery_private_dns_namespace" "lakekeeper" {
  name = "${local.name}.internal"
  vpc  = data.aws_vpc.platform.id
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
      values   = [local.account]
    }
  }
}

locals {
  subnets = sort(data.aws_subnets.public.ids)
  # Every container logs to the rig's group under its own prefix.
  logs = { for prefix in ["database-bootstrap", "authz-migrate", "authz", "catalog-migrate", "catalog", "reconciler", "probe"] : prefix => {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.lakekeeper.name
      awslogs-region        = local.region
      awslogs-stream-prefix = prefix
    }
  } }
  # How a one-off task is run and waited for: the platform's own rollout step (infra/aws/pre-deploy.sh), which
  # fails unless every container exited 0 and never prints a log line.
  one_off_script = "${path.module}/../aws/pre-deploy.sh"
}

output "cluster" {
  value = aws_ecs_cluster.lakekeeper.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.lakekeeper.name
}

output "subnets" {
  value = join(",", local.subnets)
}
