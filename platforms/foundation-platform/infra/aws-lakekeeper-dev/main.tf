# The Lakekeeper dev rig (W109, coordinator 2026-11-27). The owner chose self-hosted open-source Lakekeeper as the
# catalog's end state if the cutover-gate tests of the concepts brief "Lakekeeper to the limit" pass; this stack is
# where they run, in dev only. Names and sizes are config/lakekeeper-dev.contract.json's; the account, the region,
# the tags, the state bucket and the platform's network and database are config/aws-platform.contract.json's.
#
# What it makes (every name says lakekeeper-dev): its own ECS cluster; OpenFGA (authz.tf) and Lakekeeper
# (catalog.tf) on Fargate ARM, each with its own database and login on the platform's RDS instance and its
# migration as a one-off task (database.tf); a Cognito user pool whose app clients are the machine identities
# (identity.tf); a private DNS name as the internal endpoint; and one probe task definition per principal that
# runs the tests from inside the VPC (probes.tf, probes/). It reads the platform stack by name and changes nothing
# of it but ingress rules on the database's security group. It makes no warehouse: the warehouse's R2 storage
# credential waits for the owner's decision on the Cloudflare parent token.
#
# Applied from the laptop with the SSO profile (docs/runbooks/lakekeeper-dev.md). Destroyed when the tests are done.

locals {
  platform = jsondecode(file("${path.module}/../../config/aws-platform.contract.json"))
  dev      = jsondecode(file("${path.module}/../../config/lakekeeper-dev.contract.json"))
  versions = jsondecode(file("${path.module}/../../../../tools/technology-versions.contract.json"))
  name     = local.dev.name
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
    key                 = local.dev.state_key
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
    tags = merge(local.platform.tags, {
      environment = "dev"
      purpose     = "lakekeeper-dev-rig"
    })
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

data "aws_db_instance" "platform" {
  db_instance_identifier = local.platform.database.identifier
}

data "aws_security_group" "database" {
  vpc_id = data.aws_vpc.platform.id
  name   = "perfectory-platform-database"
}

resource "aws_ecs_cluster" "dev" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_cloudwatch_log_group" "dev" {
  name              = "/${local.name}"
  retention_in_days = local.dev.log_retention_days
}

# The internal endpoint: catalog.<namespace> and authz.<namespace>, a private hosted zone of the platform's VPC.
resource "aws_service_discovery_private_dns_namespace" "dev" {
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
  logs = { for prefix in ["database-bootstrap", "authz-migrate", "authz", "catalog-migrate", "catalog", "probe"] : prefix => {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.dev.name
      awslogs-region        = local.region
      awslogs-stream-prefix = prefix
    }
  } }
  # How a one-off task is run and waited for: the platform's own rollout step (infra/aws/pre-deploy.sh), which
  # fails unless every container exited 0 and never prints a log line.
  one_off_script = "${path.module}/../aws/pre-deploy.sh"
}

output "cluster" {
  value = aws_ecs_cluster.dev.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.dev.name
}

output "subnets" {
  value = join(",", local.subnets)
}
