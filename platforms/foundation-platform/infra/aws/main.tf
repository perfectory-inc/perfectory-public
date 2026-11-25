# The base of the AWS platform account (root ADR-0619), where the data jobs move (root ADR-0616): the
# account's monthly budget, the state bucket this stack keeps its state in, and the network the tasks
# and the database will sit in; Dagster's place (root ADR-0616 A1) is dagster.tf. Every value is read
# from config/aws-platform.contract.json; nothing is copied here. The provider refuses every account
# but the contract's, so an apply under the wrong profile stops before it reads or changes anything.
# Applied from the laptop with the SSO profile after the PR merges (docs/runbooks/aws-platform.md);
# no host applies it.

locals {
  contract = jsondecode(file("${path.module}/../../config/aws-platform.contract.json"))
}

terraform {
  required_version = "1.13.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.72.0"
    }
  }
  # In the account itself, made by this stack (the runbook's first apply runs on a local state and
  # moves it here). OpenTofu resolves locals in the backend at init.
  backend "s3" {
    bucket              = local.contract.state.bucket
    key                 = local.contract.state.key
    region              = local.contract.region
    allowed_account_ids = [local.contract.account.id]
    use_lockfile        = true
    encrypt             = true
  }
}

variable "budget_alert_emails" {
  description = "The account's own registered address the budget mails. Given at apply time (TF_VAR_budget_alert_emails), never committed: this repository is public."
  type        = list(string)
  sensitive   = true
  validation {
    condition     = length(var.budget_alert_emails) > 0 && length(var.budget_alert_emails) <= 10
    error_message = "A budget that mails nobody is no alarm, and AWS Budgets takes at most 10 addresses."
  }
}

variable "vpc_cidr" {
  description = "The VPC's address block, given at apply time (TF_VAR_vpc_cidr) because this public repository holds no private addressing. Once the VPC exists the runbook reads it back from AWS."
  type        = string
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0)) && endswith(var.vpc_cidr, "/${local.contract.network.vpc_prefix_length}")
    error_message = "The VPC block is a CIDR of the contract's prefix length."
  }
}

provider "aws" {
  region              = local.contract.region
  allowed_account_ids = [local.contract.account.id]
  default_tags {
    tags = local.contract.tags
  }
}

# The first resource: the account's spend is watched before anything that costs exists.
resource "aws_budgets_budget" "monthly" {
  name         = local.contract.budget.name
  budget_type  = "COST"
  limit_amount = local.contract.budget.limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = local.contract.budget.notifications
    content {
      comparison_operator        = "GREATER_THAN"
      notification_type          = notification.value.type
      threshold                  = tonumber(notification.value.over_usd)
      threshold_type             = "ABSOLUTE_VALUE"
      subscriber_email_addresses = var.budget_alert_emails
    }
  }
}

resource "aws_s3_bucket" "state" {
  bucket = local.contract.state.bucket
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    id     = "earlier-states-leave"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = local.contract.state.noncurrent_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  depends_on = [aws_s3_bucket_versioning.state]
}

data "aws_iam_policy_document" "state" {
  statement {
    sid       = "TlsOnly"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  bucket     = aws_s3_bucket.state.id
  policy     = data.aws_iam_policy_document.state.json
  depends_on = [aws_s3_bucket_public_access_block.state]
}

# The network: no NAT gateway, no load balancer (root ADR-0616 §1). Tasks take a public IPv4 address
# in a public subnet and accept nothing inbound; the database sits in subnets with no route out.
resource "aws_vpc" "platform" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "perfectory-platform"
  }
  # A block other than the live one would replace the network; the plan stops here instead.
  lifecycle {
    prevent_destroy = true
  }
}

# The VPC's default security group keeps no rule, so nothing that forgets to name its own group can talk.
resource "aws_default_security_group" "platform" {
  vpc_id = aws_vpc.platform.id
  tags = {
    Name = "perfectory-platform-default-closed"
  }
}

resource "aws_internet_gateway" "platform" {
  vpc_id = aws_vpc.platform.id
  tags = {
    Name = "perfectory-platform"
  }
}

resource "aws_subnet" "public" {
  for_each          = local.contract.network.public_subnets
  vpc_id            = aws_vpc.platform.id
  availability_zone = each.key
  cidr_block        = cidrsubnet(var.vpc_cidr, local.contract.network.subnet_newbits, each.value)
  # A task asks for its address (assignPublicIp); nothing gets one by landing here.
  map_public_ip_on_launch = false
  lifecycle {
    prevent_destroy = true
  }
  tags = {
    Name = "perfectory-platform-public-${each.key}"
    tier = "public"
  }
}

resource "aws_subnet" "private" {
  for_each          = local.contract.network.private_subnets
  vpc_id            = aws_vpc.platform.id
  availability_zone = each.key
  cidr_block        = cidrsubnet(var.vpc_cidr, local.contract.network.subnet_newbits, each.value)
  lifecycle {
    prevent_destroy = true
  }
  tags = {
    Name = "perfectory-platform-private-${each.key}"
    tier = "private"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.platform.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.platform.id
  }
  tags = {
    Name = "perfectory-platform-public"
  }
}

resource "aws_route_table_association" "public" {
  for_each       = aws_subnet.public
  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

# The VPC's local route only: what sits here reaches nothing outside the VPC.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.platform.id
  tags = {
    Name = "perfectory-platform-private"
  }
}

resource "aws_route_table_association" "private" {
  for_each       = aws_subnet.private
  subnet_id      = each.value.id
  route_table_id = aws_route_table.private.id
}

# Names and ids only, no secret: the runbook's checks read them.
output "vpc_id" {
  value = aws_vpc.platform.id
}

output "public_subnet_ids" {
  value = { for zone, subnet in aws_subnet.public : zone => subnet.id }
}

output "private_subnet_ids" {
  value = { for zone, subnet in aws_subnet.private : zone => subnet.id }
}
