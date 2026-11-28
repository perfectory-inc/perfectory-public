# The credential vendor and the lanes' run roles (root ADR-0623 §2). A lane is data (config/aws-platform.contract.json,
# collect.lanes: its role's suffix and its scope); no block here names one. Only the vendor's object half is here
# (coordinator 2026-11-27): a lane reaches a table through the Lakekeeper catalog (root ADR-0624), with its own client,
# whose slot infra/aws-lakekeeper makes for each lane config/lakekeeper-declaration.v1.json grants, and which the lane's
# role alone reads.

locals {
  collect          = local.contract.collect
  project          = local.contract.tags.project
  vendor_name      = "${local.project}-r2-vend"
  lakehouse_bucket = jsondecode(file("${path.module}/../../config/r2-connections.contract.json")).connections.lakehouse.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  # A lane's own state prefix is generated from its id, beside the prefixes it declares (contract collect.state_reason).
  # The rollout's ledger import (ledger-import.tf) is one more alias: read-only on the host's ledger exports.
  vendor_lanes = merge({ for id, lane in local.collect.lanes : id => {
    write_prefixes = concat(lane.write_prefixes, ["${local.collect.state_prefix}${id}/"])
    read_prefixes  = try(lane.read_prefixes, [])
    } }, {
    (local.ledger_import) = {
      write_prefixes = ["${local.collect.state_prefix}${local.ledger_import}/"]
      read_prefixes  = ["control/evidence/ledger_move/"]
    }
  })
  # The lanes the Lakekeeper catalog grants a table (generated from the pipeline graph), and the catalog's name,
  # which names each one's client slot (<name>/principal-<lane>, infra/aws-lakekeeper/identity.tf).
  lakekeeper_lanes = jsondecode(file("${path.module}/../../config/lakekeeper-declaration.v1.json")).lanes
  lakekeeper_name  = jsondecode(file("${path.module}/../../config/lakekeeper-prod.contract.json")).name
  # Source key slots, each with the lanes whose commands read it (contract collect.source_secrets).
  source_secrets = { for name, slot in local.collect.source_secrets : name => {
    slot  = slot
    lanes = [for id, lane in local.collect.lanes : id if contains(lane.secrets, name)]
  } }
}

# The parent of every credential the vendor mints: a dedicated Cloudflare account API token, perfectory-r2-vend-prod, R2
# "Admin Read & Write" (R2's temp-access-credentials endpoint takes no narrower one; upstream Lakekeeper storage-s3
# "Cloudflare R2", v0.15.1), never CLOUDFLARE_API_TOKEN and never the Lakekeeper dev rig's token. The owner puts its
# value once (docs/runbooks/aws-platform.md); OpenTofu makes the slot only and never holds a value. The token is
# account-wide, so the slot's own policy refuses every reader but the vendor, the applier included.
resource "aws_secretsmanager_secret" "r2_mint_token" {
  name        = "${local.project}/r2-mint-token"
  description = "<account id>:<token> of the Cloudflare account API token perfectory-r2-vend-prod (R2 Admin Read & Write) the vendor mints R2 temporary credentials from. Put by the owner; read only by the vendor."
}

data "aws_iam_policy_document" "r2_mint_token" {
  statement {
    sid       = "OnlyTheVendorReads"
    effect    = "Deny"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotEquals"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.r2_vend.arn]
    }
  }
}

resource "aws_secretsmanager_secret_policy" "r2_mint_token" {
  secret_arn          = aws_secretsmanager_secret.r2_mint_token.arn
  policy              = data.aws_iam_policy_document.r2_mint_token.json
  block_public_policy = true
}

# A source's key (contract collect.source_secrets): put once by the owner (docs/runbooks/aws-platform.md), read only by
# the roles of the lanes whose commands read it; the slot's own policy refuses every other reader, the applier
# included. Until it holds a value those lanes ask their source nothing.
resource "aws_secretsmanager_secret" "source" {
  for_each    = local.source_secrets
  name        = "${local.project}/source/${each.value.slot}"
  description = "${each.key} of the lanes ${join(", ", each.value.lanes)}. Put by the owner; read only by those lanes' roles."
}

data "aws_iam_policy_document" "source_secret" {
  for_each = local.source_secrets
  statement {
    sid       = "OnlyItsLanesRead"
    effect    = "Deny"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotEquals"
      variable = "aws:PrincipalArn"
      values   = [for id in each.value.lanes : aws_iam_role.lane[id].arn]
    }
  }
}

resource "aws_secretsmanager_secret_policy" "source" {
  for_each            = local.source_secrets
  secret_arn          = aws_secretsmanager_secret.source[each.key].arn
  policy              = data.aws_iam_policy_document.source_secret[each.key].json
  block_public_policy = true
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.contract.account.id]
    }
  }
}

resource "aws_iam_role" "r2_vend" {
  name               = local.vendor_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_cloudwatch_log_group" "r2_vend" {
  name              = "/aws/lambda/${local.vendor_name}"
  retention_in_days = local.dagster.log_retention_days
}

# The vendor alone reads the minting token.
data "aws_iam_policy_document" "r2_vend" {
  statement {
    sid       = "ReadTheMintingToken"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.r2_mint_token.arn]
  }
  statement {
    sid       = "WriteItsLog"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.r2_vend.arn}:*"]
  }
}

resource "aws_iam_role_policy" "r2_vend" {
  name   = "vend"
  role   = aws_iam_role.r2_vend.id
  policy = data.aws_iam_policy_document.r2_vend.json
}

# The vendor's code and its lanes, one package: the lanes are data that grows with every lane, and Lambda holds at most
# 4 KB of environment (2026-11-28, W109e: eleven lanes measured 4,744 bytes and the update was refused). A new package
# is a new version, so every alias moves with it, as it did when the lanes were an environment variable.
data "archive_file" "r2_vend" {
  type        = "zip"
  output_path = "${path.module}/.build/r2-vend.zip"
  dynamic "source" {
    for_each = { for name in fileset("${path.module}/r2-vend", "*.py") : name => file("${path.module}/r2-vend/${name}") }
    content {
      filename = source.key
      content  = source.value
    }
  }
  source {
    filename = "lanes.json"
    content  = jsonencode(local.vendor_lanes)
  }
}

resource "aws_lambda_function" "r2_vend" {
  function_name    = local.vendor_name
  description      = "Vends a lane's R2 temporary credentials on its own prefixes (root ADR-0623 2, object half)."
  role             = aws_iam_role.r2_vend.arn
  runtime          = "python3.14"
  architectures    = ["arm64"]
  handler          = "vend.handler"
  filename         = data.archive_file.r2_vend.output_path
  source_code_hash = data.archive_file.r2_vend.output_base64sha256
  publish          = true
  timeout          = 60
  memory_size      = 128
  environment {
    variables = {
      MINT_SECRET            = aws_secretsmanager_secret.r2_mint_token.name
      BUCKET                 = local.lakehouse_bucket
      CREDENTIAL_TTL_SECONDS = tostring(local.collect.credential_ttl_seconds)
    }
  }
  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.r2_vend.name
  }
  depends_on = [aws_iam_role_policy.r2_vend]
}

# A lane is its alias: the vendor reads the lane from the alias it was invoked through (ADR-0623 §2-2).
resource "aws_lambda_alias" "lane" {
  for_each         = local.collect.lanes
  name             = each.key
  function_name    = aws_lambda_function.r2_vend.function_name
  function_version = aws_lambda_function.r2_vend.version
}

# A lane's run role, under the contract's run path, the only one Dagster may pass (dagster.tf): it may invoke its own alias, read the
# foundation database's runtime secret and its own source keys, nothing else. Its R2 rights are the vendor's credentials'.
resource "aws_iam_role" "lane" {
  for_each           = local.collect.lanes
  name               = "${local.project}-${each.value.role}"
  path               = local.contract.ecs.run_role_path
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "lane" {
  for_each = local.collect.lanes
  statement {
    sid       = "AskTheVendorForThisLane"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_alias.lane[each.key].arn]
  }
  statement {
    sid       = "ReadTheDatabaseRole"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.foundation_runtime.arn]
  }
  dynamic "statement" {
    for_each = length(each.value.secrets) > 0 ? [each.value.secrets] : []
    content {
      sid       = "ReadItsSourceKeys"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [for name in statement.value : aws_secretsmanager_secret.source[name].arn]
    }
  }
  # Its catalog client (a Secrets Manager ARN ends in six characters of its own, hence the one wildcard); the slot's
  # own policy refuses every other reader (infra/aws-lakekeeper/identity.tf).
  dynamic "statement" {
    for_each = contains(keys(local.lakekeeper_lanes), each.key) ? [each.key] : []
    content {
      sid       = "ReadItsCatalogClient"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = ["arn:aws:secretsmanager:${local.contract.region}:${local.contract.account.id}:secret:${local.lakekeeper_name}/principal-${statement.value}-??????"]
    }
  }
}

resource "aws_iam_role_policy" "lane" {
  for_each = local.collect.lanes
  name     = "lane"
  role     = aws_iam_role.lane[each.key].id
  policy   = data.aws_iam_policy_document.lane[each.key].json
}

output "credential_vendor" {
  value = aws_lambda_function.r2_vend.function_name
}

output "credential_vendor_secrets" {
  value = [aws_secretsmanager_secret.r2_mint_token.name]
}

output "source_secrets" {
  value = { for name, slot in aws_secretsmanager_secret.source : name => slot.name }
}

output "lane_roles" {
  value = { for id, role in aws_iam_role.lane : id => role.arn }
}
