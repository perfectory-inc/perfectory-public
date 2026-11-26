# The credential vendor and the lanes' run roles (root ADR-0623 §2). A lane is data (config/aws-platform.contract.json,
# collect.lanes: its role's suffix and its scope); no block here names one.

locals {
  collect          = local.contract.collect
  project          = local.contract.tags.project
  vendor_name      = "${local.project}-r2-vend"
  run_role_path    = "/${local.project}/run/"
  lakehouse_bucket = jsondecode(file("${path.module}/../../config/r2-connections.contract.json")).connections.lakehouse.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  vendor_lanes     = { for id, lane in local.collect.lanes : id => { write_prefixes = lane.write_prefixes, read_tables = lane.read_tables } }
}

resource "aws_secretsmanager_secret" "r2_mint_token" {
  name        = "${local.project}/r2-mint-token"
  description = "<account id>:<token> of the Cloudflare API token the vendor mints R2 temporary credentials from."
}

resource "aws_secretsmanager_secret" "r2_catalog_reader_token" {
  name        = "${local.project}/r2-catalog-reader-token"
  description = "Cloudflare API token that reads the lakehouse bucket's R2 Data Catalog; only the vendor reads it."
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

# The vendor alone reads the two tokens.
data "aws_iam_policy_document" "r2_vend" {
  statement {
    sid       = "ReadTheTwoTokens"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.r2_mint_token.arn, aws_secretsmanager_secret.r2_catalog_reader_token.arn]
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

data "archive_file" "r2_vend" {
  type        = "zip"
  source_dir  = "${path.module}/r2-vend"
  excludes    = ["__pycache__"]
  output_path = "${path.module}/.build/r2-vend.zip"
}

resource "aws_lambda_function" "r2_vend" {
  function_name    = local.vendor_name
  description      = "Vends a lane's scoped R2 temporary credentials and pins its tables' snapshots (root ADR-0623 2)."
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
      CATALOG_SECRET         = aws_secretsmanager_secret.r2_catalog_reader_token.name
      BUCKET                 = local.lakehouse_bucket
      LANES                  = jsonencode(local.vendor_lanes)
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

# A lane's run role, under the run path the launcher passes (B1): it may invoke its own alias and read the
# foundation database's runtime secret, nothing else. Its R2 rights are the vendor's credentials'.
resource "aws_iam_role" "lane" {
  for_each           = local.collect.lanes
  name               = "${local.project}-${each.value.role}"
  path               = local.run_role_path
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
  value = [aws_secretsmanager_secret.r2_mint_token.name, aws_secretsmanager_secret.r2_catalog_reader_token.name]
}

output "lane_roles" {
  value = { for id, role in aws_iam_role.lane : id => role.arn }
}
