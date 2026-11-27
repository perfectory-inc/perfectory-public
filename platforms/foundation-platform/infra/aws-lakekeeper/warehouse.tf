# The warehouse's R2 storage credential: a Cloudflare account API token of this environment only, R2 "Admin Read &
# Write" (upstream storage-s3 "Cloudflare R2", v0.15.1: R2's temp-access-credentials endpoint accepts no other),
# never the existing CLOUDFLARE_API_TOKEN (owner decision 2026-11-27, option A): perfectory-lakekeeper-dev for dev,
# perfectory-lakekeeper-prod for prod. The owner puts its value once (docs/runbooks/lakekeeper.md); OpenTofu makes
# the slot only and never holds a value.
#
# Lakekeeper reads no AWS secret: the reconciler (reconciler.tf; in dev also the warehouse probe) hands the credential
# to Lakekeeper once, in POST /management/v1/warehouse, and Lakekeeper keeps it encrypted in its own database with the
# catalog's encryption key (catalog.tf). So those tasks' execution roles are the slot's only readers, and the slot's
# own policy refuses every other principal, the applier included. The warehouse is the contract's (warehouse). Until
# the owner puts the value, a task that injects it cannot start: no warehouse is made before then.

locals {
  r2_credential_readers = concat(
    [aws_iam_role.reconciler_execution.arn],
    [for name, role in aws_iam_role.probe_execution : role.arn if name == "warehouse"],
  )
}

resource "aws_secretsmanager_secret" "r2_warehouse_credential" {
  name                    = "${local.name}/r2-warehouse-credential"
  description             = "<account id>:<access key id>:<secret access key>:<token> of the ${local.env.title}'s own Cloudflare R2 Admin Read & Write token. Put by the owner; read only by the reconciler (and dev's warehouse probe)."
  recovery_window_in_days = local.env.secret_recovery_days
}

data "aws_iam_policy_document" "r2_warehouse_credential" {
  statement {
    sid       = "OnlyTheWarehouseTasksRead"
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
      values   = local.r2_credential_readers
    }
  }
}

resource "aws_secretsmanager_secret_policy" "r2_warehouse_credential" {
  secret_arn          = aws_secretsmanager_secret.r2_warehouse_credential.arn
  policy              = data.aws_iam_policy_document.r2_warehouse_credential.json
  block_public_policy = true
}

output "r2_warehouse_credential_secret" {
  value = aws_secretsmanager_secret.r2_warehouse_credential.name
}
