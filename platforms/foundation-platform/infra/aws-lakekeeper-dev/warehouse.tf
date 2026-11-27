# The dev warehouse's R2 storage credential (owner decision 2026-11-27, option A): a new Cloudflare account API
# token used only by this rig, R2 "Admin Read & Write" (upstream storage-s3 "Cloudflare R2", v0.15.1: R2's
# temp-access-credentials endpoint accepts no other), never the existing CLOUDFLARE_API_TOKEN. The owner puts its
# value once (docs/runbooks/lakekeeper-dev.md); OpenTofu makes the slot only and never holds a value.
#
# Lakekeeper reads no AWS secret: the warehouse task (probes.tf, the operator's probe that runs the
# warehouse-create step) hands the credential to Lakekeeper once, in POST /management/v1/warehouse, and Lakekeeper
# keeps it encrypted in its own database with the catalog's encryption key (catalog.tf). So that task's execution
# role is the slot's one reader, and the slot's own policy refuses every other principal, the applier included.
# The warehouse is the contract's (warehouse): the dev bucket, never a prod bucket.

resource "aws_secretsmanager_secret" "r2_warehouse_credential" {
  name                    = "${local.name}/r2-warehouse-credential"
  description             = "<account id>:<access key id>:<secret access key>:<token> of the Lakekeeper dev rig's own Cloudflare R2 Admin Read & Write token. Put by the owner; read only by the warehouse task."
  recovery_window_in_days = 0
}

data "aws_iam_policy_document" "r2_warehouse_credential" {
  statement {
    sid       = "OnlyTheWarehouseTaskReads"
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
      values   = [aws_iam_role.probe_execution["warehouse"].arn]
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
