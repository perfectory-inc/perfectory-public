# Machine identity (coordinator 2026-11-27): one Cognito app client per principal, OAuth2 client credentials.
# Lakekeeper accepts OIDC JWTs only and has no AWS IAM login (upstream authentication, v0.15.1), so a job proves
# who it is with its client's secret, which only its own task's execution role can inject (probes.tf). The
# principals are the contract's (identity.principals); the catalog admits the declared ones only (catalog.tf).
#
# The client secret is Cognito's: OpenTofu's aws_cognito_user_pool_client returns it as a computed attribute, so,
# unlike every other secret of the platform, it is in this stack's state (provider 6.72.0 has no write-only form).
# The state bucket is private, versioned and encrypted, and only the applying SSO role reads it. Prod must decide
# this (Cognito's AddUserPoolClientSecret lets a secret be set and rotated outside the state).

resource "aws_cognito_user_pool" "machines" {
  name                = local.name
  user_pool_tier      = "LITE"
  deletion_protection = "INACTIVE"
  # No people: nobody signs up, and no user is ever made.
  admin_create_user_config {
    allow_admin_create_user_only = true
  }
}

# The token endpoint's host (<prefix>.auth.<region>.amazoncognito.com/oauth2/token).
resource "aws_cognito_user_pool_domain" "machines" {
  domain       = local.dev.identity.domain_prefix
  user_pool_id = aws_cognito_user_pool.machines.id
}

resource "aws_cognito_resource_server" "catalog" {
  identifier   = local.dev.identity.resource_server
  name         = local.dev.identity.resource_server
  user_pool_id = aws_cognito_user_pool.machines.id
  scope {
    scope_name        = local.dev.identity.scope
    scope_description = "Call the Lakekeeper dev catalog"
  }
  # A second scope that is not the catalog's: a token carrying only it must be refused (T6).
  scope {
    scope_name        = local.dev.identity.other_scope
    scope_description = "Not the catalog (negative test)"
  }
}

resource "aws_cognito_user_pool_client" "principal" {
  for_each                             = local.dev.identity.principals
  name                                 = "${local.name}-${each.key}"
  user_pool_id                         = aws_cognito_user_pool.machines.id
  generate_secret                      = true
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = [for scope in each.value.scopes : "${aws_cognito_resource_server.catalog.identifier}/${scope}"]
  supported_identity_providers         = ["COGNITO"]
  explicit_auth_flows                  = ["ALLOW_REFRESH_TOKEN_AUTH"]
  access_token_validity                = each.value.access_token_minutes
  id_token_validity                    = each.value.access_token_minutes
  token_validity_units {
    access_token = "minutes"
    id_token     = "minutes"
  }
  enable_token_revocation       = true
  prevent_user_existence_errors = "ENABLED"
}

# Each principal's id and secret, in the shape a job reads them; only that principal's probe injects it.
resource "aws_secretsmanager_secret" "principal" {
  for_each                = local.dev.identity.principals
  name                    = "${local.name}/principal-${each.key}"
  description             = "OAuth2 client credentials of the Lakekeeper dev principal ${each.key}."
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "principal" {
  for_each  = local.dev.identity.principals
  secret_id = aws_secretsmanager_secret.principal[each.key].id
  secret_string_wo = jsonencode({
    client_id     = aws_cognito_user_pool_client.principal[each.key].id
    client_secret = aws_cognito_user_pool_client.principal[each.key].client_secret
  })
  secret_string_wo_version = 1
}

locals {
  token_url = "https://${aws_cognito_user_pool_domain.machines.domain}.auth.${local.region}.amazoncognito.com/oauth2/token"
}

output "issuer" {
  value = local.issuer
}

output "token_url" {
  value = local.token_url
}

# Client ids name the principals in Lakekeeper (oidc~<client id>); they are identifiers, not secrets.
output "principals" {
  value = { for name, client in aws_cognito_user_pool_client.principal : name => client.id }
}
