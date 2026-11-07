# The console's edge settings as code (root ADR-0436): the Zero Trust organization, the Access
# application and its one allow policy, the remotely managed tunnel with its one route, and the DNS
# record that points at it. scripts/deploy/edge_settings.py applies this on the data host after the
# host runs main; nobody clicks through the dashboard. The values are read from the files that
# already decide them, never copied here: the public origin from the console's Zitadel app
# (identity-platform staff-console-applications.v1.json), the origin the tunnel reaches from the
# console's compose service (compose.console.yml DAWNEER_BIND).

terraform {
  required_version = "1.13.9"
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.28.1"
    }
  }
  # R2 through its S3 API; bucket, key and endpoint come from edge_settings.py (-backend-config).
  # use_lockfile is OpenTofu's S3-native lock: an If-None-Match conditional PUT, which R2 supports.
  backend "s3" {
    region                      = "auto"
    use_lockfile                = true
    use_path_style              = true
    skip_credentials_validation = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_metadata_api_check     = true
    skip_s3_checksum            = true
  }
  # The state holds the tunnel's secret: it is encrypted before it leaves the host.
  encryption {
    key_provider "pbkdf2" "state" {
      passphrase = var.state_passphrase
    }
    method "aes_gcm" "state" {
      keys = key_provider.pbkdf2.state
    }
    state {
      method   = method.aes_gcm.state
      enforced = true
    }
    plan {
      method   = method.aes_gcm.state
      enforced = true
    }
  }
}

variable "account_id" {
  type = string
}

variable "staff_emails" {
  description = "The active human staff of the identity platform (Zitadel grants on the project), read at apply time; never written to the repository."
  type        = list(string)
  sensitive   = true
  validation {
    condition     = length(var.staff_emails) > 0
    error_message = "An empty allowlist would lock every person out; edge_settings.py refuses before this."
  }
}

variable "state_passphrase" {
  type      = string
  sensitive = true
}

locals {
  console_app = one([for app in jsondecode(file("${path.module}/../../../identity-platform/config/staff-console-applications.v1.json")).applications : app if app.name == "dawneer"])
  hostname    = regex("^https://([^/:]+)/", local.console_app.redirect_uris[0])[0]
  zone_name   = join(".", slice(split(".", local.hostname), length(split(".", local.hostname)) - 2, length(split(".", local.hostname))))
  origin      = "http://${yamldecode(file("${path.module}/../../compose.console.yml")).services.dawneer.environment.DAWNEER_BIND}"
}

provider "cloudflare" {}

data "cloudflare_zone" "console" {
  filter = {
    name = local.zone_name
  }
}

# Created once through the API on 2026-11-07 (root ADR-0436 Context); adopted here.
import {
  to = cloudflare_zero_trust_organization.this
  id = var.account_id
}

resource "cloudflare_zero_trust_organization" "this" {
  account_id = var.account_id
  name       = "perfectory"
  lifecycle {
    # The team domain Cloudflare generated is kept; renaming it would move every login URL.
    ignore_changes = [auth_domain]
  }
}

resource "cloudflare_zero_trust_access_identity_provider" "one_time_pin" {
  account_id = var.account_id
  name       = "One-time PIN"
  type       = "onetimepin"
  config     = {}
}

resource "cloudflare_zero_trust_access_policy" "staff" {
  account_id = var.account_id
  name       = "staff"
  decision   = "allow"
  include    = [for email in sort(distinct(var.staff_emails)) : { email = { email = email } }]
  depends_on = [cloudflare_zero_trust_organization.this]
}

resource "cloudflare_zero_trust_access_application" "console" {
  account_id                = var.account_id
  name                      = "dawneer"
  type                      = "self_hosted"
  domain                    = local.hostname
  destinations              = [{ type = "public", uri = local.hostname }]
  session_duration          = "24h"
  allowed_idps              = [cloudflare_zero_trust_access_identity_provider.one_time_pin.id]
  auto_redirect_to_identity = true
  app_launcher_visible      = false
  policies                  = [{ id = cloudflare_zero_trust_access_policy.staff.id, precedence = 1 }]
}

resource "cloudflare_zero_trust_tunnel_cloudflared" "console" {
  account_id = var.account_id
  name       = "perfectory-console"
  config_src = "cloudflare"
}

resource "cloudflare_zero_trust_tunnel_cloudflared_config" "console" {
  account_id = var.account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.console.id
  config = {
    ingress = [
      { hostname = local.hostname, service = local.origin },
      { service = "http_status:404" },
    ]
  }
}

# Only once Access guards the hostname does the name resolve to the tunnel.
resource "cloudflare_dns_record" "console" {
  zone_id    = data.cloudflare_zone.console.zone_id
  name       = local.hostname
  type       = "CNAME"
  content    = "${cloudflare_zero_trust_tunnel_cloudflared.console.id}.cfargotunnel.com"
  proxied    = true
  ttl        = 1
  comment    = "Dawneer console tunnel (root ADR-0436)"
  depends_on = [cloudflare_zero_trust_access_application.console]
}

data "cloudflare_zero_trust_tunnel_cloudflared_token" "console" {
  account_id = var.account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.console.id
}

# edge_settings.py writes it into the runtime-secrets group console-tunnel; nobody copies it.
output "tunnel_token" {
  value     = data.cloudflare_zero_trust_tunnel_cloudflared_token.console.token
  sensitive = true
}
