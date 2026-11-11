# The production shape ledger's bucket as code (root ADR-0476): the R2 bucket the host's shape ledger
# check publishes production's table shapes to, the lock rules that make its objects and heads'
# history append-only, and the custom domain CI reads it through anonymously. scripts/deploy/
# edge_settings.py applies it on the data host after the console's edge, with the same account
# token; nobody clicks through the dashboard. The bucket name and the public origin are read from the
# R2 connection contract (generated from definitions/platform.definition.json) and passed in.

terraform {
  required_version = "1.13.9"
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.28.1"
    }
  }
  # The console edge's state bucket under its own key; edge_settings.py passes bucket, key, endpoint.
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
  # Nothing here is secret; the state is encrypted like its neighbours' so one passphrase rule holds.
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

variable "state_passphrase" {
  type      = string
  sensitive = true
}

variable "bucket" {
  description = "The ledger bucket: connections.shape_ledger FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET."
  type        = string
}

variable "public_base_url" {
  description = "Where CI reads it: connections.shape_ledger FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_PUBLIC_BASE_URL."
  type        = string
  validation {
    condition     = can(regex("^https://[a-z0-9.-]+$", var.public_base_url))
    error_message = "The public base URL is https://<hostname> with no path."
  }
}

locals {
  hostname  = trimprefix(var.public_base_url, "https://")
  labels    = split(".", local.hostname)
  zone_name = join(".", slice(local.labels, length(local.labels) - 2, length(local.labels)))
}

provider "cloudflare" {}

data "cloudflare_zone" "ledger" {
  filter = {
    name = local.zone_name
  }
}

resource "cloudflare_r2_bucket" "ledger" {
  account_id = var.account_id
  name       = var.bucket
  # The host that writes it is in Korea; best effort, honoured only at creation.
  location = "apac"
}

# Append-only (root ADR-0476 §2): a shape object and a head's history entry can be neither replaced
# nor deleted, by anyone, ever. The heads shape-ledger/latest.json and parcel-editions/latest.json are
# the keys outside the rules.
resource "cloudflare_r2_bucket_lock" "ledger" {
  account_id  = var.account_id
  bucket_name = cloudflare_r2_bucket.ledger.name
  rules = [
    {
      id        = "shape objects are kept forever"
      prefix    = "shape-ledger/objects/"
      enabled   = true
      condition = { type = "Indefinite" }
    },
    {
      id        = "head history is kept forever"
      prefix    = "shape-ledger/history/"
      enabled   = true
      condition = { type = "Indefinite" }
    },
    # Root ADR-0490 §5: the copies of the host promotions ledger CI reads, and their heads' history.
    {
      id        = "promotions ledgers are kept forever"
      prefix    = "parcel-editions/objects/"
      enabled   = true
      condition = { type = "Indefinite" }
    },
    {
      id        = "promotions head history is kept forever"
      prefix    = "parcel-editions/history/"
      enabled   = true
      condition = { type = "Indefinite" }
    },
  ]
}

# Public read: the ledgers carry table and column names this public repository has always carried.
resource "cloudflare_r2_custom_domain" "ledger" {
  account_id  = var.account_id
  bucket_name = cloudflare_r2_bucket.ledger.name
  domain      = local.hostname
  zone_id     = data.cloudflare_zone.ledger.zone_id
  enabled     = true
  min_tls     = "1.2"
}
