# The Seoul front door as code (root ADR-0461): a CloudFront distribution in front of each Cloudflare
# Worker Korean visitors read, its ACM certificate (us-east-1, validated through the Cloudflare zone),
# the DNS-only CNAME of each door config/cdn-front-door.contract.json has switched to CloudFront, and
# the account's zero-spend budget. scripts/deploy/edge_settings.py applies it on the data host after
# the console's edge, once the runtime-secrets group aws-cdn exists; until then nothing here exists.
# The values are read from the files that decide them, never copied here: the doors, their origins,
# the caching and the budget from the front door contract, each gateway's public hostname from the
# R2 connection contract (generated from definitions/platform.definition.json).

terraform {
  required_version = "1.13.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.72.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.28.1"
    }
  }
  # The console edge's bucket under its own key; edge_settings.py passes bucket, key and endpoint.
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
  # Encrypted with the console edge's passphrase (runtime-secrets group edge-settings).
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

variable "aws_access_key_id" {
  description = "The apply key of the separate AWS account (runtime-secrets group aws-cdn); never gongzzang's account."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  type      = string
  sensitive = true
}

variable "workers_subdomain" {
  description = "The Cloudflare account's workers.dev subdomain, read from the API at apply time (the preview's origin)."
  type        = string
}

variable "alert_emails" {
  description = "The identity platform's staff with a role, the console edge's allowlist; the budget mails them."
  type        = list(string)
  sensitive   = true
  validation {
    condition     = length(var.alert_emails) > 0
    error_message = "A budget that mails nobody is no alarm; edge_settings.py refuses before this."
  }
}

variable "state_passphrase" {
  type      = string
  sensitive = true
}

locals {
  contract = jsondecode(file("${path.module}/../../config/cdn-front-door.contract.json"))
  r2       = jsondecode(file("${path.module}/../../config/r2-connections.contract.json"))
  doors = {
    for name, door in local.contract.doors : name => {
      public_hostname = try(door.public_hostname, local.r2[door.gateway].public_hostname)
      origin_hostname = try(door.origin_hostname, "${door.workers_dev_worker}.${var.workers_subdomain}.workers.dev")
      answered_by     = door.answered_by
      caching         = door.caching
    }
  }
  # Only a door switched to CloudFront has its public name pointed at the distribution.
  switched     = { for name, door in local.doors : name => door if door.answered_by == "cloudfront" }
  every_method = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
}

provider "aws" {
  # CloudFront's certificates live in us-east-1; CloudFront and Budgets are global.
  region     = "us-east-1"
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
  default_tags {
    tags = {
      managed-by = "opentofu"
      decision   = "root-adr-0461"
    }
  }
}

provider "cloudflare" {}

data "cloudflare_zone" "front" {
  filter = {
    name = local.contract.zone
  }
}

resource "aws_acm_certificate" "door" {
  for_each          = local.doors
  domain_name       = each.value.public_hostname
  validation_method = "DNS"
  lifecycle {
    create_before_destroy = true
  }
}

# One validation name per certificate (one hostname each), DNS-only: ACM must see the record itself.
resource "cloudflare_dns_record" "validation" {
  for_each = local.doors
  zone_id  = data.cloudflare_zone.front.zone_id
  name     = trimsuffix(one(aws_acm_certificate.door[each.key].domain_validation_options).resource_record_name, ".")
  type     = one(aws_acm_certificate.door[each.key].domain_validation_options).resource_record_type
  content  = trimsuffix(one(aws_acm_certificate.door[each.key].domain_validation_options).resource_record_value, ".")
  proxied  = false
  ttl      = 60
  comment  = "ACM validation of the ${each.key} front door (root ADR-0461)"
}

resource "aws_acm_certificate_validation" "door" {
  for_each                = local.doors
  certificate_arn         = aws_acm_certificate.door[each.key].arn
  validation_record_fqdns = [cloudflare_dns_record.validation[each.key].name]
}

resource "aws_cloudfront_cache_policy" "origin" {
  name        = "perfectory-origin-cache-control"
  comment     = "The origin's Cache-Control decides (root ADR-0461)"
  min_ttl     = local.contract.cache.min_ttl_seconds
  default_ttl = local.contract.cache.default_ttl_seconds
  max_ttl     = local.contract.cache.max_ttl_seconds
  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "whitelist"
      headers {
        items = local.contract.cache.key_headers
      }
    }
    query_strings_config {
      query_string_behavior = "none"
    }
  }
}

data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_viewer_except_host" {
  name = "Managed-AllViewerExceptHostHeader"
}

# Server-Timing names the PoP and whether the edge held the answer: what the verification reads.
resource "aws_cloudfront_response_headers_policy" "timing" {
  name    = "perfectory-server-timing"
  comment = "Server-Timing on every answer (root ADR-0461)"
  server_timing_headers_config {
    enabled       = true
    sampling_rate = 100
  }
}

resource "aws_cloudfront_distribution" "door" {
  for_each        = local.doors
  enabled         = true
  comment         = "${each.key} front door (root ADR-0461)"
  aliases         = [each.value.public_hostname]
  http_version    = "http2and3"
  is_ipv6_enabled = true
  price_class     = local.contract.price_class

  origin {
    origin_id   = "cloudflare"
    domain_name = each.value.origin_hostname
    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = local.contract.origin.protocol
      origin_ssl_protocols     = [local.contract.origin.tls]
      origin_keepalive_timeout = local.contract.origin.keepalive_timeout_seconds
      origin_read_timeout      = local.contract.origin.read_timeout_seconds
    }
  }

  default_cache_behavior {
    target_origin_id           = "cloudflare"
    viewer_protocol_policy     = "redirect-to-https"
    compress                   = true
    allowed_methods            = each.value.caching == "origin" ? ["GET", "HEAD", "OPTIONS"] : local.every_method
    cached_methods             = each.value.caching == "origin" ? ["GET", "HEAD", "OPTIONS"] : ["GET", "HEAD"]
    cache_policy_id            = each.value.caching == "origin" ? aws_cloudfront_cache_policy.origin.id : data.aws_cloudfront_cache_policy.disabled.id
    origin_request_policy_id   = each.value.caching == "origin" ? null : data.aws_cloudfront_origin_request_policy.all_viewer_except_host.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.timing.id
  }

  # A pass-through door still caches the build's content-hashed assets.
  dynamic "ordered_cache_behavior" {
    for_each = each.value.caching == "pass-through" ? ["/_next/static/*"] : []
    content {
      path_pattern               = ordered_cache_behavior.value
      target_origin_id           = "cloudflare"
      viewer_protocol_policy     = "redirect-to-https"
      compress                   = true
      allowed_methods            = ["GET", "HEAD"]
      cached_methods             = ["GET", "HEAD"]
      cache_policy_id            = aws_cloudfront_cache_policy.origin.id
      response_headers_policy_id = aws_cloudfront_response_headers_policy.timing.id
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.door[each.key].certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }

  lifecycle {
    precondition {
      condition     = contains(["cloudflare", "cloudfront"], each.value.answered_by) && contains(["origin", "pass-through"], each.value.caching)
      error_message = "answered_by is cloudflare or cloudfront, caching is origin or pass-through (config/cdn-front-door.contract.json)."
    }
    precondition {
      condition     = each.value.origin_hostname != each.value.public_hostname
      error_message = "A door's origin is its public hostname: once switched, CloudFront would ask itself."
    }
  }
}

# The switch (contract answered_by = cloudfront). Rollback: set it back to cloudflare; the Worker deploy
# takes the hostname back as a custom domain first and this record leaves the state.
resource "cloudflare_dns_record" "door" {
  for_each = local.switched
  zone_id  = data.cloudflare_zone.front.zone_id
  name     = each.value.public_hostname
  type     = "CNAME"
  content  = aws_cloudfront_distribution.door[each.key].domain_name
  proxied  = false
  ttl      = 60
  comment  = "${each.key} front door: CloudFront (root ADR-0461)"
}

resource "aws_budgets_budget" "free_tier" {
  name         = local.contract.budget.name
  budget_type  = "COST"
  limit_amount = local.contract.budget.limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    notification_type          = "ACTUAL"
    threshold                  = tonumber(local.contract.budget.actual_over_usd)
    threshold_type             = "ABSOLUTE_VALUE"
    subscriber_email_addresses = slice(sort(distinct(var.alert_emails)), 0, min(10, length(distinct(var.alert_emails))))
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    notification_type          = "FORECASTED"
    threshold                  = local.contract.budget.forecast_over_percent
    threshold_type             = "PERCENTAGE"
    subscriber_email_addresses = slice(sort(distinct(var.alert_emails)), 0, min(10, length(distinct(var.alert_emails))))
  }
}

# Names only, no secret: edge_settings.py logs them, and the verification connects to them by name.
output "distributions" {
  value = { for name, distribution in aws_cloudfront_distribution.door : name => distribution.domain_name }
}
