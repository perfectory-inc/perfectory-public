#!/usr/bin/env bash
# Every credential-shaped FOUNDATION_PLATFORM_ name in a tracked file is derived from a Cloudflare
# token recorded once in platforms/foundation-platform/config/runtime-secrets.contract.json, or named
# there in not_cloudflare with the reason (root ADR-0247).
#
# What real incident does failing this prevent? On 2026-10-14 the operations map needed a read-only
# catalog token and there was no name to give it: the write token was called
# ..._LAKEHOUSE_CATALOG_TOKEN, and seven R2 key pairs carried an R2_ their tokens do not. Names were
# made up per use and kept in three hand lists. This refuses a name the token rule does not make,
# including every retired one. The checker is scripts/deploy/runtime_secrets.py; this only calls it.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL credential-names: python3 is required" >&2
  exit 1
}
area="${root}/platforms/foundation-platform"
python3 "${area}/scripts/deploy/runtime_secrets.py" --area "${area}" credentials --repo "${root}"
