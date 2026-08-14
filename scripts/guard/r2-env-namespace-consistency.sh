#!/usr/bin/env bash
# Prevents Foundation R2 connection-environment drift.
# Internal locals such as R2_MODE are allowed; only configuration declarations/lookups are checked.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
cd "$root"

fail=0
foundation_env="platforms/foundation-platform/.env.example"
contract="platforms/foundation-platform/config/r2-connections.contract.json"

canonical_keys=(
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY
)

for key in "${canonical_keys[@]}"; do
  if ! grep -Eq "^${key}=" "$foundation_env"; then
    printf 'FAIL r2-env-namespace: missing canonical key %s in %s\n' "$key" "$foundation_env" >&2
    fail=1
  fi
done

if [[ ! -f "$contract" ]]; then
  printf 'FAIL r2-env-namespace: missing R2 connection contract %s\n' "$contract" >&2
  fail=1
else
  for key in "${canonical_keys[@]}"; do
    if ! grep -Fq "\"$key\"" "$contract"; then
      printf 'FAIL r2-env-namespace: contract is missing canonical key %s\n' "$key" >&2
      fail=1
    fi
  done
fi

# When a developer-local profile exists, reject stale assignments without ever printing their values.
# CI has no `.env.local`, so this remains a local safety check rather than a secret-file dependency.
local_env="platforms/foundation-platform/.env.local"
legacy_local_keys=(
  R2_ACCOUNT_ID R2_BUCKET_NAME R2_ENDPOINT R2_REGION R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_PUBLIC_BASE_URL
  FOUNDATION_RECOVERY_R2_BUCKET FOUNDATION_RECOVERY_R2_ENDPOINT_HOST FOUNDATION_RECOVERY_R2_ACCESS_KEY_ID FOUNDATION_RECOVERY_R2_SECRET_ACCESS_KEY
  FOUNDATION_TILE_DERIVATIVE_R2_ACCOUNT_ID FOUNDATION_TILE_DERIVATIVE_R2_ENDPOINT FOUNDATION_TILE_DERIVATIVE_R2_BUCKET FOUNDATION_TILE_DERIVATIVE_R2_REGION FOUNDATION_TILE_DERIVATIVE_R2_PREFIX
  FOUNDATION_TILE_DERIVATIVE_R2_WRITE_ACCESS_KEY_ID FOUNDATION_TILE_DERIVATIVE_R2_WRITE_SECRET_ACCESS_KEY
  FOUNDATION_TILE_DERIVATIVE_R2_READ_ACCESS_KEY_ID FOUNDATION_TILE_DERIVATIVE_R2_READ_SECRET_ACCESS_KEY
  FOUNDATION_TILE_PROOF_R2_ACCOUNT_ID FOUNDATION_TILE_PROOF_R2_ACCESS_KEY_ID FOUNDATION_TILE_PROOF_R2_SECRET_ACCESS_KEY
  FOUNDATION_TILE_PROOF_R2_BUCKET FOUNDATION_TILE_PROOF_R2_ENDPOINT FOUNDATION_TILE_PROOF_R2_READ_BASE_URL FOUNDATION_TILE_PROOF_R2_READ_URL FOUNDATION_TILE_PROOF_R2_OBJECT_KEY
)
if [[ -f "$local_env" ]]; then
  for key in "${canonical_keys[@]}"; do
    local_count="$(grep -Ec "^[[:space:]]*${key}=" "$local_env" || true)"
    if [[ "$local_count" != 1 ]]; then
      printf 'FAIL r2-env-namespace: local profile must declare %s exactly once (count=%s)\n' "$key" "$local_count" >&2
      fail=1
    fi
  done
  for key in "${legacy_local_keys[@]}"; do
    if grep -Eq "^[[:space:]]*${key}=" "$local_env"; then
      printf 'FAIL r2-env-namespace: stale local key remains: %s\n' "$key" >&2
      fail=1
    fi
  done
fi

# These patterns deliberately require an assignment, lookup, or Compose/template interpolation.
# A bare local variable such as R2_MODE or a child-process mapping is not a Foundation config use.
legacy_patterns=(
  '(^|[^[:alnum:]_])(R2_ACCOUNT_ID|R2_BUCKET_NAME|R2_ENDPOINT|R2_REGION|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY|R2_PUBLIC_BASE_URL)[[:space:]]*='
  '\$\{(R2_ACCOUNT_ID|R2_BUCKET_NAME|R2_ENDPOINT|R2_REGION|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY|R2_PUBLIC_BASE_URL)\?'
  '\$\{ENV:(R2_ACCOUNT_ID|R2_BUCKET_NAME|R2_ENDPOINT|R2_REGION|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY|R2_PUBLIC_BASE_URL)\}'
  '(std::env::var|required_env|optional_env|value_from_file|value_from_dotenv_or_env)[^\n]*(R2_ACCOUNT_ID|R2_BUCKET_NAME|R2_ENDPOINT|R2_REGION|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY|R2_PUBLIC_BASE_URL)'
  '(^|[^[:alnum:]_])FOUNDATION_RECOVERY_R2_[A-Z0-9_]+[[:space:]]*='
  '\$\{FOUNDATION_RECOVERY_R2_[A-Z0-9_]+\?'
  '(^|[^[:alnum:]_])FOUNDATION_TILE_DERIVATIVE_R2_[A-Z0-9_]+[[:space:]]*='
  '\$\{FOUNDATION_TILE_DERIVATIVE_R2_[A-Z0-9_]+\?'
)

for pattern in "${legacy_patterns[@]}"; do
  matches="$(git grep -n -I -E "$pattern" -- \
    'platforms/foundation-platform' 'scripts' 'docs' \
    ':!scripts/guard/r2-env-namespace-consistency.sh' \
    ':!scripts/guard/r2-env-namespace-consistency-self-test.sh' \
    2>/dev/null || true)"
  if [ -n "$matches" ]; then
    printf 'FAIL r2-env-namespace: legacy Foundation R2 configuration use remains:\n%s\n' "$matches" >&2
    fail=1
  fi
done

# ---------------------------------------------------------------------------
# Unlisted names are rejected, not just known-legacy ones.
#
# Everything above is a deny-list: it rejects the specific legacy spellings we
# already know about. That is fail-OPEN. A brand-new connection-like name nobody
# thought to add — `FOUNDATION_PLATFORM_R2_ARCHIVE_SECRET_ACCESS_KEY`, say —
# passes every check on this page. The plan that introduced this namespace asked
# for the opposite shape: allow exactly the declared control families and reject
# unlisted connection-like names.
#
# Two rules, because the two risks are not the same.
#
#   1. A *connection field* (credential, endpoint, bucket, region) must be
#      canonical or an explicitly named exception. This is where a mistake leaks
#      access, so it is exact-match and fail-closed.
#   2. Any other name must belong to a declared *control family*. Operational
#      switches churn with the work, so families keep the guard from becoming a
#      second changelog — but an unfamiliar family still fails.
# ---------------------------------------------------------------------------

# A field name ending this way configures a connection, whatever family it sits in.
connection_field_suffixes=(
  ACCESS_KEY_ID SECRET_ACCESS_KEY ACCOUNT_ID
  ENDPOINT ENDPOINT_HOST BUCKET BUCKET_NAME REGION PUBLIC_BASE_URL
)

# Connection-shaped names that are deliberately NOT canonical connections.
# Each one is isolated from production credentials on purpose; listing them here
# is what makes that isolation reviewable instead of assumed.
connection_exceptions=(
  # Proof-only tile credentials. Loaded from TILES_SLICE_PROOF_ENV_FILE outside
  # the repository, never from the canonical Foundation profile.
  FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID
  FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET
  FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT
  # Live-smoke target selection. A bucket *name* chosen by the operator for a
  # bounded write probe, not a connection the runtime configures itself with.
  FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET
  # Billing report input: the bucket whose usage rows are being summed, read
  # from a CSV export rather than connected to.
  FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS_BUCKET_NAME
)

# Declared operational control families. A name outside all of them fails.
allowed_control_prefixes=(
  FOUNDATION_PLATFORM_R2_LIVE_
  FOUNDATION_PLATFORM_R2_SMOKE_
  FOUNDATION_PLATFORM_R2_INVENTORY_
  FOUNDATION_PLATFORM_R2_BILLING_
  FOUNDATION_PLATFORM_R2_CLEANUP_VERIFY
  FOUNDATION_PLATFORM_R2_DELETE_CANDIDATES
  FOUNDATION_PLATFORM_R2_BRONZE_KEY_
  FOUNDATION_PLATFORM_R2_TILE_PROOF_
)

in_list() {
  local needle="$1"
  shift
  local candidate
  for candidate in "$@"; do
    [ "$candidate" = "$needle" ] && return 0
  done
  return 1
}

has_connection_suffix() {
  local name="$1" suffix
  for suffix in "${connection_field_suffixes[@]}"; do
    case "$name" in
      *"_$suffix") return 0 ;;
    esac
  done
  return 1
}

has_allowed_prefix() {
  local name="$1" prefix
  for prefix in "${allowed_control_prefixes[@]}"; do
    case "$name" in
      "$prefix"*) return 0 ;;
    esac
  done
  return 1
}

# `|| true`: an empty result is a legitimate state and `set -euo pipefail` would
# otherwise abort with no verdict, which is worse than either answer.
discovered_names="$(
  git grep -oh -E 'FOUNDATION_PLATFORM_R2_[A-Z0-9_]+' -- \
    ':!scripts/guard/r2-env-namespace-consistency.sh' \
    ':!scripts/guard/r2-env-namespace-consistency-self-test.sh' \
    2>/dev/null | sort -u || true
)"

unlisted=""
while IFS= read -r name; do
  [ -n "$name" ] || continue
  # Trailing underscore means a prefix constant assembled in code
  # (`FOUNDATION_PLATFORM_R2_LAKEHOUSE_`), not an environment name.
  case "$name" in
    *_) continue ;;
  esac
  if in_list "$name" "${canonical_keys[@]}"; then
    continue
  fi
  if has_connection_suffix "$name"; then
    if ! in_list "$name" "${connection_exceptions[@]}"; then
      unlisted="${unlisted}${name}: connection field that is neither canonical nor a declared exception
"
    fi
    continue
  fi
  if ! has_allowed_prefix "$name"; then
    unlisted="${unlisted}${name}: belongs to no declared control family
"
  fi
done <<EOF
$discovered_names
EOF

if [ -n "$unlisted" ]; then
  printf 'FAIL r2-env-namespace: Foundation R2 names that no list authorises:\n' >&2
  printf '%s' "$unlisted" | sed 's/^/      /' >&2
  echo '    A connection field belongs in canonical_keys, or in connection_exceptions with' >&2
  echo '    the reason it is isolated. A control belongs to a declared family prefix.' >&2
  echo '    Rejecting only known-legacy spellings would let a new name through.' >&2
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo 'OK r2-env-namespace-consistency'
fi
exit "$fail"
