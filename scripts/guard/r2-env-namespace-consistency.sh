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

if [ "$fail" -eq 0 ]; then
  echo 'OK r2-env-namespace-consistency'
fi
exit "$fail"
