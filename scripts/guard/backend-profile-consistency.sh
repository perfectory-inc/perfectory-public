#!/usr/bin/env bash
# Prevents environment/profile drift between developer templates, CI, and the runtime policy.
# The pre-launch shared profile is intentionally explicit; after launch the same files may move
# together to the local/development profile, but mixed tuples are never valid.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
cd "$root"

fail=0
foundation_env_dir="platforms/foundation-platform"
template_files=(
  "$foundation_env_dir/.env.example"
  "$foundation_env_dir/.env.local.example"
)

value_from_file() {
  local file="$1"
  local key="$2"
  [ -f "$file" ] || return 1
  sed -n "s/^${key}=//p" "$file" | head -n 1 | tr -d '\r'
}

check_profile_file() {
  local file="$1"
  local runtime context acknowledgement bucket catalog_driver bronze_driver
  runtime="$(value_from_file "$file" FOUNDATION_PLATFORM_RUNTIME_ENV || true)"
  context="$(value_from_file "$file" FOUNDATION_PLATFORM_EXECUTION_CONTEXT || true)"
  acknowledgement="$(value_from_file "$file" FOUNDATION_PLATFORM_PRELAUNCH_SHARED || true)"
  bucket="$(value_from_file "$file" FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET || true)"
  catalog_driver="$(value_from_file "$file" FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER || true)"
  bronze_driver="$(value_from_file "$file" FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER || true)"

  case "${runtime}|${context}|${acknowledgement}|${bucket}|${catalog_driver}|${bronze_driver}" in
    'production|developer|1|foundation-platform-lakehouse-prod|r2|r2'|'local|developer|0|foundation-platform-lakehouse-dev|r2|r2')
      ;;
    *)
      echo "FAIL backend-profile: invalid Foundation profile in $file" >&2
      echo "  runtime=$runtime context=$context prelaunch=$acknowledgement bucket=$bucket catalog_driver=$catalog_driver bronze_driver=$bronze_driver" >&2
      fail=1
      ;;
  esac
}

for file in "${template_files[@]}"; do
  if [ -f "$file" ]; then
    check_profile_file "$file"
  else
    echo "FAIL backend-profile: missing required template $file" >&2
    fail=1
  fi
done

# A developer's ignored .env.local is checked when present, without ever printing secret values.
if [ -f "$foundation_env_dir/.env.local" ]; then
  check_profile_file "$foundation_env_dir/.env.local"
fi

workflow='.github/workflows/foundation-ci.yml'
if [ -f "$workflow" ]; then
  ci_runtime_count=$(grep -Ec '^[[:space:]]*FOUNDATION_PLATFORM_RUNTIME_ENV:[[:space:]]+ci[[:space:]]*$' "$workflow" || true)
  ci_context_count=$(grep -Ec '^[[:space:]]*FOUNDATION_PLATFORM_EXECUTION_CONTEXT:[[:space:]]+ci[[:space:]]*$' "$workflow" || true)
  if [ "$ci_runtime_count" -lt 2 ] || [ "$ci_context_count" -lt 2 ]; then
    echo "FAIL backend-profile: foundation CI must set runtime=ci and execution_context=ci for integration and compose smoke" >&2
    fail=1
  fi
fi

for doc in \
  docs/technology-stack.md \
  platforms/foundation-platform/docs/adr/0029-runtime-environment-backend-separation.md \
  platforms/foundation-platform/docs/runbooks/runtime-environment-separation.md; do
  if ! grep -Fq 'FOUNDATION_PLATFORM_PRELAUNCH_SHARED' "$doc"; then
    echo "FAIL backend-profile: $doc must document the pre-launch acknowledgement" >&2
    fail=1
  fi
done

# Every operational Bronze write must cross the shared runtime/bucket preflight at the storage
# adapter boundary. A direct call to the read/configuration builder is an escape hatch: a future
# ingest command could stream provider bytes while targeting an unvalidated bucket. Keep the
# low-level builders private to their module's policy implementation and require callers to use
# the `live_write_...` wrappers instead.
bronze_source_dir="platforms/foundation-platform/services/foundation-outbox-publisher/src"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  direct_bronze_builders=$(git grep -n -E '(^|[^[:alnum:]_])bronze_(streaming_)?object_storage_from_env[[:space:]]*\(' -- \
    "$bronze_source_dir" \
    ':(exclude)platforms/foundation-platform/services/foundation-outbox-publisher/src/bronze_object_storage.rs' || true)
else
  direct_bronze_builders=$(grep -R -n -E '(^|[^[:alnum:]_])bronze_(streaming_)?object_storage_from_env[[:space:]]*\(' \
    --include='*.rs' "$bronze_source_dir" 2>/dev/null \
    | grep -v '/bronze_object_storage\.rs:' || true)
fi
if [ -n "$direct_bronze_builders" ]; then
  echo 'FAIL backend-profile: live Bronze callers must use the preflighted storage adapter' >&2
  printf '%s\n' "$direct_bronze_builders" >&2
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo 'OK backend-profile-consistency'
fi
exit "$fail"
