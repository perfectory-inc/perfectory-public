#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/bootstrap-creates-what-it-grants-on.sh"
real="$(pwd -P)/platforms/foundation-platform/infra/compose/bootstrap-foundation.sql"
name="bootstrap-creates-what-it-grants-on-self-test"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL ${name}: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

fixture() {
  local label="$1" edit="$2"
  local root="$test_root/$label"
  local dir="$root/platforms/foundation-platform/infra/compose"
  mkdir -p "$dir"
  cp "$real" "$dir/bootstrap-foundation.sql"
  case "$edit" in
    intact) ;;
    drop-create)
      # The exact 2026-09-01 state: the ALTER and GRANT stay, the CREATE goes.
      python3 - "$dir/bootstrap-foundation.sql" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("CREATE SCHEMA IF NOT EXISTS catalog AUTHORIZATION foundation_migrator;\n", "")
)
PY
      ;;
    grant-on-a-new-schema)
      printf 'GRANT USAGE ON SCHEMA reporting TO foundation_migrator;\n' \
        >>"$dir/bootstrap-foundation.sql"
      ;;
  esac
  printf '%s' "$root"
}

expect() {
  local label="$1" edit="$2" want="$3" needle="${4:-}"
  local root output rc
  root="$(fixture "$label" "$edit")"
  output="$(bash "$checker" "$root" 2>&1)" && rc=0 || rc=$?
  if [[ "$want" == "reject" && "$rc" -eq 0 ]]; then
    echo "FAIL ${name}: ${label} was accepted and should not be" >&2
    exit 1
  fi
  if [[ "$want" == "accept" && "$rc" -ne 0 ]]; then
    echo "FAIL ${name}: ${label} was rejected and should not be" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  if [[ -n "$needle" ]] && ! grep -Fq "$needle" <<<"$output"; then
    echo "FAIL ${name}: ${label} did not say '${needle}'" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  printf '  %s %s\n' "$want" "$label"
}

expect real-tree intact accept
expect the-2026-09-01-state drop-create reject "never creates it"
expect a-new-schema-granted-only grant-on-a-new-schema reject "reporting"

printf 'OK %s\n' "$name"
