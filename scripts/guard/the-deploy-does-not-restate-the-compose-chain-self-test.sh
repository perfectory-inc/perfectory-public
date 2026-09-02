#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/the-deploy-does-not-restate-the-compose-chain.sh"
real_release="$(pwd -P)/platforms/foundation-platform/scripts/deploy/foundation-release.sh"
real_compose="$(pwd -P)/platforms/foundation-platform/docker-compose.yml"
name="the-deploy-does-not-restate-the-compose-chain-self-test"
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
  local dir="$root/platforms/foundation-platform"
  mkdir -p "$dir/scripts/deploy"
  cp "$real_compose" "$dir/docker-compose.yml"
  cp "$real_release" "$dir/scripts/deploy/foundation-release.sh"
  local target="$dir/scripts/deploy/foundation-release.sh"
  case "$edit" in
    intact) ;;
    the-2026-09-02-state)
      # Exactly what shipped: the chain walked by hand and stopped two links early.
      python3 - "$target" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace(
        '  "${runtime}" up -d --wait foundation-api\n',
        '  "${runtime}" up -d --no-deps postgres\n'
        '  "${runtime}" run --rm foundation-migrate\n',
    )
)
PY
      ;;
    names-a-later-link)
      # A subtler version: the whole chain is walked, so the outcome is right today, but the
      # order now lives in two files and only one of them is compose.
      python3 - "$target" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace(
        '  "${runtime}" up -d --wait foundation-api\n',
        '  "${runtime}" run --rm foundation-runtime-grants\n'
        '  "${runtime}" up -d --wait foundation-api\n',
    )
)
PY
      ;;
    forgets-to-wait)
      python3 - "$target" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace('up -d --wait foundation-api', 'up -d foundation-api')
)
PY
      ;;
    *) echo "unknown fixture edit: $edit" >&2; exit 2 ;;
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
  if [[ -n "$needle" ]] && ! grep -Fq -e "$needle" <<<"$output"; then
    echo "FAIL ${name}: ${label} did not say '${needle}'" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  printf '  %s %s\n' "$want" "$label"
}

expect real-tree            intact               accept
expect the-2026-09-02-state the-2026-09-02-state reject "--no-deps"
expect names-a-later-link   names-a-later-link   reject "foundation-runtime-grants"
expect forgets-to-wait      forgets-to-wait      reject "never waits"

printf 'OK %s\n' "$name"
