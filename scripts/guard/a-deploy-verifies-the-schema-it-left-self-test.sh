#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/a-deploy-verifies-the-schema-it-left.sh"
real_release="$(pwd -P)/platforms/foundation-platform/scripts/deploy/foundation-release.sh"
real_assert="$(pwd -P)/platforms/foundation-platform/scripts/deploy/assert-runtime-migrations.sh"
name="a-deploy-verifies-the-schema-it-left-self-test"
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
  local dir="$root/platforms/foundation-platform/scripts/deploy"
  mkdir -p "$dir"
  cp "$real_release" "$dir/foundation-release.sh"
  cp "$real_assert" "$dir/assert-runtime-migrations.sh"
  case "$edit" in
    intact) ;;
    drop-check)
      rm "$dir/assert-runtime-migrations.sh"
      ;;
    install-skips-verify)
      # The subcommand still exists; the install path just stops calling it. This is the shape
      # a hurried edit takes, and the one a file-wide grep would miss.
      python3 - "$dir/foundation-release.sh" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
head, sep, tail = text.partition("  install)")
branch, sep2, rest = tail.partition(";;")
io.open(path, "w", encoding="utf-8", newline="").write(
    head + sep + branch.replace("    verify_runtime_schema\n", "") + sep2 + rest
)
PY
      ;;
    check-passes-when-blind)
      # Turn the unreachable-database refusal into a pass.
      python3 - "$dir/assert-runtime-migrations.sh" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
text = text.replace(
    'fail "runtime postgres container is not running; the schema cannot be read"',
    'exit 0',
)
io.open(path, "w", encoding="utf-8", newline="").write(text)
PY
      ;;
    no-way-to-apply)
      python3 - "$dir/foundation-release.sh" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("  migrate)\n", "  migrate-disabled)\n")
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
  if [[ -n "$needle" ]] && ! grep -Fq "$needle" <<<"$output"; then
    echo "FAIL ${name}: ${label} did not say why (${needle})" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  printf '  %s %s\n' "$want" "$label"
}

# Accepting the real tree matters as much as rejecting the broken ones: a guard that rejects
# correct code is a guard people switch off.
expect real-tree intact accept
expect missing-check drop-check reject "runtime schema check is missing"
expect install-skips-verify install-skips-verify reject "install does not verify"
expect check-passes-when-blind check-passes-when-blind reject "unanswered question"
expect no-way-to-apply no-way-to-apply reject "no way to apply"

printf 'OK %s\n' "$name"
