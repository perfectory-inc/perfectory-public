#!/usr/bin/env bash
# Proves the runtime migration check answers correctly, without a database.
#
# The check runs on a deployment host and CI cannot reach one, so the comparison would otherwise
# never be exercised by anything but the incident it exists to catch. The release directory, the
# compose wrapper and `docker` are all substituted here; the check's own logic is not.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/platforms/foundation-platform/scripts/deploy/assert-runtime-migrations.sh"
name="assert-runtime-migrations-self-test"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL ${name}: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# A `docker` that reports whatever the fixture wrote, so the check reads a ledger it did not
# also produce.
mkdir -p "$test_root/bin"
cat >"$test_root/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
# Only `docker exec <container> sh -lc <psql...>` is used by the check.
if [[ "${1:-}" == "exec" ]]; then
  cat "${FAKE_APPLIED_FILE}"
  exit 0
fi
exit 1
DOCKER
chmod +x "$test_root/bin/docker"

fixture() {
  local label="$1" shipped="$2" applied="$3" wrapper="$4"
  local root="$test_root/$label"
  mkdir -p "$root/migrations" "$root/scripts/deploy"
  local version
  for version in $shipped; do
    printf -- '-- fixture\n' >"$root/migrations/${version}_fixture.sql"
  done
  printf '%s\n' $applied >"$root/applied.txt"
  case "$wrapper" in
    works)
      cat >"$root/scripts/deploy/foundation-runtime.sh" <<'WRAP'
#!/usr/bin/env bash
printf 'fake-container-id\n'
WRAP
      ;;
    refuses)
      cat >"$root/scripts/deploy/foundation-runtime.sh" <<'WRAP'
#!/usr/bin/env bash
printf 'environment file is not readable: /etc/foundation-platform/recovery.env\n' >&2
exit 66
WRAP
      ;;
    no-container)
      cat >"$root/scripts/deploy/foundation-runtime.sh" <<'WRAP'
#!/usr/bin/env bash
exit 0
WRAP
      ;;
  esac
  chmod +x "$root/scripts/deploy/foundation-runtime.sh"
  printf '%s' "$root"
}

expect() {
  local label="$1" shipped="$2" applied="$3" wrapper="$4" want="$5" needle="${6:-}"
  local root output rc
  root="$(fixture "$label" "$shipped" "$applied" "$wrapper")"
  output="$(PATH="$test_root/bin:$PATH" FAKE_APPLIED_FILE="$root/applied.txt" \
    bash "$checker" "$root" 2>&1)" && rc=0 || rc=$?
  if [[ "$want" == "reject" && "$rc" -eq 0 ]]; then
    echo "FAIL ${name}: ${label} was accepted and should not be" >&2
    printf '%s\n' "$output" >&2
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

expect in-step            "1 2 3" "1 2 3" works accept "3 shipped, 3 applied"
# The shape of the 2026-09-01 incident: the runtime carries the oldest few and nothing else.
expect runtime-behind     "1 2 3" "1"     works reject "missing 2"
expect names-every-gap    "1 2 3" "1"     works reject "missing 3"
expect runtime-ahead      "1 2"   "1 2 3" works reject "unknown 3"
expect empty-ledger       "1 2"   ""      works reject "no applied migrations"
expect wrapper-refuses    "1 2"   "1 2"   refuses reject "could not ask the runtime"
expect no-container       "1 2"   "1 2"   no-container reject "is not running"

printf 'OK %s\n' "$name"
