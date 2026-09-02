#!/usr/bin/env bash
# Proves the runtime environment check answers correctly, without a deployment host.
#
# Like the migration check, this one runs where CI cannot reach, so the comparison would
# otherwise be exercised only by the incident it exists to catch. Release directory and
# environment file are substituted; the check's own logic is not.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/platforms/foundation-platform/scripts/deploy/assert-runtime-environment.sh"
name="assert-runtime-environment-self-test"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL ${name}: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# The check reads which compose files the runtime loads out of `foundation-runtime.sh`'s `-f`
# arguments, so every fixture ships one. Writing the list into the fixture instead would make the
# self-test agree with a copy rather than with the thing the check actually reads.
write_runtime_wrapper() {
  local root="$1"; shift
  mkdir -p "$root/scripts/deploy"
  {
    printf '#!/usr/bin/env bash\n'
    printf 'compose=(\n'
    printf '  docker compose\n'
    local file
    for file in "$@"; do
      printf '  -f "${root_dir}/%s"\n' "$file"
    done
    printf ')\n'
  } >"$root/scripts/deploy/foundation-runtime.sh"
  chmod +x "$root/scripts/deploy/foundation-runtime.sh"
}

fixture() {
  local label="$1" compose="$2" env_body="$3"
  local root="$test_root/$label"
  mkdir -p "$root"
  case "$compose" in
    both)
      cat >"$root/docker-compose.yml" <<'YML'
services:
  api:
    environment:
      A: ${ALPHA:?set ALPHA}
      B: ${BETA:-a-default-nobody-must-supply}
YML
      cat >"$root/compose.recovery.yml" <<'YML'
services:
  writer:
    environment:
      C: ${GAMMA:?set GAMMA}
YML
      write_runtime_wrapper "$root" docker-compose.yml compose.recovery.yml
      ;;
    missing-recovery)
      cat >"$root/docker-compose.yml" <<'YML'
services: {}
YML
      # The wrapper still loads two files; only one is on disk.
      write_runtime_wrapper "$root" docker-compose.yml compose.recovery.yml
      ;;
    no-required)
      cat >"$root/docker-compose.yml" <<'YML'
services:
  api:
    environment:
      B: ${BETA:-a-default}
YML
      cat >"$root/compose.recovery.yml" <<'YML'
services: {}
YML
      write_runtime_wrapper "$root" docker-compose.yml compose.recovery.yml
      ;;
    three-files)
      # A third compose file joins the runtime. A check holding its own list would keep asking
      # about two and report complete coverage of a set it no longer covers.
      cat >"$root/docker-compose.yml" <<'YML'
services:
  api:
    environment:
      A: ${ALPHA:?set ALPHA}
YML
      cat >"$root/compose.recovery.yml" <<'YML'
services: {}
YML
      cat >"$root/compose.extra.yml" <<'YML'
services:
  extra:
    environment:
      D: ${DELTA:?set DELTA}
YML
      write_runtime_wrapper "$root" docker-compose.yml compose.recovery.yml compose.extra.yml
      ;;
  esac
  case "$env_body" in
    none) ;;
    *) printf '%s\n' "$env_body" >"$root/runtime.env" ;;
  esac
  printf '%s' "$root"
}

expect() {
  local label="$1" compose="$2" env_body="$3" want="$4" needle="${5:-}"
  local root output rc
  root="$(fixture "$label" "$compose" "$env_body")"
  output="$(bash "$checker" "$root" "$root/runtime.env" 2>&1)" && rc=0 || rc=$?
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

expect in-step both 'ALPHA=x
GAMMA=y' accept "2 required variable(s), all present"

# The 2026-09-01 shape: the file carries the old names and none of the new ones.
expect renamed-away both 'OLD_ALPHA=x
OLD_GAMMA=y' reject "missing ALPHA"
expect names-every-gap both 'OLD_ALPHA=x
OLD_GAMMA=y' reject "missing GAMMA"

# A variable with a default is the deployment's choice, not a requirement. Demanding it would
# make this check refuse hosts that work.
expect defaults-are-not-required both 'ALPHA=x
GAMMA=y' accept

# A name that only appears as a prefix of another must not count as present.
expect prefix-is-not-a-match both 'ALPHABET=x
GAMMA=y' reject "missing ALPHA"

# The list comes from the wrapper, so a file added there is covered without editing this check.
expect a-third-compose-file-is-covered three-files 'ALPHA=x' reject "missing DELTA"

expect unreadable-env both none reject "cannot read"
expect missing-compose-file missing-recovery 'ALPHA=x' reject "missing a compose file"
expect nothing-required no-required 'ALPHA=x' reject "no required variables"

printf 'OK %s\n' "$name"
