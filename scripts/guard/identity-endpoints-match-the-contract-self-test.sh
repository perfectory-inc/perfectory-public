#!/usr/bin/env bash
# Proves the endpoint guard rejects what it claims to reject.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/identity-endpoints-match-the-contract.sh"
name="identity-endpoints-match-the-contract-self-test"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL ${name}: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

tracked=(
  platforms/identity-platform/config/identity-runtime-endpoints.contract.json
  platforms/identity-platform/infra/zitadel/docker-compose.yml
  platforms/identity-platform/compose.server.yml
  platforms/foundation-platform/compose.identity-bridge.yml
  platforms/identity-platform/scripts/deploy/zitadel-runtime.sh
  platforms/identity-platform/scripts/deploy/identity-runtime.sh
  platforms/foundation-platform/scripts/deploy/foundation-runtime.sh
)

fixture() {
  local label="$1"
  local root="$test_root/$label"
  local file
  for file in "${tracked[@]}"; do
    mkdir -p "$root/$(dirname "$file")"
    cp "$file" "$root/$file"
  done
  printf '%s' "$root"
}

expect() {
  local label="$1" want="$2" needle="${3:-}"
  local root="$test_root/$label" output rc
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

root="$(fixture intact)"
expect intact accept

root="$(fixture drifted-bridge-default)"
python3 - "$root/platforms/foundation-platform/compose.identity-bridge.yml" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("FOUNDATION_PLATFORM_IDENTITY_ISSUER_LOOPBACK_PORT:-18453",
                 "FOUNDATION_PLATFORM_IDENTITY_ISSUER_LOOPBACK_PORT:-18999")
)
PY
expect drifted-bridge-default reject "contract says"

root="$(fixture restored-port-literal)"
python3 - "$root/platforms/identity-platform/infra/zitadel/docker-compose.yml" <<'PY'
import io, re, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
text = re.sub(r"ZITADEL_PORT: \$\{IDENTITY_ZITADEL_LOOPBACK_PORT[^}]*\}",
              'ZITADEL_PORT: "18453"', text)
io.open(path, "w", encoding="utf-8", newline="").write(text)
PY
expect restored-port-literal reject "outside the guarded ports entry"

root="$(fixture drifted-ports-default)"
python3 - "$root/platforms/identity-platform/infra/zitadel/docker-compose.yml" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("IDENTITY_ZITADEL_LOOPBACK_PORT:-18453", "IDENTITY_ZITADEL_LOOPBACK_PORT:-18999")
)
PY
expect drifted-ports-default reject "ports entry"

root="$(fixture wrapper-forgets-the-contract)"
python3 - "$root/platforms/identity-platform/scripts/deploy/zitadel-runtime.sh" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("identity-runtime-endpoints.contract.json", "a-file-that-owns-nothing.json")
)
PY
expect wrapper-forgets-the-contract reject "does not derive"

root="$(fixture renamed-alias-target)"
python3 - "$root/platforms/identity-platform/compose.server.yml" <<'PY'
import io, sys
path = sys.argv[1]
text = io.open(path, encoding="utf-8", newline="").read()
io.open(path, "w", encoding="utf-8", newline="").write(
    text.replace("TCP:zitadel:", "TCP:zit:")
)
PY
expect renamed-alias-target reject "does not target"

printf 'OK %s\n' "$name"
