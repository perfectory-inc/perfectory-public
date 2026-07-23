#!/usr/bin/env bash
# Proves identity capture pins GitHub.com and rejects a renamed/transferred owner.
set -euo pipefail
cd "$(dirname "$0")/../.."

test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || rm -rf -- "$test_root" ;;
    *) echo "repository-identity-capture-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT
mkdir -p "$test_root/bin"
cat >"$test_root/bin/gh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$GH_ARGUMENT_CAPTURE"
owner_id=306911903
[ "${FAKE_WRONG_OWNER:-0}" = 0 ] || owner_id=1
printf '{"hostname":"github.com","full_name":"perfectory-inc/perfectory-public","repository_id":123456789,"repository_node_id":"R_kgDOSynthetic","owner":{"login":"perfectory-inc","id":%s,"node_id":"O_kgDOEksanw"}}\n' "$owner_id"
SH
chmod +x "$test_root/bin/gh"

capture="$test_root/gh.args"
PATH="$test_root/bin:$PATH" GH_ARGUMENT_CAPTURE="$capture" \
  bash scripts/github/show-public-repository-identity.sh >"$test_root/candidate.json"
grep -Fqx -- --hostname "$capture"
grep -Fqx -- github.com "$capture"
grep -Fqx -- repos/perfectory-inc/perfectory-public "$capture"
python3 scripts/github/github-policy-json.py validate-repository-identity \
  "$test_root/candidate.json"

if PATH="$test_root/bin:$PATH" GH_ARGUMENT_CAPTURE="$capture" GH_HOST=example.invalid \
  bash scripts/github/show-public-repository-identity.sh >/dev/null 2>&1; then
  echo "FAIL repository-identity-capture-self-test: accepted non-GitHub host" >&2
  exit 1
fi
if PATH="$test_root/bin:$PATH" GH_ARGUMENT_CAPTURE="$capture" FAKE_WRONG_OWNER=1 \
  bash scripts/github/show-public-repository-identity.sh >/dev/null 2>&1; then
  echo "FAIL repository-identity-capture-self-test: accepted wrong owner identity" >&2
  exit 1
fi

echo "OK repository-identity-capture-self-test"
