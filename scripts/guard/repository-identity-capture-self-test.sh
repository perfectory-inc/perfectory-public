#!/usr/bin/env bash
# Proves identity capture reads only GitHub.com's public REST API without a
# credential or `gh` (ADR-0136), and rejects a renamed/transferred owner, a
# wrong repository id, a non-200 answer and an unreachable API.
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
# A `gh` that fails loudly: the reader must never call it.
cat >"$test_root/bin/gh" <<'SH'
#!/usr/bin/env bash
echo called >"$GH_CALLED"
exit 1
SH
cat >"$test_root/bin/curl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$CURL_ARGUMENT_CAPTURE"
output=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[ "${FAKE_UNREACHABLE:-0}" = 0 ] || exit 6
repository_id=123456789
owner_id=306911903
[ "${FAKE_WRONG_OWNER:-0}" = 0 ] || owner_id=1
[ "${FAKE_WRONG_REPOSITORY:-0}" = 0 ] || repository_id=987654321
printf '{"id":%s,"node_id":"R_kgDOSynthetic","full_name":"perfectory-inc/perfectory-public","private":false,"owner":{"login":"perfectory-inc","id":%s,"node_id":"O_kgDOEksanw","type":"Organization"}}\n' \
  "$repository_id" "$owner_id" >"$output"
printf '%s' "${FAKE_HTTP_STATUS:-200}"
SH
chmod +x "$test_root/bin/gh" "$test_root/bin/curl"

capture="$test_root/curl.args"
gh_called="$test_root/gh.called"
reader() {
  PATH="$test_root/bin:$PATH" CURL_ARGUMENT_CAPTURE="$capture" GH_CALLED="$gh_called" \
    bash scripts/github/show-public-repository-identity.sh "$@"
}
reader >"$test_root/candidate.json"
python3 scripts/github/github-policy-json.py validate-repository-identity \
  "$test_root/candidate.json"
grep -Fqx -- https://api.github.com/repos/perfectory-inc/perfectory-public "$capture"
grep -Fqx -- '=https' "$capture"
grep -Fqx -- 'Accept: application/vnd.github+json' "$capture"
grep -Fqx -- --max-time "$capture"
if grep -Eiq -- '^(--netrc|--netrc-file|--user|-u|--oauth2-bearer)$|^Authorization:' "$capture"; then
  echo "FAIL repository-identity-capture-self-test: identity read sent a credential" >&2
  exit 1
fi
if [ -e "$gh_called" ]; then
  echo "FAIL repository-identity-capture-self-test: identity read called gh" >&2
  exit 1
fi
# GH_HOST no longer selects anything: the host is fixed in the URL.
GH_HOST=example.invalid reader >/dev/null
grep -Fqx -- https://api.github.com/repos/perfectory-inc/perfectory-public "$capture"

expect_refusal() {
  local label="$1" pattern="$2"
  shift 2
  if env "$@" PATH="$test_root/bin:$PATH" CURL_ARGUMENT_CAPTURE="$capture" GH_CALLED="$gh_called" \
    bash scripts/github/show-public-repository-identity.sh >/dev/null 2>"$test_root/stderr"; then
    echo "FAIL repository-identity-capture-self-test: accepted $label" >&2
    exit 1
  fi
  if ! grep -Eq -- "$pattern" "$test_root/stderr"; then
    echo "FAIL repository-identity-capture-self-test: $label refusal did not say why" >&2
    cat "$test_root/stderr" >&2
    exit 1
  fi
}
expect_refusal "wrong owner identity" 'owner identity drift' FAKE_WRONG_OWNER=1
expect_refusal "non-200 answer" 'public-repository-identity: .*HTTP 404' FAKE_HTTP_STATUS=404
expect_refusal "unreachable API" 'public-repository-identity: .*unreachable' FAKE_UNREACHABLE=1
# A wrong repository id is a valid shape; the caller's comparison with the
# pinned policy refuses it (release admission tests prove that comparison).
FAKE_WRONG_REPOSITORY=1 reader >"$test_root/wrong.json"
if cmp -s "$test_root/wrong.json" "$test_root/candidate.json"; then
  echo "FAIL repository-identity-capture-self-test: repository id was not read from the API" >&2
  exit 1
fi

echo "OK repository-identity-capture-self-test"
