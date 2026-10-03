#!/usr/bin/env bash
# Prints the immutable identity candidate for the one approved GitHub.com
# repository. It validates but never edits the checked-in policy.
#
# The repository is public, so the identity is read from GitHub's REST API
# without any credential (ADR-0136): no `gh`, no token, no netrc, no curlrc.
# The host is fixed in the URL below; no environment variable can change it.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
target="perfectory-inc/perfectory-public"
url="https://api.github.com/repos/$target"
helper="$root/scripts/github/github-policy-json.py"
for command_name in curl mktemp python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-repository-identity: missing command '$command_name'" >&2
    exit 1
  }
done

work="$(mktemp -d)"
cleanup() {
  [ ! -e "${work:-}" ] || rm -rf -- "$work"
}
trap cleanup EXIT

# -q (first) ignores ~/.curlrc. No --netrc/--user/Authorization: the read is anonymous.
# Proxy and CA variables are dropped so the caller's shell cannot redirect or re-root the read.
set +e
status="$(env -u HTTPS_PROXY -u https_proxy -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy \
  -u CURL_CA_BUNDLE -u SSL_CERT_FILE -u SSL_CERT_DIR -u CURL_HOME \
  curl -q --proto '=https' --tlsv1.2 --silent --show-error \
  --connect-timeout 10 --max-time 30 --max-redirs 0 \
  --header 'Accept: application/vnd.github+json' \
  --header 'X-GitHub-Api-Version: 2022-11-28' \
  --output "$work/repository.json" --write-out '%{http_code}' \
  "$url")"
curl_status=$?
set -e
if [ "$curl_status" -ne 0 ]; then
  echo "FAIL public-repository-identity: $url is unreachable (curl exit $curl_status)" >&2
  exit 1
fi
if [ "$status" != 200 ]; then
  echo "FAIL public-repository-identity: $url answered HTTP $status, not 200" >&2
  exit 1
fi

python3 "$helper" repository-identity-from-rest "$work/repository.json" >"$work/candidate.json"
python3 "$helper" validate-repository-identity "$work/candidate.json"
python3 "$helper" canonical "$work/candidate.json"
