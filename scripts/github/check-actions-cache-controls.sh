#!/usr/bin/env bash
# Verifies fail-closed, no-paid-opt-in Actions cache controls for the public repo.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <owner/repository> <repository-identity.json> <cache-policy.json>" >&2
  exit 2
fi
target="$1"
repository_identity="$2"
cache_policy="$3"
expected_target="perfectory-inc/perfectory-public"
api_version="2026-03-10"

if [ "$target" != "$expected_target" ]; then
  echo "FAIL actions-cache-controls: refusing unexpected target '$target'" >&2
  exit 1
fi
if [ -n "${GH_HOST:-}" ] && [ "$GH_HOST" != github.com ]; then
  echo "FAIL actions-cache-controls: GH_HOST must be unset or github.com" >&2
  exit 1
fi
for command_name in gh grep mktemp python3 sed; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL actions-cache-controls: missing command '$command_name'" >&2
    exit 1
  }
done
[ -f "$repository_identity" ] && [ -f "$cache_policy" ] || {
  echo "FAIL actions-cache-controls: identity or cache policy is missing" >&2
  exit 1
}

api() {
  env -u GH_HOST gh api --hostname github.com \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: $api_version" \
    "$@"
}

read_cache_limit() {
  local endpoint="$1"
  local field="$2"
  local maximum="$3"
  local output_name="$4"
  local response error_file exit_code status actual message expected_status expected_message
  error_file="$(mktemp)"
  set +e
  response="$(api --include "$endpoint" 2>"$error_file")"
  exit_code=$?
  set -e
  status="$(printf '%s\n' "$response" | sed -n '1s#^HTTP/[0-9.]* \([0-9][0-9][0-9]\).*#\1#p')"
  if [ "$status" = 200 ] && [ "$exit_code" -eq 0 ]; then
    actual="$(api "$endpoint" --jq ".$field")"
    if ! printf '%s\n' "$actual" | grep -Eq '^[0-9]+$' \
      || [ "$actual" -gt "$maximum" ]; then
      echo "FAIL actions-cache-controls: $field must be at most $maximum, found '$actual'" >&2
      rm -f -- "$error_file"
      return 1
    fi
    printf -v "$output_name" '%s' configured
    rm -f -- "$error_file"
    return
  fi

  expected_status="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["unavailable_http_status"])' "$cache_policy")"
  expected_message="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["unavailable_message"])' "$cache_policy")"
  message="$(printf '%s\n' "$response" | python3 -c '
import json, sys
raw = sys.stdin.read()
start = raw.find("{")
if start < 0:
    print("")
else:
    try:
        value, _ = json.JSONDecoder().raw_decode(raw[start:])
        print(value.get("message", ""))
    except (json.JSONDecodeError, AttributeError):
        print("")
')"
  if [ "$status" != "$expected_status" ] \
    || [ "$message" != "$expected_message" ]; then
    echo "FAIL actions-cache-controls: unexpected cache-limit response HTTP ${status:-unknown}" >&2
    [ ! -s "$error_file" ] || sed -n '1,3p' "$error_file" >&2
    rm -f -- "$error_file"
    return 1
  fi
  printf -v "$output_name" '%s' unavailable
  rm -f -- "$error_file"
}

max_size="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["max_cache_size_gb"])' "$cache_policy")"
max_retention="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["max_cache_retention_days"])' "$cache_policy")"
read_cache_limit "repos/$target/actions/cache/storage-limit" \
  max_cache_size_gb "$max_size" size_state
read_cache_limit "repos/$target/actions/cache/retention-limit" \
  max_cache_retention_days "$max_retention" retention_state
if [ "$size_state" != "$retention_state" ]; then
  echo "FAIL actions-cache-controls: cache storage and retention controls disagree" >&2
  exit 1
fi
if [ "$size_state" = unavailable ]; then
  owner_login="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["owner"]["login"])' "$repository_identity")"
  required_plan="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["required_owner_plan_when_unavailable"])' "$cache_policy")"
  owner_plan="$(api "orgs/$owner_login" --jq '.plan.name')"
  if [ "$owner_plan" != "$required_plan" ]; then
    echo "FAIL actions-cache-controls: unavailable controls require owner plan '$required_plan', found '$owner_plan'" >&2
    exit 1
  fi
fi

usage="$(api "repos/$target/actions/cache/usage" \
  --jq '[.active_caches_size_in_bytes, .active_caches_count] | @tsv')"
if ! printf '%s\n' "$usage" | grep -Eq '^[0-9]+[[:space:]][0-9]+$'; then
  echo "FAIL actions-cache-controls: cache usage response is malformed" >&2
  exit 1
fi
echo "OK actions-cache-controls state=$size_state usage-bytes/count=$usage"
