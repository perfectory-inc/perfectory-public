#!/usr/bin/env bash
# Exercises configured, no-payment, and fail-closed Actions cache API states.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/github/check-actions-cache-controls.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *) echo "actions-cache-controls-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
include=0
jq_filter=""
endpoint=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    api|--hostname|-H) [ "$1" = api ] || shift ;;
    --include) include=1 ;;
    --jq) shift; jq_filter="${1:-}" ;;
    repos/*|orgs/*) endpoint="$1" ;;
  esac
  shift
done
case "$endpoint" in
  */actions/cache/storage-limit) field=max_cache_size_gb; value="${FAKE_CACHE_SIZE:-10}" ;;
  */actions/cache/retention-limit) field=max_cache_retention_days; value="${FAKE_CACHE_RETENTION:-7}" ;;
  */actions/cache/usage)
    printf '0\t0\n'
    exit 0
    ;;
  orgs/*)
    printf '%s\n' "${FAKE_OWNER_PLAN:-free}"
    exit 0
    ;;
  *) echo "unexpected fake endpoint: $endpoint" >&2; exit 90 ;;
esac
if [ "${FAKE_CACHE_STATE:-configured}" = unavailable ]; then
  if [ "$include" -eq 1 ]; then
    printf 'HTTP/2.0 %s Payment Required\r\n\r\n' "${FAKE_CACHE_STATUS:-402}"
    printf '{"message":"%s","status":"%s"}\n' \
      "${FAKE_CACHE_MESSAGE:-Please ensure your account has a valid payment method on file to access this service.}" \
      "${FAKE_CACHE_STATUS:-402}"
  fi
  exit 1
fi
if [ "$include" -eq 1 ]; then
  printf 'HTTP/2.0 200 OK\r\n\r\n{"%s":%s}\n' "$field" "$value"
elif [ -n "$jq_filter" ]; then
  printf '%s\n' "$value"
fi
SH
chmod +x "$fake_bin/gh"

run_checker() {
  PATH="$fake_bin:$PATH" "$checker" \
    perfectory-inc/perfectory-public \
    tools/github/repository-identity.json \
    tools/github/actions-cache-policy.json
}

run_checker >/dev/null
FAKE_CACHE_STATE=unavailable run_checker >/dev/null

if FAKE_CACHE_SIZE=11 run_checker >/dev/null 2>&1; then
  echo "FAIL actions-cache-controls-self-test: accepted cache storage above policy" >&2
  exit 1
fi
if FAKE_CACHE_STATE=unavailable FAKE_CACHE_MESSAGE=unexpected \
  run_checker >/dev/null 2>&1; then
  echo "FAIL actions-cache-controls-self-test: accepted an unknown 402 response" >&2
  exit 1
fi
if FAKE_CACHE_STATE=unavailable FAKE_OWNER_PLAN=paid \
  run_checker >/dev/null 2>&1; then
  echo "FAIL actions-cache-controls-self-test: accepted paid-plan unavailable controls" >&2
  exit 1
fi

echo "OK actions-cache-controls-self-test"
