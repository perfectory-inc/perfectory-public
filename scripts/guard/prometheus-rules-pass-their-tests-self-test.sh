#!/usr/bin/env bash
# Proves prometheus-rules-pass-their-tests rejects what it claims to reject: a host rule that no
# longer fires on its incident's series, a rule reading a renamed metric, a new alert beside tested
# ones without a test, and promtool drifting from the Prometheus image.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/prometheus-rules-pass-their-tests.sh"
real="$(pwd -P)"
name="prometheus-rules-pass-their-tests-self-test"
rules="platforms/foundation-platform/infra/observability/prometheus"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL ${name}: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

fixture() {
  local label="$1" root="$test_root/$1"
  mkdir -p "$root/tools" "$root/$rules"
  cp "$real/tools/promtool.env" "$root/tools/"
  cp "$real/platforms/foundation-platform/compose.observability.yml" "$root/platforms/foundation-platform/"
  cp "$real/$rules"/*.yml "$root/$rules/"
  printf '%s\n' "$root"
}

# Replace one literal string in a fixture file; a replacement that matches nothing is a broken test.
replace() {
  python3 - "$1" "$2" "$3" <<'PY'
import sys
path, old, new = sys.argv[1:]
text = open(path, encoding="utf-8").read()
if old not in text:
    sys.exit(f"self-test fixture: {old!r} not in {path}")
open(path, "w", encoding="utf-8", newline="\n").write(text.replace(old, new, 1))
PY
}

expect() {
  local verdict="$1" label="$2" root="$3"
  if bash "$checker" "$root" >"$test_root/$label.out" 2>&1; then
    actual=pass
  else
    actual=fail
  fi
  if [ "$actual" != "$verdict" ]; then
    echo "FAIL ${name}: ${label} was expected to ${verdict} but did ${actual}" >&2
    cat "$test_root/$label.out" >&2
    exit 1
  fi
}

expect pass intact "$(fixture intact)"

root="$(fixture oom-threshold-typo)"
replace "$root/$rules/foundation-api.rules.yml" "increase(node_vmstat_oom_kill[1h]) > 0" "increase(node_vmstat_oom_kill[1h]) > 5"
expect fail oom-threshold-typo "$root"

root="$(fixture renamed-metric)"
replace "$root/$rules/foundation-api.rules.yml" "expr: node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes" "expr: node_memory_MemFree_bytes / node_memory_MemTotal_bytes"
expect fail renamed-metric "$root"

root="$(fixture untested-alert)"
replace "$root/$rules/foundation-api.rules.yml" "      - alert: FoundationHostMetricsUnread" "      - alert: FoundationHostSwapFull
        expr: node_memory_SwapFree_bytes == 0
        labels:
          severity: ticket
          service: host
        annotations:
          summary: swap
      - alert: FoundationHostMetricsUnread"
expect fail untested-alert "$root"

root="$(fixture promtool-drift)"
replace "$root/tools/promtool.env" "PROMTOOL_VERSION=" "PROMTOOL_VERSION=0."
expect fail promtool-drift "$root"

echo "OK ${name}: 4 rejected, intact passes"
