#!/usr/bin/env bash
# Proves alertmanager-routes-pass-their-tests rejects what it claims to reject: a ticket route that no
# longer catches tickets, a service route placed above it, a case naming the wrong receiver, a config
# amtool refuses, a rule whose severity is neither page nor ticket, a page the ticket route batches,
# and amtool drifting from the Alertmanager image.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/alertmanager-routes-pass-their-tests.sh"
real="$(pwd -P)"
name="alertmanager-routes-pass-their-tests-self-test"
config="platforms/foundation-platform/infra/observability/alertmanager"
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
  mkdir -p "$root/tools" "$root/$config" "$root/$rules"
  cp "$real/tools/amtool.env" "$root/tools/"
  cp "$real/platforms/foundation-platform/compose.observability.yml" "$root/platforms/foundation-platform/"
  cp "$real/$config/alertmanager.yml" "$real/$config/alertmanager.routes.test" "$root/$config/"
  cp "$real/$rules"/*.rules.yml "$root/$rules/"
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

# The fixtures are independent and each runs the whole checker (every rule through amtool), so they
# run side by side, as prometheus-rules-pass-their-tests-self-test does.
pids=()
expect pass intact "$(fixture intact)" &
pids+=("$!")

root="$(fixture ticket-matcher-typo)"
replace "$root/$config/alertmanager.yml" '        - severity = "ticket"' '        - severity = "tickets"'
expect fail ticket-matcher-typo "$root" &
pids+=("$!")

root="$(fixture service-route-above-tickets)"
replace "$root/$config/alertmanager.yml" "  routes:
" '  routes:
    - matchers:
        - service = "data-flow"
'
expect fail service-route-above-tickets "$root" &
pids+=("$!")

root="$(fixture wrong-receiver)"
replace "$root/$config/alertmanager.routes.test" "slack-security alertname=FoundationSecurityAdvisory " "slack-ticket alertname=FoundationSecurityAdvisory "
expect fail wrong-receiver "$root" &
pids+=("$!")

root="$(fixture unknown-receiver)"
replace "$root/$config/alertmanager.yml" "      receiver: slack-ticket" "      receiver: slack-tickets"
expect fail unknown-receiver "$root" &
pids+=("$!")

root="$(fixture rule-severity-typo)"
replace "$root/$rules/foundation-api.rules.yml" "      - alert: FoundationScheduledJobProblem
        expr: max without (summary, link) (foundation_scheduled_job_problem) == 1
        for: 1m
        keep_firing_for: 15m
        labels:
          severity: ticket" "      - alert: FoundationScheduledJobProblem
        expr: max without (summary, link) (foundation_scheduled_job_problem) == 1
        for: 1m
        keep_firing_for: 15m
        labels:
          severity: tickets"
expect fail rule-severity-typo "$root" &
pids+=("$!")

root="$(fixture page-batched)"
replace "$root/$config/alertmanager.yml" '        - severity = "ticket"
        - service != "security"' '        - severity =~ "ticket|page"
        - service != "security"'
expect fail page-batched "$root" &
pids+=("$!")

root="$(fixture amtool-drift)"
replace "$root/tools/amtool.env" "AMTOOL_VERSION=" "AMTOOL_VERSION=0."
expect fail amtool-drift "$root" &
pids+=("$!")

failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=1
done
[ "$failed" -eq 0 ] || exit 1

echo "OK ${name}: 7 rejected, intact passes"
