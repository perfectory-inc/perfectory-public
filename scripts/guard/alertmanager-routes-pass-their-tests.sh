#!/usr/bin/env bash
# The Alertmanager config passes amtool's check, and every alert in alertmanager.routes.test reaches the
# receiver it names (root ADR-0591).
#
# What real incident does failing this prevent? 2026-11-18/19 #alerts got about 480 messages a day and
# nobody could read it: tickets were sent one per job with a resolved message each. The fix routes
# every ticket but the security advisories to a batched receiver; a matcher typo, a route moved above
# it, or a new service route that catches tickets first sends them back to one message per job, and
# nothing in the repository would say so. amtool, Alertmanager's own route tester, resolves each case
# through the routing tree the way the server does.
#
# amtool comes from the pinned Alertmanager release (tools/amtool.env, root ADR-0197); its version must
# equal the Alertmanager image compose runs, so the routes are tested by the engine that routes them.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="alertmanager-routes-pass-their-tests"
fetcher="$(cd "$(dirname "$0")/../.." && pwd -P)/scripts/ci/fetch_pinned.py"
for command_name in mktemp python3 tar uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL ${name}: missing command '$command_name'" >&2
    exit 1
  }
done

# Asked once and apart from the message that names them, so a failing uname stops the guard.
kernel="$(uname -s)"
machine="$(uname -m)"
case "$kernel" in
  Linux) os=linux; executable_name=amtool ;;
  Darwin) os=darwin; executable_name=amtool ;;
  MINGW*|MSYS*|CYGWIN*) os=windows; executable_name=amtool.exe ;;
  *) echo "FAIL ${name}: unsupported operating system ${kernel}" >&2; exit 1 ;;
esac
case "$machine" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "FAIL ${name}: unsupported machine ${machine}" >&2; exit 1 ;;
esac

# The pin and the image: one version for both.
pinned="$(sed -n 's/^AMTOOL_VERSION=//p' "$root/tools/amtool.env")"
image="$(sed -n 's/^ *image: prom\/alertmanager:v\([^@]*\)@.*/\1/p' "$root/platforms/foundation-platform/compose.observability.yml")"
if [ -z "$pinned" ] || [ "$pinned" != "$image" ]; then
  echo "FAIL ${name}: amtool ${pinned:-<none>} (tools/amtool.env) is not the Alertmanager image ${image:-<none>} (compose.observability.yml)" >&2
  exit 1
fi

config_dir="$root/platforms/foundation-platform/infra/observability/alertmanager"
config="$config_dir/alertmanager.yml"
cases="$config_dir/alertmanager.routes.test"
[ -f "$config" ] && [ -f "$cases" ] || {
  echo "FAIL ${name}: $config or $cases is missing" >&2
  exit 1
}

runtime_root="$(mktemp -d)"
cleanup() {
  if [ -n "${runtime_root:-}" ] && [ -d "$runtime_root" ]; then
    rm -rf -- "$runtime_root"
  fi
}
trap cleanup EXIT
archive="$runtime_root/alertmanager.tar.gz"
python3 "$fetcher" tool amtool --output "$archive" >&2
member="alertmanager-${pinned}.${os}-${arch}/${executable_name}"
tar -xzf "$archive" -C "$runtime_root" "$member"
amtool="$runtime_root/$member"

rc=0
# check-config reads the receivers' credential files only when it sends; the token file is not needed.
"$amtool" check-config "$config" >/dev/null || {
  echo "FAIL ${name}: amtool check-config refused alertmanager.yml" >&2
  rc=1
}
count=0
while read -r receiver labels; do
  case "$receiver" in ''|'#'*) continue ;; esac
  count=$((count + 1))
  # shellcheck disable=SC2086 # the labels are separate words: name=value name=value
  if ! got="$("$amtool" config routes test --config.file="$config" --verify.receivers="$receiver" $labels 2>&1)"; then
    echo "FAIL ${name}: {${labels// /, }} reaches ${got%%$'\n'*}, not ${receiver}" >&2
    rc=1
  fi
done <"$cases"
if [ "$count" -eq 0 ]; then
  echo "FAIL ${name}: no route cases in $cases" >&2
  rc=1
fi

# Every alert rule is a case too, so a new rule cannot miss its route by a severity typo or by having
# none: a page reaches slack-page, a ticket outside security slack-ticket, security slack-security, and
# an inhibition source (severity none) inhibition-only.
# Read by the narrow shapes the rules files use (no YAML parser on the runner), as
# prometheus-rules-pass-their-tests reads them.
rules_dir="$root/platforms/foundation-platform/infra/observability/prometheus"
derived="$(python3 - "$rules_dir" <<'PY'
import pathlib
import re
import sys

for path in sorted(pathlib.Path(sys.argv[1]).glob("*.rules.yml")):
    alert = labels = None
    rules = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if match := re.match(r"^      - alert: (\S+)\s*$", line):
            alert, labels = match.group(1), {}
            rules.append((alert, labels))
        elif alert and (match := re.match(r"^          (severity|service): (\S+)\s*$", line)):
            labels.setdefault(match.group(1), match.group(2).strip("'\""))
    for alert, labels in rules:
        severity, service = labels.get("severity", ""), labels.get("service", "")
        if severity not in ("page", "ticket", "none"):
            print(f"BAD {path.name} {alert} severity={severity or '<none>'}")
            continue
        # `none`: an alert that only inhibits others and reaches no one (root ADR-0625).
        want = ("inhibition-only" if severity == "none" else
                "slack-security" if service == "security" else f"slack-{severity}")
        print(f"{want} alertname={alert} severity={severity}" + (f" service={service}" if service else ""))
PY
)"
rules=0
while read -r want labels; do
  [ -n "$want" ] || continue
  if [ "$want" = BAD ]; then
    echo "FAIL ${name}: a rule has no severity of page or ticket, so its route is not known: ${labels}" >&2
    rc=1
    continue
  fi
  rules=$((rules + 1))
  # shellcheck disable=SC2086 # the labels are separate words: name=value name=value
  got="$("$amtool" config routes test --config.file="$config" $labels 2>&1)" || true
  got="${got%%$'\n'*}"
  if [ "$got" != "$want" ]; then
    echo "FAIL ${name}: the rule {${labels// /, }} reaches ${got}, not ${want}" >&2
    rc=1
  fi
done <<<"$derived"
if [ "$rules" -eq 0 ]; then
  echo "FAIL ${name}: no alert rules read under $rules_dir" >&2
  rc=1
fi

if [ "$rc" -ne 0 ]; then
  exit 1
fi
echo "OK ${name}: cases=${count} rules=${rules} amtool=${pinned}"
