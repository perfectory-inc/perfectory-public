#!/usr/bin/env bash
# Every Prometheus rules file passes promtool's check, every rules test passes, and a group that has
# one alert under test has all of them under test (root ADR-0385).
#
# What real incident does failing this prevent? From 2026-10-28 to 11-03 the kernel killed 100
# processes on ai-server, 99 of them a Spark java at a 2.5GiB container cap, and
# foundation-onbid-silver-load.service exited 137 65 times; nothing said OOM because no host series
# existed. The host rules that now say so (foundation-host in foundation-api.rules.yml) only help if
# their expressions fire on the series the collectors write: a renamed metric, a wrong label or a
# threshold typo is a rule that never fires, which is the same silence. promtool, the engine's own
# unit tester, feeds each rule synthetic series and checks the alert it raises, the way the
# Prometheus project and the kubernetes-mixin test their rules.
#
# promtool comes from the pinned Prometheus release (tools/promtool.env, root ADR-0197); its
# version must equal the Prometheus image compose runs, so the rules are tested by the engine that
# evaluates them.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="prometheus-rules-pass-their-tests"
fetcher="$(cd "$(dirname "$0")/../.." && pwd -P)/scripts/ci/fetch_pinned.py"
for command_name in mktemp python3 tar uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL ${name}: missing command '$command_name'" >&2
    exit 1
  }
done

case "$(uname -s)" in
  Linux) os=linux; executable_name=promtool ;;
  Darwin) os=darwin; executable_name=promtool ;;
  MINGW*|MSYS*|CYGWIN*) os=windows; executable_name=promtool.exe ;;
  *) echo "FAIL ${name}: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "FAIL ${name}: unsupported machine $(uname -m)" >&2; exit 1 ;;
esac

# The pin and the image: one version for both.
pinned="$(sed -n 's/^PROMTOOL_VERSION=//p' "$root/tools/promtool.env")"
image="$(sed -n 's/^ *image: prom\/prometheus:v\([^@]*\)@.*/\1/p' "$root/platforms/foundation-platform/compose.observability.yml")"
if [ -z "$pinned" ] || [ "$pinned" != "$image" ]; then
  echo "FAIL ${name}: promtool ${pinned:-<none>} (tools/promtool.env) is not the Prometheus image ${image:-<none>} (compose.observability.yml)" >&2
  exit 1
fi

runtime_root="$(mktemp -d)"
cleanup() {
  if [ -n "${runtime_root:-}" ] && [ -d "$runtime_root" ]; then
    rm -rf -- "$runtime_root"
  fi
}
trap cleanup EXIT
archive="$runtime_root/prometheus.tar.gz"
python3 "$fetcher" tool promtool --output "$archive" >&2
member="prometheus-${pinned}.${os}-${arch}/${executable_name}"
tar -xzf "$archive" -C "$runtime_root" "$member"
promtool="$runtime_root/$member"

rules_dir="$root/platforms/foundation-platform/infra/observability/prometheus"
shopt -s nullglob
rule_files=("$rules_dir"/*.rules.yml)
test_files=("$rules_dir"/*.rules.test.yml)
if [ "${#rule_files[@]}" -eq 0 ] || [ "${#test_files[@]}" -eq 0 ]; then
  echo "FAIL ${name}: no rules or no rules tests under $rules_dir" >&2
  exit 1
fi

rc=0
"$promtool" check rules "${rule_files[@]}" >/dev/null || {
  echo "FAIL ${name}: promtool check rules refused a rules file" >&2
  rc=1
}
for test_file in "${test_files[@]}"; do
  # promtool resolves rule_files against the test file's directory.
  (cd "$rules_dir" && "$promtool" test rules "$(basename "$test_file")") || {
    echo "FAIL ${name}: $(basename "$test_file") failed" >&2
    rc=1
  }
done

# A group with one tested alert has every alert tested: a new rule beside tested ones cannot land
# untested. Read by the narrow shapes the rules files use (no YAML parser on the runner).
python3 - "$name" "${rule_files[@]}" -- "${test_files[@]}" <<'PY' || rc=1
import pathlib
import re
import sys

name = sys.argv[1]
split = sys.argv.index("--")
groups: dict[str, list[str]] = {}
for path in sys.argv[2:split]:
    group = None
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if match := re.match(r"^  - name: (\S+)\s*$", line):
            group = match.group(1)
            groups.setdefault(group, [])
        elif (match := re.match(r"^      - alert: (\S+)\s*$", line)) and group:
            groups[group].append(match.group(1))
tested = set()
for path in sys.argv[split + 1:]:
    tested |= set(re.findall(r"^\s*alertname: (\S+)\s*$", pathlib.Path(path).read_text(encoding="utf-8"), re.M))
unknown = tested - {alert for alerts in groups.values() for alert in alerts}
failed = False
for alert in sorted(unknown):
    print(f"FAIL {name}: a test names {alert}, which no rules file defines", file=sys.stderr)
    failed = True
for group, alerts in sorted(groups.items()):
    if tested & set(alerts):
        for alert in alerts:
            if alert not in tested:
                print(f"FAIL {name}: group {group} has tested alerts but no test for {alert}", file=sys.stderr)
                failed = True
sys.exit(1 if failed else 0)
PY

if [ "$rc" -ne 0 ]; then
  exit 1
fi
echo "OK ${name}: rules=${#rule_files[@]} tests=${#test_files[@]} promtool=${pinned}"
