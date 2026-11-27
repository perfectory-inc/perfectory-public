#!/usr/bin/env bash
# The upgrade canary (root ADR-0624, decision 6): every step of the contract's upgrade_canary, in order, each one
# probe run (probe.sh). A step with a known_failure is expected to fail, strictly: when it passes, the canary fails
# too, so the version pin, the decision and the list move together. Prints one line per step and exits 0 only when
# every step did what the list says. Run after applying a new Lakekeeper or OpenFGA version here, before it is pinned
# for anything else.
set -euo pipefail
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8

here="$(cd "$(dirname "$0")" && pwd -P)"
if command -v cygpath >/dev/null; then here="$(cygpath -m "$here")"; fi
contract="$here/../../config/lakekeeper-dev.contract.json"
logs="$(mktemp -d)"

# One line per step: the test, what is expected (pass or fail), then the probe.sh arguments, tab-separated.
listing="$(python3 -c '
import json, sys
for step in json.load(open(sys.argv[1], encoding="utf-8"))["upgrade_canary"]["steps"]:
    print(chr(9).join([step["test"], "fail" if "known_failure" in step else "pass", *step["run"]]))
' "$contract")"
mapfile -t steps <<<"${listing//$'\r'/}"

wrong=0
for line in "${steps[@]}"; do
  IFS=$'\t' read -r -a fields <<<"$line"
  test="${fields[0]}" expected="${fields[1]}" run=("${fields[@]:2}")
  log="$logs/$test-${run[0]}-${run[1]}.log"
  if bash "$here/probe.sh" "${run[@]}" >"$log" 2>&1; then got=pass; else got=fail; fi
  verdict=ok
  [ "$got" = "$expected" ] || { verdict=WRONG; wrong=$((wrong + 1)); }
  printf '{"test": "%s", "run": "%s", "expected": "%s", "got": "%s", "verdict": "%s", "log": "%s"}\n' \
    "$test" "${run[*]}" "$expected" "$got" "$verdict" "$log"
done
result=fail
[ "$wrong" != 0 ] || result=pass
printf '{"canary": "%s", "steps": %d, "wrong": %d}\n' "$result" "${#steps[@]}" "$wrong"
[ "$result" = pass ]
