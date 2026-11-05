#!/usr/bin/env bash
# Proves every rule under tools/semgrep/rules fires on its incident and stays quiet on the accepted
# forms: each rule file has a fixture of the same name whose `ruleid:` lines must match and whose
# `ok:` lines must not (`opengrep scan --test`). A rule with no fixture is refused.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
missing=0
for rule in tools/semgrep/rules/*.yml; do
  base="${rule%.yml}"
  fixtures=("$base".*)
  if [ "${#fixtures[@]}" -lt 2 ]; then
    echo "FAIL semgrep-rules-self-test: $rule has no fixture ($base.<ext>)" >&2
    missing=1
  fi
done
[ "$missing" -eq 0 ] || exit 1
if ! bash scripts/ci/opengrep.sh scan --test --disable-version-check tools/semgrep/rules; then
  echo "FAIL semgrep-rules-self-test: a rule missed its incident or flagged an accepted form" >&2
  exit 1
fi
echo "OK semgrep-rules-self-test"
