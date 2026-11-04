#!/usr/bin/env bash
# Code-pattern rules as Semgrep rule files (tools/semgrep/rules/*.yml), run by the pinned Opengrep
# engine over every tracked file. Each rule file names the incident it prevents (root ADR-0001);
# this script only runs them, so a new code-pattern guard is a rule file, not a new parser.
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
cd "$root"
if ! bash scripts/ci/opengrep.sh scan --config tools/semgrep/rules --error --quiet \
  --disable-version-check .; then
  echo "FAIL semgrep-rules: a rule under tools/semgrep/rules matched (see the findings above)" >&2
  exit 1
fi
echo "OK semgrep-rules"
