#!/usr/bin/env bash
# Code-pattern rules as Semgrep rule files (tools/semgrep/rules/*.yml), run by the pinned Opengrep
# engine over every tracked file. Each rule file names the incident it prevents (root ADR-0001);
# this script only runs them, so a new code-pattern guard is a rule file, not a new parser.
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
cd "$root"
# Without it the engine applies its built-in ignore list and skips tests/ and build/ (.semgrepignore).
[ -f .semgrepignore ] || {
  echo "FAIL semgrep-rules: .semgrepignore is missing, so the engine would skip tests/ and build/" >&2
  exit 1
}
# The rule directory holds each rule's `--test` fixture, which plants its incident on purpose;
# semgrep-rules-self-test.sh runs those, so the scan of the tree leaves them out.
if ! bash scripts/ci/opengrep.sh scan --config tools/semgrep/rules --error --quiet \
  --disable-version-check --exclude=tools/semgrep/rules .; then
  echo "FAIL semgrep-rules: a rule under tools/semgrep/rules matched (see the findings above)" >&2
  exit 1
fi
echo "OK semgrep-rules"
