#!/usr/bin/env bash
# Proves schema-registry refuses what it claims (root ADR-0336), with the checker's own tests: in
# fixture repositories they register a revision that adds a required field, makes an optional one
# required, changes a type, widens a closed enum, drops a field a consumer requires, carries its
# own number in schema_version, comes as legacy after a registered one, takes a legacy string as a
# new major, uses a keyword outside the subset, edits a merged revision or its file, and a lane line
# or a source file that spells the version by hand; and they pass the 10-27 v1 -> v2 expansion
# registered again, an added optional field, and the committed registry.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"
root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/platforms/foundation-platform/orchestration/tests/test_schema_registry.py"
py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }
if ! py -m unittest -q "$tests" >"${TMPDIR:-/tmp}/schema-registry-self-test.$$" 2>&1; then
  cat "${TMPDIR:-/tmp}/schema-registry-self-test.$$" >&2
  rm -f "${TMPDIR:-/tmp}/schema-registry-self-test.$$"
  echo "FAIL schema-registry-self-test: the checker's tests failed" >&2
  exit 1
fi
rm -f "${TMPDIR:-/tmp}/schema-registry-self-test.$$"
echo "OK schema-registry-self-test"
