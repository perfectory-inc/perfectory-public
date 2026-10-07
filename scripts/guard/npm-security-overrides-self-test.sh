#!/usr/bin/env bash
# Threat model: honest-mistake detection.
# Prevents: the npm override renderer and checker losing a refusal unnoticed: every check is
# shown to reject a planted violation in a throwaway git repository (root ADR-0158).
# Does not prevent: a maintainer deliberately changing the tests and the tool together.
set -euo pipefail
# The test's fixture repositories must not inherit a hook's binding to this checkout.
. "$(dirname "$0")/lib/fixture-repo.sh"
command -v node >/dev/null || {
  echo "FAIL npm-security-overrides-self-test: missing command 'node'" >&2
  exit 1
}
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
node --test "$root/tools/npm/security-overrides.test.mjs"
