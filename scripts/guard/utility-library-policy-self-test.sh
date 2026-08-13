#!/usr/bin/env bash
# Proves the utility-library guard rejects what it claims to reject, and — just as
# important — that it does not react to a name written in prose.
#
# The real repository declares none of the banned packages, so it can only ever
# exercise the passing path. A guard whose failing path was never run is a guard
# nobody has evidence for.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/utility-library-policy.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL utility-library-policy-self-test: missing $checker" >&2
  exit 1
fi

work="$(mktemp -d)"
cleanup() {
  if [ -n "${work:-}" ] && [ -d "$work" ]; then
    rm -rf -- "$work"
  fi
}
trap cleanup EXIT

manifest() {
  local name="$1"
  shift
  local path="$work/$name.json"
  printf '%s\n' "$@" >"$path"
  printf '%s' "$path"
}

expect_accepted() {
  local label="$1" path="$2"
  if ! bash "$checker" "$path" >/dev/null 2>&1; then
    echo "FAIL utility-library-policy-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1" path="$2"
  if bash "$checker" "$path" >/dev/null 2>&1; then
    echo "FAIL utility-library-policy-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

expect_accepted "the canonical library alone" "$(manifest canonical \
  '{' \
  '  "dependencies": {' \
  '    "es-toolkit": "^1.39.0",' \
  '    "react": "19.2.5"' \
  '  }' \
  '}')"

expect_rejected "the displaced default" "$(manifest displaced \
  '{' \
  '  "dependencies": {' \
  '    "lodash": "^4.17.21"' \
  '  }' \
  '}')"

# Per-function packages are the same dependency wearing a different name, and are
# how the ban gets routed around one import at a time.
expect_rejected "a single-function split package" "$(manifest split \
  '{' \
  '  "dependencies": {' \
  '    "lodash.debounce": "^4.0.8"' \
  '  }' \
  '}')"

# Types-only is still a declaration that the runtime package is expected.
expect_rejected "a types-only declaration in devDependencies" "$(manifest types \
  '{' \
  '  "devDependencies": {' \
  '    "@types/lodash": "^4.17.0"' \
  '  }' \
  '}')"

expect_rejected "a different alternative" "$(manifest alternative \
  '{' \
  '  "dependencies": {' \
  '    "ramda": "^0.30.1"' \
  '  }' \
  '}')"

# A second role, to prove the list is a list and not one hard-coded name.
expect_rejected "a rival for a role §1.1 already assigned" "$(manifest rival \
  '{' \
  '  "dependencies": {' \
  '    "react-error-boundary": "^6.0.0"' \
  '  }' \
  '}')"

# The name written as a value, not a key. Migration notes and deprecation comments
# have to be able to say what was removed, or the guard makes its own history
# unwritable — the failure mode this repository has already hit with text guards.
expect_accepted "the name appearing only as a value" "$(manifest prose \
  '{' \
  '  "description": "migrated off lodash to es-toolkit",' \
  '  "config": { "replaces": "lodash" },' \
  '  "dependencies": {' \
  '    "es-toolkit": "^1.39.0"' \
  '  }' \
  '}')"

echo "OK utility-library-policy-self-test"
