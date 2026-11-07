#!/usr/bin/env bash
# Proves the lefthook policies refuse what they claim to: their Rego unit tests
# (tools/conftest/lefthook/*_test.rego) plant each fallback in a parsed lefthook.yml.
set -euo pipefail
cd "$(dirname "$0")/../.."
if ! bash scripts/ci/conftest.sh verify --policy tools/conftest/lefthook --no-color; then
  echo "FAIL lefthook-policy-self-test: a lefthook policy unit test failed" >&2
  exit 1
fi
echo "OK lefthook-policy-self-test"
