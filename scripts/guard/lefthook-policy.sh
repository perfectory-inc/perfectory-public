#!/usr/bin/env bash
# The root lefthook.yml against the Rego policies in tools/conftest/lefthook (root ADR-0422), read
# by Conftest's YAML parser. Each policy names the incident it prevents (root ADR-0001); this script
# only runs them.
set -euo pipefail
cd "$(dirname "$0")/../.."
if ! bash scripts/ci/conftest.sh test --no-color --policy tools/conftest/lefthook \
  --namespace lefthook lefthook.yml >&2; then
  echo "FAIL lefthook-policy: lefthook.yml breaks a repository contract (see the failures above)" >&2
  exit 1
fi
echo "OK lefthook-policy"
