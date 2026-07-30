#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/document-contract-markers.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL document-contract-markers-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Builds a synthetic repository: one document body and one test body. Both are written verbatim so a
# fixture can hold a deliberately malformed marker.
fixture() {
  local root="$1"
  local document_body="$2"
  local source_body="$3"
  mkdir -p "$root/docs/architecture" "$root/platforms/example-platform/tests"
  {
    printf -- '---\nstatus: current\n---\n\n# Example\n\n'
    printf '%s\n' "$document_body"
  } >"$root/docs/architecture/example.md"
  printf '%s\n' "$source_body" >"$root/platforms/example-platform/tests/test_example.py"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL document-contract-markers-self-test: rejected allowed fixture $1" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL document-contract-markers-self-test: accepted forbidden fixture $1" >&2
    exit 1
  fi
}

declared_and_referenced="$test_root/declared-and-referenced"
fixture "$declared_and_referenced" \
'<!-- contract: alpha-rule -->
## 알파 규칙' \
'CONTRACT_IDS = {"alpha-rule"}'
expect_allowed "$declared_and_referenced"

# The defect this guard exists for: a test asserting an id no document declares.
dangling="$test_root/dangling"
fixture "$dangling" \
'<!-- contract: alpha-rule -->
## 알파 규칙' \
'CONTRACT_IDS = {"alpha-rule", "beta-rule"}'
expect_rejected "$dangling"

# A marker naming two sections makes a reference ambiguous.
duplicate_in_document="$test_root/duplicate-in-document"
fixture "$duplicate_in_document" \
'<!-- contract: alpha-rule -->
## 알파 규칙

<!-- contract: alpha-rule -->
## 다른 절' \
'CONTRACT_IDS = {"alpha-rule"}'
expect_rejected "$duplicate_in_document"

# A malformed marker declares nothing while looking declarative, so it must fail loudly.
malformed="$test_root/malformed"
fixture "$malformed" \
'<!-- contract: Alpha_Rule -->
## 알파 규칙' \
'CONTRACT_IDS = {"alpha-rule"}'
expect_rejected "$malformed"

# An inline marker is malformed too: it does not sit on the section it would name.
inline="$test_root/inline"
fixture "$inline" \
'## 알파 규칙 <!-- contract: alpha-rule -->' \
'CONTRACT_IDS = {"alpha-rule"}'
expect_rejected "$inline"

empty_collection="$test_root/empty-collection"
fixture "$empty_collection" \
'<!-- contract: alpha-rule -->
## 알파 규칙' \
'CONTRACT_IDS: set[str] = set()
CONTRACT_IDS = {}'
expect_rejected "$empty_collection"

# Unrelated kebab-case strings must not be read as references. `"utf-8"` was a real false positive
# from the first version, which scanned every quoted kebab-case string in a participating file.
unrelated_kebab="$test_root/unrelated-kebab"
fixture "$unrelated_kebab" \
'<!-- contract: alpha-rule -->
## 알파 규칙' \
'TEXT = open("x").read()  # encoding="utf-8", vendor-name, some-other-string
CONTRACT_IDS = {"alpha-rule"}'
expect_allowed "$unrelated_kebab"

# A document with no markers is not a participant and must not fail.
no_markers="$test_root/no-markers"
fixture "$no_markers" '## 알파 규칙' 'X = 1'
expect_allowed "$no_markers"

echo "OK document-contract-markers-self-test"
