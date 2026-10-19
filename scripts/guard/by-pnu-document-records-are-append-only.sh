#!/usr/bin/env bash
# Prevents: a by-PNU document's fields changing under a schema version it already served. #746 added
# fields without a new version; the bake rewrites a whole generation only when the version changes,
# so it weighed every PNU as a patch and stopped at its mass-change brake. The publisher's test
# (foundation-publisher-by-pnu by_pnu_document_records.rs) holds each document to the record of its
# current version; this guard keeps a recorded version from being edited to match: a pull request
# may add a record, never modify, rename or delete one. Without a pull request base
# (CHANGE_SCOPE_BASE) there is nothing to compare and it passes.
set -euo pipefail
cd "${1:-$(dirname "$0")/../..}"
records="platforms/foundation-platform/contracts/by-pnu-documents"
base="${CHANGE_SCOPE_BASE:-}"
head="${CHANGE_SCOPE_HEAD:-HEAD}"
if [ -z "$base" ] || [ -z "${base//0/}" ]; then
  echo "OK by-pnu-document-records-are-append-only: no pull request base to compare with"
  exit 0
fi
if ! changes="$(git diff --no-renames --name-status "$base...$head" -- "$records")"; then
  echo "FAIL by-pnu-document-records-are-append-only: cannot compare $head with base $base" >&2
  exit 1
fi
edited="$(printf '%s\n' "$changes" | awk -F'\t' 'NF && $1 != "A"')"
if [ -n "$edited" ]; then
  printf '%s\n' "$edited" | sed 's/^/  /' >&2
  echo "FAIL by-pnu-document-records-are-append-only: a recorded by-PNU document version changed." \
    "Documents already served under it have its fields: bump the document's schema version and" \
    "add the new version's file instead; never edit, rename or delete an existing one." >&2
  exit 1
fi
echo "OK by-pnu-document-records-are-append-only"
