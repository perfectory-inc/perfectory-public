#!/usr/bin/env bash
# Fails closed when a tracked regular-file blob is missing or exceeds 25 MiB.
set -euo pipefail

limit_bytes=26214400
inventory="$(
  git ls-files --stage \
    | awk '$1 == "100644" || $1 == "100755" {
        tab = index($0, "\t")
        if (tab > 0) print $2 " " substr($0, tab + 1)
      }' \
    | git cat-file --batch-check='%(objecttype) %(objectsize) %(rest)'
)"

unreadable="$(printf '%s\n' "$inventory" | awk 'NF > 0 && $1 != "blob" { print }')"
if [ -n "$unreadable" ]; then
  echo "FAIL tracked-blob-sizes: a tracked blob is missing or unreadable:" >&2
  printf '%s\n' "$unreadable" >&2
  exit 1
fi

oversized="$(
  printf '%s\n' "$inventory" \
    | awk -v limit="$limit_bytes" '$1 == "blob" && $2 > limit {
        size = $2
        $1 = ""
        $2 = ""
        sub(/^[[:space:]]+/, "")
        print $0 " (" size " bytes)"
      }'
)"
if [ -n "$oversized" ]; then
  echo "FAIL tracked-blob-sizes: tracked regular files must not exceed 25 MiB; publish large artifacts outside Git:" >&2
  printf '%s\n' "$oversized" >&2
  exit 1
fi

echo "OK tracked-blob-sizes"
