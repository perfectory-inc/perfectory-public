#!/usr/bin/env bash
# Proves the tracked-size guard rejects an index whose referenced blob vanished.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd)/scripts/guard/check-tracked-blob-sizes.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *) echo "tracked-blob-sizes-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

repo="$test_root/repo"
git init -q --initial-branch=main "$repo"
printf 'synthetic\n' >"$repo/tracked.txt"
git -C "$repo" add tracked.txt
blob="$(git -C "$repo" rev-parse :tracked.txt)"

(
  cd "$repo"
  "$checker" >/dev/null
)

object_path="$repo/.git/objects/${blob:0:2}/${blob:2}"
case "$object_path" in
  "$repo"/.git/objects/*) ;;
  *)
    echo "FAIL tracked-blob-sizes-self-test: unsafe synthetic object path" >&2
    exit 1
    ;;
esac
rm -f -- "$object_path"
if (
  cd "$repo"
  "$checker" >"$test_root/missing.out" 2>&1
); then
  echo "FAIL tracked-blob-sizes-self-test: accepted a missing staged blob" >&2
  exit 1
fi
if ! grep -q 'missing or unreadable' "$test_root/missing.out"; then
  echo "FAIL tracked-blob-sizes-self-test: missing-blob failure was ambiguous" >&2
  exit 1
fi

large_repo="$test_root/large-repo"
git init -q --initial-branch=main "$large_repo"
dd if=/dev/zero of="$large_repo/oversized.bin" bs=1048576 count=26 status=none
git -C "$large_repo" add oversized.bin
if (
  cd "$large_repo"
  "$checker" >"$test_root/oversized.out" 2>&1
); then
  echo "FAIL tracked-blob-sizes-self-test: accepted a tracked file over 25 MiB" >&2
  exit 1
fi
if ! grep -q 'must not exceed 25 MiB' "$test_root/oversized.out"; then
  echo "FAIL tracked-blob-sizes-self-test: oversized-blob failure was ambiguous" >&2
  exit 1
fi

echo "OK tracked-blob-sizes-self-test"
