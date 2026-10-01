#!/usr/bin/env bash
# Every pinned container image matches the one list of image versions (root ADR-0118 §5).
#
# What real incident does failing this prevent? A version lived in each file that used it:
# postgres:17-alpine was pinned in 13 places, rust in 12, postgis in 11. Moving one meant finding
# all of them by hand, and a missed copy leaves two hosts on different builds of the "same"
# image with nothing saying so. tools/technology-versions.contract.json `container_images` is
# now the list; this guard refuses a reference that differs from it or is missing from it, and an
# entry nobody uses. `python3 scripts/catalog/sync-container-images.py --write` rewrites the
# references after a digest change.
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
exec python3 "$(cd "$(dirname "$0")/../.." && pwd -P)/scripts/catalog/sync-container-images.py" --check --root "$root"
