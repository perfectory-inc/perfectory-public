#!/usr/bin/env bash
# The served cadastral edition is the last promotion of the source contract's ledger, and every
# promotion since root ADR-0291 carries the readiness it passed (ADR-0291 §4, ADR-0379).
#
# What failing this prevents: until 2026-10-21 served_edition was a literal moved by hand. 202606
# stayed served four months while two newer editions sat collected, and nothing said whether a newer
# one was all there. Now the value moves only through parcel_edition_readiness.py promote, which
# refuses an edition that is not ready; a hand edit is refused here and in every reader that loads
# the contract (vworld_parcel_editions.validate).
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
jobs="$root/platforms/foundation-platform/infra/lakehouse/spark/jobs"
contract="${2:-$root/platforms/foundation-platform/infra/lakehouse/contracts/vworld-parcel-source-objects.json}"

py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }

if ! out="$(PYTHONIOENCODING=utf-8 py "$jobs/vworld_parcel_editions.py" check --contract "$contract" 2>&1)"; then
  echo "FAIL parcel-edition-promotion-is-readiness-gated: $out" >&2
  exit 1
fi
echo "OK parcel-edition-promotion-is-readiness-gated ($out)"
