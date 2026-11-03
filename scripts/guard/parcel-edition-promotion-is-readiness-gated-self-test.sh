#!/usr/bin/env bash
# Proves the promotion gate refuses what it claims to (root ADR-0291 §4, ADR-0379): on a copy of the
# real source contract it plants a candidate edition missing one readiness stage at a time and
# confirms `promote` refuses it and leaves the contract as it was; that a ready one is promoted and
# the guard accepts the result; and that a served_edition moved by hand fails the guard.
set -euo pipefail
cd "$(dirname "$0")/../.."

guard="$(pwd -P)/scripts/guard/parcel-edition-promotion-is-readiness-gated.sh"
jobs="$(pwd -P)/platforms/foundation-platform/infra/lakehouse/spark/jobs"
real="platforms/foundation-platform/infra/lakehouse/contracts/vworld-parcel-source-objects.json"
test_root="$(mktemp -d)"
if command -v cygpath >/dev/null 2>&1; then test_root="$(cygpath -m "$test_root")"; jobs="$(cygpath -m "$jobs")"; fi
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL parcel-edition-promotion-is-readiness-gated-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT
fail() { echo "FAIL parcel-edition-promotion-is-readiness-gated-self-test: $*" >&2; exit 1; }
py() { if command -v python3 >/dev/null 2>&1; then PYTHONIOENCODING=utf-8 python3 "$@"; else PYTHONIOENCODING=utf-8 python "$@"; fi; }

bash "$guard" >/dev/null || fail "the real contract does not pass"

# The candidate is the first held edition that is not served, picked by what it is, never by name.
# Evidence for it: every stage passes except the one named.
plant() {
  local missing="$1" dir="$test_root/$1"
  mkdir -p "$dir"
  cp "$real" "$dir/contract.json"
  py - "$dir" "$missing" <<'PY'
import json, sys
from pathlib import Path
dir, missing = Path(sys.argv[1]), sys.argv[2]
contract = json.loads((dir / "contract.json").read_text(encoding="utf-8"))
served = contract["served_edition"]
candidate = next(name for name in sorted(contract["editions"]) if name != served)
for entry in contract["editions"].values():
    entry["measured_at_utc"] = entry.get("measured_at_utc") or "2099-01-01"
evidence = {
    "provider_reuploads": {name: [] for name in contract["editions"]},
    "silver": {"rows_by_edition": {served: 1000, candidate: 1000}},
    "rehearsals": {candidate: {"gold": {"passed": True}, "bake": {"passed": True}}},
}
if missing == "source":
    del contract["editions"][candidate]["measured_at_utc"]
elif missing == "silver":
    evidence["silver"]["rows_by_edition"][candidate] = 0
elif missing == "rehearsal":
    evidence["rehearsals"][candidate]["bake"] = {"passed": False, "detail": "equivalence differs"}
(dir / "contract.json").write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
(dir / "evidence.json").write_text(json.dumps(evidence), encoding="utf-8")
(dir / "candidate").write_text(candidate, encoding="utf-8")
PY
  py "$jobs/parcel_edition_readiness.py" readiness --contract "$dir/contract.json" --evidence "$dir/evidence.json" \
    --output "$dir/report.json" || fail "readiness failed for $missing"
}
promote() {
  local dir="$test_root/$1"
  py "$jobs/parcel_edition_readiness.py" promote --contract "$dir/contract.json" --report "$dir/report.json" \
    --edition "$(cat "$dir/candidate")" --by self-test --reason "synthetic promotion"
}

for missing in source silver rehearsal; do
  plant "$missing"
  before="$(cat "$test_root/$missing/contract.json")"
  if promote "$missing" >/dev/null 2>"$test_root/$missing/err"; then fail "promoted an edition missing $missing"; fi
  grep -q "stage $missing failed" "$test_root/$missing/err" || fail "the refusal of $missing names another stage: $(cat "$test_root/$missing/err")"
  [ "$before" = "$(cat "$test_root/$missing/contract.json")" ] || fail "a refused promotion changed the contract"
done

plant none
promote none >/dev/null || fail "refused a ready edition"
bash "$guard" "$(pwd -P)" "$test_root/none/contract.json" >/dev/null || fail "the guard refused a gated promotion"

# A served edition moved by hand, without its promotion, fails the guard.
plant hand
py - "$test_root/hand" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1]) / "contract.json"
contract = json.loads(path.read_text(encoding="utf-8"))
contract["served_edition"] = (Path(sys.argv[1]) / "candidate").read_text(encoding="utf-8")
path.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
if bash "$guard" "$(pwd -P)" "$test_root/hand/contract.json" >/dev/null 2>&1; then fail "accepted a served edition moved by hand"; fi

echo "OK parcel-edition-promotion-is-readiness-gated-self-test"
