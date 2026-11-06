#!/usr/bin/env bash
# Proves declared-definitions-reproduce-artifacts.sh holds what it claims (root ADR-0418): the real
# definitions pass; a generated artifact committed to git, a hand edit of one in the tree, an edge
# that writes its own id (it is `<from>-to-<to>`), an id two definitions both hold, an order entry
# no definition holds, and an asset check on another group's asset are refused.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/declared-definitions-reproduce-artifacts.sh"
definitions="platforms/foundation-platform/definitions"
inputs=(
  "scripts/catalog/declared_definitions.py"
  "scripts/catalog/definition.v1.schema.json"
  "scripts/catalog/render-pipeline-map.py"
  "scripts/catalog/pipeline_graph.py"
  "platforms/foundation-platform/orchestration/dags/job_specs.py"
  "platforms/foundation-platform/infra/datahub/dataset_names.py"
  "platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json"
)
test_root="$(mktemp -d)"
# Git Bash on Windows: one spelling of the path that bash and Python both read the same way.
if command -v cygpath >/dev/null 2>&1; then test_root="$(cygpath -m "$test_root")"; fi
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL declared-definitions-reproduce-artifacts-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

py() { if command -v python3 >/dev/null 2>&1 && python3 -c '' >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }

# A git repository holding the definitions and what generating reads, with one edit. The edited item
# is picked by what it is, never by name.
fixture() {
  local name="$1" edit="$2"
  local root="$test_root/$name" path
  for path in "${inputs[@]}"; do
    mkdir -p "$root/$(dirname "$path")"
    cp "$path" "$root/$path"
  done
  mkdir -p "$root/$definitions" "$root/platforms/foundation-platform/contracts/data"
  cp "$definitions"/*.json "$root/$definitions/"
  # job_specs.py reads the data contracts for a job that checks them (reads_data_contracts).
  cp platforms/foundation-platform/contracts/data/*.odcs.yaml "$root/platforms/foundation-platform/contracts/data/"
  git -C "$root" init -q
  git -C "$root" -c user.name=t -c user.email=t@example.invalid add -A
  git -C "$root" -c user.name=t -c user.email=t@example.invalid commit -qm fixture
  py scripts/catalog/declared_definitions.py generate --root "$root"
  py - "$root" "$edit" <<'PY'
import json, pathlib, subprocess, sys
root, edit = pathlib.Path(sys.argv[1]), sys.argv[2]
definitions = root / "platforms/foundation-platform/definitions"
groups = sorted(p for p in definitions.glob("*.definition.json") if p.name != "platform.definition.json")
def rewrite(path, change):
    doc = json.loads(path.read_text(encoding="utf-8"))
    change(doc)
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
with_edges = next(p for p in groups if json.loads(p.read_text(encoding="utf-8")).get("dependencies"))
with_jobs = [p for p in groups if json.loads(p.read_text(encoding="utf-8")).get("jobs")]
if edit == "edge-id":
    rewrite(with_edges, lambda doc: doc["dependencies"][0].update(id="written-by-hand"))
elif edit == "twice":
    job = json.loads(with_jobs[0].read_text(encoding="utf-8"))["jobs"][0]
    rewrite(with_jobs[1], lambda doc: doc["jobs"].append(job))
elif edit == "stale-order":
    rewrite(definitions / "platform.definition.json", lambda doc: doc["order"]["jobs"].append("no_such_job"))
elif edit == "foreign-check":
    with_assets = [p for p in groups if json.loads(p.read_text(encoding="utf-8")).get("assets")]
    node = json.loads(with_assets[0].read_text(encoding="utf-8"))["assets"][0]["id"]
    rewrite(with_assets[1], lambda doc: doc.setdefault("asset_checks", []).append(
        {"asset": node, "check": "rows_valid_per_sido", "blocking": False}))
elif edit == "hand-edit":
    path = root / "platforms/foundation-platform/orchestration/jobs.v1.json"
    path.write_text(path.read_text(encoding="utf-8").replace('"enabled": true', '"enabled": false', 1), encoding="utf-8")
elif edit == "tracked":
    subprocess.run(["git", "-C", str(root), "add", "-f", "platforms/foundation-platform/orchestration/jobs.v1.json"], check=True)
PY
  printf '%s' "$root"
}

fail() { echo "FAIL declared-definitions-reproduce-artifacts-self-test: $1" >&2; exit 1; }

if ! bash "$checker" "$(fixture real none)" >/dev/null 2>&1; then
  fail "the real definitions are refused"
fi
for edit in tracked hand-edit edge-id twice stale-order foreign-check; do
  if bash "$checker" "$(fixture "$edit" "$edit")" >/dev/null 2>&1; then
    fail "a fixture with edit '$edit' passed"
  fi
done
echo "declared-definitions-reproduce-artifacts-self-test: ok"
