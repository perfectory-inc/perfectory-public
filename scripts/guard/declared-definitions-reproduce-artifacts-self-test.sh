#!/usr/bin/env bash
# Proves declared-definitions-reproduce-artifacts.sh holds what it claims (root ADR-0415 §5): the
# real tree passes; a pipeline-graph edge not named `<from>-to-<to>` (its id is generated) and an id
# listed twice (one definition owns one id) are refused; a hand edit of a file the definitions
# generate (job-dependencies.v1.json, docs/data-pipeline-map.md) is refused.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/declared-definitions-reproduce-artifacts.sh"
jobs="platforms/foundation-platform/orchestration/jobs.v1.json"
graph="platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json"
dependencies="platforms/foundation-platform/orchestration/job-dependencies.v1.json"
map="docs/data-pipeline-map.md"
contracts="platforms/foundation-platform/contracts/data"
inputs=(
  "$jobs" "$graph" "$dependencies" "$map"
  "platforms/foundation-platform/config/r2-connections.contract.json"
  "tools/host-memory-budget.contract.json"
  "platforms/foundation-platform/contracts/schema-registry/registry.json"
  "platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json"
  "scripts/catalog/declared_definitions.py"
  "platforms/foundation-platform/orchestration/dags/job_specs.py"
  "platforms/foundation-platform/infra/datahub/dataset_names.py"
  "scripts/catalog/render-pipeline-map.py"
  "scripts/catalog/pipeline_graph.py"
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

py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }

# A copy of the inputs with one edit; the edited item is picked by what it is, never by name.
fixture() {
  local name="$1" edit="$2"
  local root="$test_root/$name" path
  for path in "${inputs[@]}"; do
    mkdir -p "$root/$(dirname "$path")"
    cp "$path" "$root/$path"
  done
  # job_specs.py reads the data contracts for a job that checks them (reads_data_contracts).
  mkdir -p "$root/$contracts"
  cp "$contracts"/*.odcs.yaml "$root/$contracts/"
  py - "$root" "$edit" <<'PY'
import json, pathlib, sys
root, edit = pathlib.Path(sys.argv[1]), sys.argv[2]
jobs = root / "platforms/foundation-platform/orchestration/jobs.v1.json"
graph = root / "platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json"
def rewrite(path, change):
    doc = json.loads(path.read_text(encoding="utf-8"))
    change(doc)
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
if edit == "edge-id":
    rewrite(graph, lambda doc: doc["edges"][0].update(id=doc["edges"][0]["id"] + "-renamed"))
elif edit == "twice":
    rewrite(jobs, lambda doc: doc["jobs"].append(dict(doc["jobs"][0])))
elif edit == "dependencies":
    path = root / "platforms/foundation-platform/orchestration/job-dependencies.v1.json"
    path.write_text(path.read_text(encoding="utf-8").replace('"producers": []', '"producers": ["x"]', 1), encoding="utf-8")
elif edit == "map":
    path = root / "docs/data-pipeline-map.md"
    path.write_text(path.read_text(encoding="utf-8") + "hand edit\n", encoding="utf-8")
PY
  printf '%s' "$root"
}

fail() { echo "FAIL declared-definitions-reproduce-artifacts-self-test: $1" >&2; exit 1; }

if ! bash "$checker" "$(fixture real none)" >/dev/null 2>&1; then
  fail "the real tree is refused"
fi
for edit in edge-id twice dependencies map; do
  if bash "$checker" "$(fixture "$edit" "$edit")" >/dev/null 2>&1; then
    fail "a fixture with edit '$edit' passed"
  fi
done
echo "declared-definitions-reproduce-artifacts-self-test: ok"
