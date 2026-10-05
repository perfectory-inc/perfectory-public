#!/usr/bin/env bash
# Proves region-code-pairs-have-one-home.sh refuses what it claims to refuse, and accepts prose and
# the paths its definition allows (root ADR-0145 §4).
#
# Every holder name below is read from the real definition file, never written out here: this file
# is code the guard scans, and a guard that trips on its own test is one nobody can describe.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/region-code-pairs-have-one-home.sh"
definition="platforms/foundation-platform/infra/lakehouse/contracts/region-code-holders.json"
contracts="platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL region-code-pairs-have-one-home-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }
field() { py -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); print(eval(sys.argv[2]))' "$definition" "$1"; }
# A holder is picked by what it is, never by its place in the list: reordering the definition must
# not turn a case into a different one. The first retired holder some path may name, and the first
# none may.
retired_crosswalk="$(field 'next(h["name"] for h in d["retired"] if not h["allowed_paths"] and "crosswalk" in h["name"])')"
retired_transition="$(field 'next(h["name"] for h in d["retired"] if h["allowed_paths"])')"
allowed_transition="$(field 'next(h["allowed_paths"][0] for h in d["retired"] if h["allowed_paths"])')"
retired_path="$(field 'next(p for p in d["retired_paths"] if p.endswith(".py"))')"
home="$(field 'd["homes"]["code_pairs"]')"
derived="$(field 'next(iter(d["derived_tables"]))')"
source="$(field 'next(n for n in d["source_tables"] if not n.startswith("$"))')"

# A synthetic repository: the real definition, and a contracts file with the home, a derived table
# and one ordinary table.
make_repo() {
  local root="$1"
  mkdir -p "$root/$(dirname "$definition")" "$root/platforms/foundation-platform/infra/lakehouse/spark/jobs" "$root/docs/adr"
  cp "$definition" "$root/$definition"
  py - "$root/$contracts" "$home" "$derived" "$source" "${2:-}" <<'PY'
import json, sys
path, home, derived, source, extra = sys.argv[1:6]
col = lambda *names: [{"name": n} for n in names]
contracts = {home: {"columns": col("old_code", "new_code", "source")},
             derived: {"columns": col("place_id", "from_code", "to_code")},
             source: {"columns": col("old_pnu", "new_pnu", "source_record_id"),
                      "load": {"unit": "object", "column": "source_record_id"}},
             "silver.ordinary": {"columns": col("pnu", "area")}}
if extra == "derived-source":
    contracts[source]["load"] = {"unit": "object", "column": "derivation_run_id"}
elif extra:
    contracts["silver.second_crosswalk"] = {"columns": col("old_code", "new_code")}
json.dump({"contracts": contracts}, open(path, "w", encoding="utf-8"))
PY
  printf '%s\n' 'def run(): pass' >"$root/platforms/foundation-platform/infra/lakehouse/spark/jobs/job.py"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL region-code-pairs-have-one-home-self-test: rejected allowed fixture ($2)" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL region-code-pairs-have-one-home-self-test: accepted forbidden fixture ($2)" >&2
    exit 1
  fi
}

# 1. The home, a derived table and a source table the definition names, prose about a retired holder, and the paths
#    the definition lets name one.
clean="$test_root/clean"
make_repo "$clean"
printf 'The %s table was retired.\n' "$retired_crosswalk" >"$clean/docs/adr/0145.md"
mkdir -p "$clean/$(dirname "$allowed_transition")"
printf 'CREATE TABLE %s (id uuid);\n' "$retired_transition" >"$clean/$allowed_transition"
expect_allowed "$clean" "정상: 정본·파생 표·문서·허용 경로"

# 2. The incident's shape: a job writing the retired stored crosswalk again.
writer="$test_root/writer"
make_repo "$writer"
printf 'spark.sql("INSERT INTO lakehouse.%s SELECT * FROM pairs")\n' "$retired_crosswalk" \
  >"$writer/platforms/foundation-platform/infra/lakehouse/spark/jobs/job.py"
expect_rejected "$writer" "폐기된 대응표에 다시 쓰는 작업"

# 3. A second table that stores code pairs outside the home.
second="$test_root/second"
make_repo "$second" extra
expect_rejected "$second" "정본 밖에 짝을 저장하는 표"

# 4. A retired holder named outside the paths allowed for it.
elsewhere="$test_root/elsewhere"
make_repo "$elsewhere"
printf 'INSERT INTO %s VALUES (1);\n' "$retired_transition" \
  >"$elsewhere/platforms/foundation-platform/infra/lakehouse/spark/jobs/write.sql"
expect_rejected "$elsewhere" "허용 경로 밖에서 폐기된 전이표를 부름"

# 5. A retired file brought back.
revived="$test_root/revived"
make_repo "$revived"
mkdir -p "$revived/$(dirname "$retired_path")"
printf '%s\n' 'print(1)' >"$revived/$retired_path"
expect_rejected "$revived" "폐기된 파일이 돌아옴"

# 6. A machine-read file under a docs directory naming a retired holder: JSON is code wherever it
#    lives, unlike the Markdown beside it (case 1).
graph="$test_root/graph"
make_repo "$graph"
mkdir -p "$graph/platforms/foundation-platform/docs/catalog"
printf '{"nodes": [{"table_name": "%s"}]}\n' "$retired_crosswalk" \
  >"$graph/platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json"
expect_rejected "$graph" "문서 폴더의 기계 판독 파일이 폐기된 대응표를 부름"

# 7. A named source table loaded per derivation run: that is a pair store under a source's name.
derived_source="$test_root/derived-source"
make_repo "$derived_source" derived-source
expect_rejected "$derived_source" "원천 표라면서 도출 실행 단위로 적재"

echo "OK region-code-pairs-have-one-home-self-test"
