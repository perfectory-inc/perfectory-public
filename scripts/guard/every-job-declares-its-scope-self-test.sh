#!/usr/bin/env bash
# Proves every-job-declares-its-scope.sh refuses what it claims to refuse (root ADR-0314): a job
# without a scope, an unknown scope, a full job without its reason, cost or tracking, a full job the
# baseline does not list, and full fields on a partial job; and that the baseline only shrinks.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/every-job-declares-its-scope.sh"
python_checker="$(pwd -P)/scripts/guard/every-job-declares-its-scope.py"
jobs="platforms/foundation-platform/orchestration/jobs.v1.json"
baseline="tools/ci/full-scope-jobs.baseline.json"
test_root="$(mktemp -d)"
# Git Bash on Windows: one spelling of the path that bash and Python both read the same way.
if command -v cygpath >/dev/null 2>&1; then test_root="$(cygpath -m "$test_root")"; fi
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL every-job-declares-its-scope-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }

# A copy of the real job list and baseline with one edit. The edited jobs are picked by what they
# are (the first partial job, the first full one), never by name.
fixture() {
  local name="$1" edit="$2"
  local root="$test_root/$name"
  mkdir -p "$root/$(dirname "$jobs")" "$root/$(dirname "$baseline")"
  cp "$baseline" "$root/$baseline"
  py - "$jobs" "$root/$jobs" "$root/$baseline" "$edit" <<'PY'
import json, sys
source, target, baseline, edit = sys.argv[1:]
doc = json.loads(open(source, encoding="utf-8").read())
partial = next(job for job in doc["jobs"] if job["scope"] != "full")
full = next(job for job in doc["jobs"] if job["scope"] == "full")
if edit == "no-scope":
    partial.pop("scope")
elif edit == "unknown-scope":
    partial["scope"] = "incremental-ish"
elif edit in ("full_reason", "full_cost", "full_tracked_by"):
    full.pop(edit)
elif edit == "blank-reason":
    full["full_reason"] = "  "
elif edit == "new-full":
    partial.update(scope="full", full_reason="r", full_cost="c", full_tracked_by="t")
elif edit == "partial-with-reason":
    partial["full_reason"] = "left behind"
elif edit == "fixed":
    for field in ("full_reason", "full_cost", "full_tracked_by"):
        full.pop(field)
    full["scope"] = "changed-keys"
open(target, "w", encoding="utf-8").write(json.dumps(doc, ensure_ascii=False))
PY
  printf '%s' "$root"
}

expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL every-job-declares-its-scope-self-test: accepted forbidden fixture ($2)" >&2
    exit 1
  fi
}

bash "$checker" "$(pwd -P)" >/dev/null || {
  echo "FAIL every-job-declares-its-scope-self-test: rejected the real job list" >&2
  exit 1
}

# The unedited copy passes, so every refusal below is the edit's, not a broken fixture's.
bash "$checker" "$(fixture copy none)" >/dev/null || {
  echo "FAIL every-job-declares-its-scope-self-test: rejected an unedited copy" >&2
  exit 1
}

expect_rejected "$(fixture no-scope no-scope)" "scope 가 없는 작업"
expect_rejected "$(fixture unknown unknown-scope)" "모르는 scope"
expect_rejected "$(fixture reason full_reason)" "full 인데 이유가 없다"
expect_rejected "$(fixture cost full_cost)" "full 인데 잰 비용이 없다"
expect_rejected "$(fixture tracked full_tracked_by)" "full 인데 고칠 곳이 없다"
expect_rejected "$(fixture blank blank-reason)" "full 이유가 빈칸"
# 기준선에 없는 새 full 작업: 목록은 줄기만 한다.
expect_rejected "$(fixture new new-full)" "기준선에 없는 새 full"
expect_rejected "$(fixture partial partial-with-reason)" "부분 작업에 남은 full_reason"

# A job that became partial passes (with a warning), and --shrink drops it from the baseline but
# never adds the jobs that are still full and unlisted.
fixed="$(fixture fixed fixed)"
bash "$checker" "$fixed" >/dev/null 2>&1 || {
  echo "FAIL every-job-declares-its-scope-self-test: rejected a job that became partial" >&2
  exit 1
}
before="$(py -c 'import json,sys; print(len(json.load(open(sys.argv[1],encoding="utf-8"))["jobs"]))' "$fixed/$baseline")"
py "$python_checker" "$fixed" --shrink
after="$(py -c 'import json,sys; print(len(json.load(open(sys.argv[1],encoding="utf-8"))["jobs"]))' "$fixed/$baseline")"
[ "$after" -eq "$((before - 1))" ] || {
  echo "FAIL every-job-declares-its-scope-self-test: --shrink kept $after of $before, not one fewer" >&2
  exit 1
}
new="$(fixture shrink-new new-full)"
py "$python_checker" "$new" --shrink
expect_rejected "$new" "--shrink 가 새 full 을 기준선에 더했다"

echo "OK every-job-declares-its-scope-self-test"
