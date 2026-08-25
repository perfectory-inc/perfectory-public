#!/usr/bin/env bash
# Prevents: a live-backend test that belongs to no lane, and therefore runs
# nowhere while CI stays green.
#
# `cargo xtask integration <area>` used to run `--workspace -- --ignored`, which
# cannot tell a Kafka test from a Postgres one: the Postgres job ran both, the
# Kafka half found no broker, took its "resource absent" branch, and was counted
# as a pass. The fix is per-resource lanes in tools/xtask that name their targets
# instead of sweeping — but naming introduces the opposite risk. An enumerated
# lane silently stops covering a target the moment someone adds one and forgets
# the table, which is the same defect wearing the other face.
#
# So the enumeration must be provably complete: every test target carrying
# `#[ignore]` has to appear in exactly one lane. Package names are unique across
# the monorepo (scripts/guard/unique-package-names.sh), so `(package, target)` is
# a stable key.
set -euo pipefail
cd "$(dirname "$0")/../.."

xtask_src="${1:-tools/xtask/src/main.rs}"
# Every area, not just platforms/: gongzzang lives under products/ and owns the
# largest feature-gated live suite in the repository. Scanning one tree and
# reporting "complete" would be exactly the false confidence this guard exists
# to remove.
if [ "$#" -ge 2 ]; then
  scan_roots=("${@:2}")
else
  # `tools` joined the default in ADR-0011. It holds the fifth Cargo workspace —
  # xtask, the harness that runs every other area's tests — and scanning only
  # platforms/products is precisely why nothing noticed that no area declared it.
  scan_roots=(platforms products tools scripts)
fi

if [ ! -f "$xtask_src" ]; then
  echo "FAIL live-lane-completeness: missing $xtask_src" >&2
  exit 1
fi

for root in "${scan_roots[@]}"; do
  if [ ! -d "$root" ]; then
    echo "FAIL live-lane-completeness: scan root '$root' is not a directory" >&2
    exit 1
  fi
done

# Where workflows live. The self-test drives this guard against fixtures outside
# the repository, so a hard-coded `.github/workflows` would make the workflow-facing
# checks unverifiable. Defined here because both the Python and the TypeScript
# sections consult it.
workflows_dir="${LIVE_LANE_WORKFLOWS_DIR:-.github/workflows}"

# Declared: LaneTarget { package: "P", test: "T" } — the two fields are adjacent,
# so pair them by reading the package line and attaching the next test line.
declared="$(
  grep -oE '(package|test): "[A-Za-z0-9_.-]+"' "$xtask_src" \
    | sed 's/.*: "//; s/"$//' \
    | paste - - 2>/dev/null \
    | sort -u || true
)"

# Actual: every integration test target that gates itself on a backend. Two
# gating styles are in use and both must count — foundation and identity mark
# such tests `#[ignore]`, while gongzzang compiles them out with
# `#![cfg(feature = "integration")]`. Recognising only the first would let a
# whole area's live suite sit outside every lane and still report completeness.
actual=""
while IFS= read -r file; do
  [ -n "$file" ] || continue
  grep -qE '#\[ignore|#!\[cfg\(feature' "$file" || continue
  crate_dir="${file%%/tests/*}"
  manifest="$crate_dir/Cargo.toml"
  [ -f "$manifest" ] || continue
  package="$(grep -m1 '^name' "$manifest" | sed 's/.*"\(.*\)".*/\1/')"
  [ -n "$package" ] || continue
  target="$(basename "$file" .rs)"
  # `tests/common.rs` is a shared helper module pulled in by `mod common;`, not a
  # test target cargo can address with `--test`. It inherits the suite's feature
  # gate, so it looks backend-gated without being runnable on its own.
  [ "$target" = "common" ] && continue
  actual="$actual$package	$target
"
done < <(
  find "${scan_roots[@]}" \
    -type d \( -name target -o -name node_modules \) -prune -o \
    -type f -name '*.rs' -print 2>/dev/null \
    | grep -E '/tests/[^/]+\.rs$' \
    | sort
)

missing="$(comm -23 <(printf '%s' "$actual" | sort -u | sed '/^$/d') <(printf '%s\n' "$declared" | sed '/^$/d') || true)"

if [ -n "$missing" ]; then
  echo "FAIL live-lane-completeness: these backend-gated test targets belong to no lane in $xtask_src," >&2
  echo "    so no harness runs them and nothing reports their absence:" >&2
  printf '%s\n' "$missing" | sed 's/^/      /' >&2
  echo "    Add each to the owning area's live_lanes, or remove the test." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Membership is not correctness.
#
# Belonging to a lane only guarantees SOME command names the target. It does not
# guarantee the command SELECTS it. The two gating styles need opposite cargo
# flags — `#[ignore]` needs `-- --ignored`, `#![cfg(feature = "…")]` needs
# `--features …` — and a lane that declares the wrong one runs zero tests while
# cargo exits 0. That is precisely what happened to gongzzang's twenty targets:
# fully declared, provably complete, and selected by nothing.
#
# xtask catches this at run time by reading the executed-test count back, which
# is the stronger check because it observes reality. But it can only fire for a
# lane that actually runs, and five lanes (foundation r2/lakehouse/data-go-kr,
# intelligence kafka/redis) run in no CI job at all. For those, a wrong
# declaration would sit undetected until someone finally provisioned the backend.
#
# Prior art for closing this statically: rust-lang/rust PR #108905 made unknown
# compiletest directives (`//@ ignore-<typo>`) a hard error instead of a silent
# no-op, and uncovered 79 tests whose declared gating had never applied.
# ---------------------------------------------------------------------------

# Every discoverable test target with the gating its SOURCE actually uses.
# Both forms are matched as attributes anchored at the start of a line, so a
# `#[ignore]` written inside a doc comment cannot be mistaken for a real gate.
actual_gating="$(
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    crate_dir="${file%%/tests/*}"
    manifest="$crate_dir/Cargo.toml"
    [ -f "$manifest" ] || continue
    package="$(grep -m1 '^name' "$manifest" | sed 's/.*"\(.*\)".*/\1/')"
    [ -n "$package" ] || continue
    target="$(basename "$file" .rs)"
    [ "$target" = "common" ] && continue
    feature="$(
      grep -m1 -oE '^#!\[cfg\(feature = "[^"]+"\)\]' "$file" 2>/dev/null \
        | sed 's/.*"\(.*\)".*/\1/' || true
    )"
    if [ -n "$feature" ]; then
      printf '%s\t%s\tFeature("%s")\n' "$package" "$target" "$feature"
    elif grep -qE '^[[:space:]]*#\[ignore' "$file" 2>/dev/null; then
      printf '%s\t%s\tIgnored\n' "$package" "$target"
    else
      printf '%s\t%s\tNone\n' "$package" "$target"
    fi
  done < <(
    find "${scan_roots[@]}" \
      -type d \( -name target -o -name node_modules \) -prune -o \
      -type f -name '*.rs' -print 2>/dev/null \
      | grep -E '/tests/[^/]+\.rs$' \
      | sort
  )
)"

# What the lane table CLAIMS. `gating:` precedes `targets:` in each LiveLane, so
# a one-state pass attaches the lane's gating to each of its targets. The
# `#[cfg(test)] mod tests` block is cut first: it constructs bare LaneTargets
# that would otherwise inherit the last real lane's gating.
#
# `|| true`: a table with no LiveLane at all is a legitimate state — an area may
# own Python suites and no live Rust lane — but `grep` returns 1 on zero matches
# and `set -euo pipefail` turns that into a silent abort with no message and no
# verdict. Every area in this repository happens to declare a lane today, which is
# why the fragility stayed hidden until an ADR-0011 fixture exercised the empty
# case. A guard that dies quietly is worse than one that fails loudly.
declared_gating="$(
  sed '/^#\[cfg(test)\]/,$d' "$xtask_src" \
    | grep -oE 'gating: LaneGating::(Ignored|Feature\("[^"]+"\))|package: "[^"]+"|test: "[^"]+"' \
    | awk '
        /^gating:/  { g = $0; sub(/^gating: LaneGating::/, "", g); next }
        /^package:/ { p = $0; sub(/^package: "/, "", p); sub(/"$/, "", p); next }
        /^test:/    { t = $0; sub(/^test: "/, "", t); sub(/"$/, "", t)
                      if (g != "" && p != "") print p "\t" t "\t" g }' || true
)"

gating_report=""
while IFS="$(printf '\t')" read -r package target claimed; do
  [ -n "$package" ] || continue
  observed="$(
    printf '%s\n' "$actual_gating" \
      | awk -F"$(printf '\t')" -v p="$package" -v t="$target" '$1 == p && $2 == t { print $3 }'
  )"
  if [ -z "$observed" ]; then
    gating_report="${gating_report}${package} --test ${target}: declared in a lane, but no such test target exists
"
  elif [ "$observed" != "$claimed" ]; then
    gating_report="${gating_report}${package} --test ${target}: lane declares ${claimed}, source uses ${observed}
"
  fi
done <<EOF
$declared_gating
EOF

if [ -n "$gating_report" ]; then
  echo "FAIL live-lane-completeness: a lane's declared gating does not match its target's source," >&2
  echo "    so the lane's cargo flags select nothing and the run still exits 0:" >&2
  printf '%s' "$gating_report" | sed 's/^/      /' >&2
  echo "    Ignored needs '#[ignore]'; Feature(\"f\") needs '#![cfg(feature = \"f\")]'." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Python 발견-선언 대조 (ADR-0011).
#
# 위의 Rust 검사가 성립하는 이유는 `LaneTarget { package, test }`가 *타깃*을 열거하기
# 때문이다. `PythonTests { dir, python_path, args }`는 *명령*만 서술하므로 무엇을
# 책임지는지 말하지 않고, 따라서 대조할 대상이 없다. 그 침묵 속에서 Python 테스트
# 파일 4개(26 테스트)가 어느 CI·훅에서도 실행되지 않은 채 남아 있었고, 그중 하나는
# 통과가 구조적으로 불가능한 단언을 갖고 있었다.
#
# `covers`가 그 주장을 명시적으로 만든다: 이 스위트가 책임지는 발견 루트(area 기준
# 상대 경로). 발견된 모든 Python 테스트 파일은 정확히 하나의 covers 루트 아래 있어야
# 한다. 0개는 누락이고, 2개 이상은 소유가 모호하다.
#
# 발견 규약은 `test_*.py` / `*_test.py`로 좁게 둔다. 규약 밖 이름에 예외를 주지
# 않는다 — 예외 목록은 조용히 넓어지고, 그러면 발견기가 다시 불완전해진다.
#
# 발견은 위의 Rust 검사와 같은 `find` 순회를 쓴다. `git ls-files -co --exclude-standard`도
# 이 저장소에서 같은 집합을 내지만(실측: find 1.76s / git 0.73s, 결과 동일), 이 가드는
# `$2..`로 받은 임의의 디렉터리를 훑도록 만들어져 있고 자기-테스트가 저장소 밖 임시
# 픽스처로 음성 사례를 고정한다. git 기반 발견은 그 픽스처를 볼 수 없으므로 검사 자체가
# 검사되지 않는다. 발견 방식이 파일 안에서 갈라지는 비용도 크다 — 한 가드에 두 가지
# 발견 규약이 있으면 다음 사람이 어느 쪽을 확장해야 하는지 알 수 없다.
# ---------------------------------------------------------------------------

# `|| true` 는 실수가 아니라 계약이다. `set -euo pipefail` 아래에서 grep 은 0건을 만나면
# 1을 반환하고, 명령 치환에 붙은 그 실패가 가드 전체를 중단시킨다. 선언이 하나도 없는
# 상태(마이그레이션 도중)와 파이썬 테스트가 하나도 없는 영역은 둘 다 정상이며, 그 정상
# 상태에서 가드가 죽으면 검사는 "통과"도 "실패"도 아닌 침묵이 된다. 위의 Rust 선언부가
# 같은 이유로 같은 처리를 한다.
declared_python="$(
  sed '/^#\[cfg(test)\]/,$d' "$xtask_src" \
    | grep -oE 'covers: &\[[^]]*\]' \
    | grep -oE '"[^"]+"' \
    | tr -d '"' \
    | sort -u || true
)"

# Discovery is by CONTENT, not by name. Naming was the first cut and an adversarial
# pass walked straight through it: a file holding a real `unittest.TestCase` under
# any other name is invisible to this guard AND to `unittest discover -p test_*.py`,
# so it runs nowhere and the guard reports completeness. Content-first inverts that
# — a file with tests in it is found, and its name is then checked rather than
# trusted.
actual_python="$(
  find "${scan_roots[@]}" \
    -type d \( -name target -o -name node_modules -o -name .venv -o -name __pycache__ \) -prune -o \
    -type f -name '*.py' -print 2>/dev/null \
    | sed 's|^\./||' \
    | sort \
    | while IFS= read -r candidate; do
        [ -n "$candidate" ] || continue
        if grep -qE 'class[[:space:]]+[A-Za-z_][A-Za-z0-9_]*\([^)]*TestCase|^[[:space:]]*def[[:space:]]+test_' \
          "$candidate" 2>/dev/null; then
          printf '%s\n' "$candidate"
        fi
      done || true
)"

python_report=""
while IFS= read -r file; do
  [ -n "$file" ] || continue

  # A test the runner's discovery pattern cannot match is unreachable no matter
  # who claims it, so the name is checked before ownership.
  case "$(basename "$file")" in
    test_*.py | *_test.py) ;;
    *)
      python_report="${python_report}${file}: contains tests but is named outside test_*.py / *_test.py, so no runner can discover it
"
      continue
      ;;
  esac

  matched=0
  while IFS= read -r root; do
    [ -n "$root" ] || continue
    case "$file" in
      *"/$root/"*) matched=$((matched + 1)) ;;
    esac
  done <<EOF
$declared_python
EOF
  if [ "$matched" -gt 1 ]; then
    python_report="${python_report}${file}: claimed by ${matched} covers roots; exactly one is required
"
    continue
  fi
  if [ "$matched" -eq 1 ]; then
    continue
  fi

  # Not owned by an area suite. A workflow may still name the file directly —
  # `scripts/catalog/test_audit_documentation.py` is run that way by docs.yml —
  # and that is a real runner, so it counts. Anything else runs nowhere.
  if grep -rqF "$file" "$workflows_dir" 2>/dev/null; then
    continue
  fi
  python_report="${python_report}${file}: claimed by no PythonTests covers root and named by no workflow
"
done <<EOF
$actual_python
EOF

if [ -n "$python_report" ]; then
  echo "FAIL live-lane-completeness: Python test files outside exactly one declared covers root," >&2
  echo "    so no harness runs them and nothing reports their absence:" >&2
  printf '%s' "$python_report" | sed 's/^/      /' >&2
  echo "    Add each to the owning area's python_tests covers list, or remove the test." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 워크스페이스 발견-선언 대조 (ADR-0011).
#
# 위의 두 검사는 area 안에서 무엇이 도는지를 증명한다. 이 검사는 그보다 한 층 위 —
# 애초에 어떤 워크스페이스가 area로 선언됐는지를 증명한다. 저장소에 Cargo 워크스페이스가
# 다섯이었는데 AREAS에는 넷뿐이었고, 빠진 하나가 하필 `tools/xtask`(하네스 자신)였다.
# `verify`는 area의 dir 안에서만 `cargo test`를 돌므로 xtask의 단위 테스트 10개는
# 어디서도 실행되지 않았다 — 그중에는 "0개 실행 = 실패"가 작동함을 증명하는 테스트가
# 있었다. 모든 스위트가 돈다는 보장의 근거가, 아무것도 보장하지 않는 유일한 스위트였다.
#
# Area 의 `dir:` 은 `slug:` 뒤에 온다. `PythonTests` 도 `dir:` 을 갖지만 그 앞에 `slug:` 이
# 없으므로 한 상태 통과로 구분된다 — 위의 gating 파서와 같은 기법이다.
# ---------------------------------------------------------------------------

declared_workspaces="$(
  sed '/^#\[cfg(test)\]/,$d' "$xtask_src" \
    | grep -oE 'slug: "[^"]+"|dir: "[^"]+"' \
    | awk '
        /^slug:/ { pending = 1; next }
        /^dir:/  { if (pending) {
                     d = $0; sub(/^dir: "/, "", d); sub(/"$/, "", d)
                     print d; pending = 0
                   } }' || true
)"

workspace_report=""
while IFS= read -r manifest; do
  [ -n "$manifest" ] || continue
  grep -qE '^\[workspace\]' "$manifest" || continue
  workspace_dir="$(dirname "$manifest")"
  matched=0
  while IFS= read -r declared_dir; do
    [ -n "$declared_dir" ] || continue
    case "$workspace_dir/" in
      *"/$declared_dir/") matched=1 ;;
      "$declared_dir/") matched=1 ;;
    esac
  done <<EOF
$declared_workspaces
EOF
  if [ "$matched" -eq 0 ]; then
    workspace_report="${workspace_report}${workspace_dir}: Cargo workspace declared by no Area
"
  fi
done <<EOF
$(
  find "${scan_roots[@]}" \
    -type d \( -name target -o -name node_modules \) -prune -o \
    -type f -name 'Cargo.toml' -print 2>/dev/null \
    | sort || true
)
EOF

if [ -n "$workspace_report" ]; then
  echo "FAIL live-lane-completeness: Cargo workspaces that belong to no Area in $xtask_src," >&2
  echo "    so 'cargo xtask verify' never enters them and their tests run nowhere:" >&2
  printf '%s' "$workspace_report" | sed 's/^/      /' >&2
  echo "    Add each to AREAS, or remove the workspace." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# TypeScript 발견-선언 대조 (ADR-0011).
#
# `pnpm turbo test` 는 `test` 스크립트를 가진 패키지를 쓸어담는 형태다. 그래서 테스트
# 모양이지만 이름이 다른 스크립트는 어떤 워크플로도 선택하지 않는다 — `probe:naver` 가
# 정확히 그랬다. Rust 는 타깃을, Python 은 파일을 발견 단위로 삼지만 TypeScript 는
# 스크립트가 실행 단위이므로 스크립트를 센다.
#
# 워크플로 디렉터리는 환경변수로 뺀다. 자기-테스트가 저장소 밖 픽스처로 음성 사례를
# 고정하는데, `.github/workflows` 를 고정 경로로 읽으면 그 픽스처를 볼 수 없어 검사
# 자체가 검사되지 않는다. 위치 인자는 그대로 두어 기존 호출자를 건드리지 않는다.
# ---------------------------------------------------------------------------

ts_declared="$(
  find "${scan_roots[@]}" \
    -type d \( -name node_modules -o -name target -o -name .turbo \) -prune -o \
    -type f -name 'package.json' -print 2>/dev/null \
    | sort \
    | while IFS= read -r manifest; do
        [ -n "$manifest" ] || continue
        python3 - "$manifest" <<'PY' || true
import json
import sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
except (OSError, ValueError):
    sys.exit(0)
for name in data.get("scripts", {}):
    if "test" in name or "probe" in name:
        print(f"{path}\t{name}")
PY
      done || true
)"
# Python on the Windows host writes CRLF, so a script name would carry a trailing
# `\r` into the grep pattern and never match. That failure appears only off CI —
# exactly the local/CI split ADR-0004 exists to prevent — so strip it at the source
# rather than leaving a difference that only one platform sees.
ts_declared="$(printf '%s' "$ts_declared" | tr -d '\r')"

# Scripts that are outside CI on purpose, each carrying the resource that keeps
# them out. `LiveLane.required_env` is the same sentence for Rust: "this needs a
# backend CI does not have" is a claim, not a silence.
ts_local_only="$(
  sed '/^#\[cfg(test)\]/,$d' "$xtask_src" \
    | grep -oE 'manifest: "[^"]+"|script: "[^"]+"' \
    | awk '
        /^manifest:/ { m = $0; sub(/^manifest: "/, "", m); sub(/"$/, "", m); next }
        /^script:/   { s = $0; sub(/^script: "/, "", s); sub(/"$/, "", s)
                       if (m != "") { print m "\t" s; m = "" } }' || true
)"

# Node suites are owned by the same `cargo xtask verify <area>` entrypoint as Rust/Python. Parse the
# declarative Area table instead of forcing workflows to repeat those package commands beside the
# verification SSOT. The parser understands balanced Rust blocks and ignores strings/comments, so
# adding a nested lane or a brace in prose cannot silently move a suite between areas.
ts_xtask_declared="$(python3 - "$xtask_src" <<'PY'
import re
import sys
from pathlib import PurePosixPath

text = open(sys.argv[1], encoding="utf-8").read().split("#[cfg(test)]", 1)[0]

def balanced(source: str, start: int, opener: str, closer: str) -> tuple[str, int]:
    depth = 0
    index = start
    quote = None
    while index < len(source):
        char = source[index]
        pair = source[index:index + 2]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if pair == "//":
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
            continue
        if pair == "/*":
            end = source.find("*/", index + 2)
            index = len(source) if end < 0 else end + 2
            continue
        if char == '"':
            quote = char
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return source[start:index + 1], index + 1
        index += 1
    raise ValueError(f"unbalanced {opener}{closer} in xtask Area table")

cursor = 0
while True:
    match = re.search(r"\bArea\s*\{", text[cursor:])
    if match is None:
        break
    start = cursor + match.start() + match.group(0).rfind("{")
    area, cursor = balanced(text, start, "{", "}")
    area_dir = re.search(r"\bdir:\s*\"([^\"]+)\"", area)
    node_marker = re.search(r"\bnode_tests:\s*&\[", area)
    if area_dir is None or node_marker is None:
        continue
    list_start = node_marker.end() - 1
    node_list, _ = balanced(area, list_start, "[", "]")
    node_cursor = 0
    while True:
        suite_match = re.search(r"\bNodeTests\s*\{", node_list[node_cursor:])
        if suite_match is None:
            break
        suite_start = node_cursor + suite_match.start() + suite_match.group(0).rfind("{")
        suite, node_cursor = balanced(node_list, suite_start, "{", "}")
        suite_dir = re.search(r"\bdir:\s*\"([^\"]+)\"", suite)
        scripts = re.search(r"\bscripts:\s*&\[(.*?)\]", suite, re.DOTALL)
        if suite_dir is None or scripts is None:
            continue
        manifest = PurePosixPath(area_dir.group(1), suite_dir.group(1), "package.json")
        for script in re.findall(r'"([^\"]+)"', scripts.group(1)):
            print(f"{manifest}\t{script}")
PY
)"

ts_report=""
while IFS="$(printf '\t')" read -r manifest script; do
  [ -n "$script" ] || continue
  # `turbo <script>` covers every package declaring that script name, so a
  # workspace-wide sweep counts as an invocation for each of them.
  if grep -rqE "run ${script}\b|turbo ${script}\b|turbo run ${script}\b" \
    "$workflows_dir" 2>/dev/null; then
    continue
  fi
  xtask_owned=0
  while IFS="$(printf '\t')" read -r declared_manifest declared_script; do
    [ -n "$declared_script" ] || continue
    [ "$declared_script" = "$script" ] || continue
    case "$manifest" in
      *"/$declared_manifest" | "$declared_manifest") xtask_owned=1 ;;
    esac
  done <<INNER
$ts_xtask_declared
INNER
  if [ "$xtask_owned" -eq 1 ]; then
    continue
  fi
  # Declarations are repository-relative; discovered manifests are whatever the
  # scan root produced, which is absolute when the self-test drives the guard
  # against a temporary fixture. Compare by suffix so one declaration form works
  # for both, exactly as the covers and workspace checks do.
  excused=0
  while IFS="$(printf '\t')" read -r declared_manifest declared_script; do
    [ -n "$declared_script" ] || continue
    [ "$declared_script" = "$script" ] || continue
    case "$manifest" in
      *"/$declared_manifest" | "$declared_manifest") excused=1 ;;
    esac
  done <<INNER
$ts_local_only
INNER
  if [ "$excused" -eq 1 ]; then
    continue
  fi
  ts_report="${ts_report}${manifest} script '${script}': no workflow invokes it and it is not declared local-only
"
done <<EOF
$ts_declared
EOF

if [ -n "$ts_report" ]; then
  echo "FAIL live-lane-completeness: test-shaped npm scripts that no workflow invokes," >&2
  echo "    so they run nowhere and nothing reports their absence:" >&2
  printf '%s' "$ts_report" | sed 's/^/      /' >&2
  echo "    Invoke each from a workflow, or remove the script." >&2
  exit 1
fi

echo "OK live-lane-completeness"
