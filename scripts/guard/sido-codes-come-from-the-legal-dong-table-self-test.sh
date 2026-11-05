#!/usr/bin/env bash
# Proves sido-codes-come-from-the-legal-dong-table.sh refuses what it claims to (root ADR-0403):
# the hand-written prefix map that sent 광주·전남 to abolished codes, a code-to-word table either way
# round, a list named for 시도, a listed file that holds no table any more (the list only shrinks),
# and a listed file without its reason; and that it passes a clean tree and a shorter table.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"

guard="$(cd "$(dirname "$0")" && pwd)/sido-codes-come-from-the-legal-dong-table.sh"
fixture_root
test_root="$FIXTURE_ROOT"
trap 'rm -rf -- "$test_root"' EXIT

fail() {
  echo "FAIL sido-codes-come-from-the-legal-dong-table-self-test: $*" >&2
  exit 1
}

# Synthetic tables built in a loop (codes 91–98, words 가가…아아), so this file is not itself one.
words=(가가 나나 다다 라라 마마 바바 사사 아아)
word_to_code() { local i out=""; for i in "${!words[@]}"; do out+="${out:+, }\"${words[$i]}\": \"9$((i + 1))\""; done; printf '%s' "$out"; }
code_to_word() { local i out=""; for i in "${!words[@]}"; do out+="${out:+, }\"9$((i + 1))\": \"${words[$i]}\""; done; printf '%s' "$out"; }
code_list() { local i out=""; for i in "${!words[@]}"; do out+="${out:+, }\"9$((i + 1))\""; done; printf '%s' "$out"; }

# A clean fixture tree in $1 with the baseline given as $2 (a JSON "files" array).
build() {
  local repo="$1" files="${2:-[]}"
  mkdir -p "$repo/tools/ci" "$repo/src"
  fixture_git "$repo" init -q --initial-branch=main
  printf '{ "files": %s }\n' "$files" >"$repo/tools/ci/hardcoded-sido-codes.baseline.json"
  printf 'CODES = {"01": "가나", "02": "다라"}\n' >"$repo/src/short.py"
}

check() {
  local repo="$1" label="$2"
  fixture_git "$repo" add -A
  bash "$guard" "$repo" >"$test_root/$label.out" 2>&1
}

expect_refused() {
  local repo="$1" needle="$2" label="$3"
  if check "$repo" "$label"; then
    fail "$label: accepted"
  fi
  grep -qF "$needle" "$test_root/$label.out" || fail "$label: refused for the wrong reason: $(cat "$test_root/$label.out")"
}

clean="$test_root/clean"
build "$clean"
check "$clean" clean || fail "clean fixture refused: $(cat "$test_root/clean.out")"

# The incident's shape: a source contract mapping address words to codes (synthetic words, codes 91–98).
contract="$test_root/contract"
build "$contract"
printf '{"sido_prefixes": {%s}}\n' "$(word_to_code)" >"$contract/src/source.contract.json"
expect_refused "$contract" "src/source.contract.json: a code-to-word mapping of 8" contract

labels="$test_root/labels"
build "$labels"
printf 'export const LABEL = { %s };\n' "$(code_to_word)" >"$labels/src/labels.ts"
expect_refused "$labels" "src/labels.ts: a code-to-word mapping of 8" labels

list="$test_root/list"
build "$list"
printf 'export const SIDO_CODES = [%s] as const;\n' "$(code_list)" >"$list/src/sido.ts"
expect_refused "$list" "src/sido.ts: a sido list of 8" list

stale="$test_root/stale"
build "$stale" '[{"path": "src/short.py", "reason": "r", "removed_by": "x"}]'
expect_refused "$stale" "holds no 시도 table any more" stale

reasonless="$test_root/reasonless"
build "$reasonless" '[{"path": "src/sido.ts", "reason": "", "removed_by": ""}]'
printf 'export const SIDO_CODES = [%s] as const;\n' "$(code_list)" >"$reasonless/src/sido.ts"
expect_refused "$reasonless" "names no reason or removed_by" reasonless

listed="$test_root/listed"
build "$listed" '[{"path": "src/sido.ts", "reason": "r", "removed_by": "x"}]'
printf 'export const SIDO_CODES = [%s] as const;\n' "$(code_list)" >"$listed/src/sido.ts"
check "$listed" listed || fail "a listed file was refused: $(cat "$test_root/listed.out")"

echo "OK sido-codes-come-from-the-legal-dong-table-self-test"
