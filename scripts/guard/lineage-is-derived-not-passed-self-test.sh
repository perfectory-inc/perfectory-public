#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject, and accepts what it must not refuse.
#
# A check only ever seen passing is a check nobody has tested. The last fixture matters as much
# as the first: written as "the name is forbidden", this guard refused three publication commands
# whose value is a revision row id rather than an object name.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/lineage-is-derived-not-passed.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL lineage-is-derived-not-passed-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Assembled, so this file is not itself a match for the rule it tests.
token="SOURCE""_RECORD_ID"

make_repo() {
  mkdir -p "$1/scripts/load" "$1/services/x/src"
  git -C "$1" init -q
  git -C "$1" config user.email guard@example.invalid
  git -C "$1" config user.name guard
}
commit_all() {
  git -C "$1" add -A >/dev/null
  git -C "$1" -c commit.gpgsign=false commit -qm fixture >/dev/null
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL lineage-is-derived-not-passed-self-test: rejected allowed fixture ($2)" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL lineage-is-derived-not-passed-self-test: accepted forbidden fixture ($2)" >&2
    exit 1
  fi
}

# 1. A command handed an input, deriving the name itself.
allowed="$test_root/allowed"
make_repo "$allowed"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'FOUNDATION_X_INPUT_OBJECT_KEY="$key" "$PUBLISHER" convert' \
  >"$allowed/scripts/load/run.sh"
printf '%s\n' \
  'const INPUT: &str = "FOUNDATION_X_INPUT_OBJECT_KEY";' \
  'fn lineage(&self) -> String { self.input.key().to_owned() }' \
  >"$allowed/services/x/src/lib.rs"
commit_all "$allowed"
expect_allowed "$allowed" "정상"

# 2. A command that opens nothing, recording a revision row id. This is the shape the first
#    version of this guard wrongly refused, and refusing it would have been a false alarm that
#    teaches people to disable the check.
publication="$test_root/publication"
make_repo "$publication"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  "FOUNDATION_X_${token}=019d2b87-3fd1-7e3a-8d88-0b72c8743702 \"\$PUBLISHER\" promote" \
  >"$publication/scripts/load/promote.sh"
printf '%s\n' \
  "const RECORD: &str = \"FOUNDATION_X_${token}\";" \
  "let id = required_env(\"FOUNDATION_X_${token}\")?;" \
  >"$publication/services/x/src/promote.rs"
commit_all "$publication"
expect_allowed "$publication" "여는 입력이 없는 명령은 대상이 아니다"

# 3. A caller handing the command an input and also naming it. This is the incident's shape.
passed="$test_root/passed"
make_repo "$passed"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'FOUNDATION_X_INPUT_OBJECT_KEY="$key" \' \
  "FOUNDATION_X_${token}=\"\$key\" \"\$PUBLISHER\" convert" \
  >"$passed/scripts/load/run.sh"
commit_all "$passed"
expect_rejected "$passed" "입력을 주면서 이름까지 알려 준다"

# 4. The same defect one layer down: the command opens an input and still takes its name from
#    outside, so the two can disagree without anything noticing.
from_env="$test_root/from_env"
make_repo "$from_env"
printf '%s\n' \
  'const INPUT: &str = "FOUNDATION_X_INPUT_OBJECT_KEY";' \
  "let id = required_env(\"FOUNDATION_X_${token}\")?;" \
  >"$from_env/services/x/src/lib.rs"
commit_all "$from_env"
expect_rejected "$from_env" "명령이 입력을 열면서 이름은 환경에서 읽는다"

# 5. Optional is not a lesser version of the same thing: a value that may arrive is a value that
#    may arrive wrong.
optional="$test_root/optional"
make_repo "$optional"
printf '%s\n' \
  'const INPUT: &str = "FOUNDATION_X_INPUT_PATH";' \
  "let id = optional_env(\"FOUNDATION_X_${token}\")?;" \
  >"$optional/services/x/src/lib.rs"
commit_all "$optional"
expect_rejected "$optional" "선택으로 읽어도 밖에서 오는 값이다"

echo "OK lineage-is-derived-not-passed-self-test"
