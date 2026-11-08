#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/no-hand-cleared-gate-file.yml (`opengrep scan --test`). Not run.
set -euo pipefail

# ruleid: no-run-waits-for-a-person-to-remove-a-file
# 그 파일이 있는 동안 모든 run 이 요청 없이 실패한다: 사람이 읽고 지워야 다시 묻는다.
[ ! -e "${STATE}/refused.json" ] || exit 1

# ruleid: no-run-waits-for-a-person-to-remove-a-file
echo "blocked until the operator acknowledges ${STATE}/halt" >&2

# ok: no-run-waits-for-a-person-to-remove-a-file
# 다음 실행을 막는 파일도, 사람이 지울 것도 없다.
echo "court-auction collect: refused before any side effect: missing env" >&2
