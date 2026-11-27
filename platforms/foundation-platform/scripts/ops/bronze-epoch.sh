#!/usr/bin/env bash
# Bronze 시기 (root ADR-0626 §8 N2). 배포가 이 릴리스로 바뀐 뒤 한 번, 그리고 매시간: 실행 도장이 잎에 있는 객체와
# 이름이 바뀐 원천의 객체를 오늘의 쓰는 쪽이 짓는 키로 옮기고(증거 목록 먼저, 만들기만 복사와 전체 바이트 다시
# 해시, 원천마다 한 트랜잭션의 제자리 UPDATE, 확인), 확인된 옛 키를 버킷 잠금이 놓아주는 대로 지운다. 옮길 원천의
# 쓰는 쪽이 방금 돌았으면 아무것도 옮기지 않고 실패한다(다음 실행이 다시 본다). 할 일이 남지 않으면 `unchanged`.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 수집기들과 같은 재료로 조립한다(원장의 제자리 UPDATE).
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

out="$(mktemp)"
trap 'rm -f "${out}"' EXIT
"${PUBLISHER_BIN}" rekey-bronze-epoch 2>&1 | tee "${out}"
# The publisher's last line is the job's outcome (root ADR-0171); the summary goes to the journal with it.
grep -E '^foundation-job-outcome (changed|unchanged)$' "${out}" | tail -n 1 | grep -q . \
  || { echo "bronze-epoch: the epoch printed no outcome" >&2; exit 1; }
