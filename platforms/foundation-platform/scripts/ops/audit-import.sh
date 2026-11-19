#!/usr/bin/env bash
# 감사 기록 가져오기 (root ADR-0569 §6). 15분마다: 자동 배포의 상태 파일 전부와, 지난 커서 뒤의 운영자
# 호스트 명령(journal `foundation-operator-start`, root 가 쓴 줄만)을 audit.event 에 덧붙인다. 두 번 읽은
# 줄은 한 번만 쓴다. 덧붙인 줄이 있으면 `changed`, 없으면 `unchanged`.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 다른 작업과 같은 재료로 조립한다. 가져오기는 그 연결에서
# SET SESSION AUTHORIZATION foundation_audit_importer 로 바꿔 쓴다: 그 역할은 로그인하지 못한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

out="$(mktemp)"
trap 'rm -f "${out}"' EXIT
"${PUBLISHER_BIN}" import-audit-events 2>&1 | tee "${out}"
result="$(grep -E '^audit-import ' "${out}" | tail -n 1)"
# The job's last line, the one every scheduled job ends with (root ADR-0171).
case "${result}" in
  'audit-import changed '*) echo 'foundation-job-outcome changed' ;;
  'audit-import unchanged '*) echo 'foundation-job-outcome unchanged' ;;
  *) echo "audit-import: the import printed no outcome" >&2; exit 1 ;;
esac
