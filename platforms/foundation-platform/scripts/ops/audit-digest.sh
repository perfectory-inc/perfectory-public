#!/usr/bin/env bash
# 감사 기록 digest (root ADR-0569 §4). 매시: prepare 가 지난 digest 뒤의 줄을 확인해 digest 를 만들고, 루트의
# audit-witness.sh 가 그 해시를 Rekor 에 올리고, record 가 그 답(번들)과 함께 audit.digest 에 남긴다.
# 월요일 00:05 KST(일요일 15:05 UTC)에는 prepare 가 처음부터 사슬 전체와 모든 digest 를 다시 확인하고,
# 증인이 모든 번들을 Rekor 와 대조한다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

case "${1:-}" in
  prepare)
    weekday_hour="$(date -u +%u%H)"
    if [[ "${weekday_hour}" == "715" ]]; then
      export FOUNDATION_AUDIT_VERIFY_ALL=1
    fi
    exec "${PUBLISHER_BIN}" prepare-audit-digest
    ;;
  record)
    out="$(mktemp)"
    trap 'rm -f "${out}"' EXIT
    "${PUBLISHER_BIN}" record-audit-digest 2>&1 | tee "${out}"
    # The job's last line, the one every scheduled job ends with (root ADR-0171).
    case "$(grep -E '^audit-digest ' "${out}" | tail -n 1)" in
      'audit-digest changed '*) echo 'foundation-job-outcome changed' ;;
      'audit-digest unchanged '*) echo 'foundation-job-outcome unchanged' ;;
      *) echo "audit-digest: the record step printed no outcome" >&2; exit 1 ;;
    esac
    ;;
  *) echo "usage: audit-digest.sh prepare|record" >&2; exit 64 ;;
esac
