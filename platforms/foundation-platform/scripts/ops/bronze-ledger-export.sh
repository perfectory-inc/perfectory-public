#!/usr/bin/env bash
# Bronze 원장 줄 내보내기 (root ADR-0616 A1b, A4; 코디네이터 2026-11-28). 한 원천의 원장 줄은 그 수집기와 함께 넘어간다:
# AWS 레인으로 옮겨 Lakekeeper 표를 쥔 레인이 쓰는 원천(config/lakekeeper-declaration.v1.json ledger_sources)마다,
# 아직 내보낸 것이 없으면 그 원장 줄을 control/evidence/ledger_move/<날짜>-<원천>.jsonl.gz 에 만들기만으로 한 번 쓴다.
# AWS 의 배포가 그것을 원장(RDS)에 들인다(infra/aws/ledger-import.tf). 그 원천의 쓰는 쪽이 방금 돌았으면 아무것도
# 쓰지 않고 실패한다(다음 실행이 다시 본다). 내보낼 것이 남지 않으면 `unchanged`.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"

if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

# The sources, from the generated declaration (scripts/catalog/declared_definitions.py lakekeeper_declaration).
sources="$(python3 -I -c '
import json, sys
declaration = json.load(open(sys.argv[1], encoding="utf-8"))
print(",".join(sorted({source for sources in declaration["ledger_sources"].values() for source in sources})))
' "${RELEASE_ROOT}/config/lakekeeper-declaration.v1.json")"
if [ -z "${sources}" ]; then
  echo "bronze-ledger-export: no lane on AWS holds a Lakekeeper table; nothing to export"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

out="$(mktemp)"
trap 'rm -f "${out}"' EXIT
FOUNDATION_PLATFORM_LEDGER_EXPORT_SOURCES="${sources}" "${PUBLISHER_BIN}" export-bronze-ledger-rows 2>&1 | tee "${out}"
grep -qE '^foundation-job-outcome (changed|unchanged)$' "${out}" \
  || { echo "bronze-ledger-export: the export printed no outcome" >&2; exit 1; }
