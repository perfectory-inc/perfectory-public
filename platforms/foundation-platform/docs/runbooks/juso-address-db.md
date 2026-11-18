---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 주소DB 수집 런북

루트 ADR-0495 의 운영 절차다. 질문·파일 종류·상한은 `infra/lakehouse/contracts/juso-address-db.contract.json`
하나에 있다. 작업은 `juso_address_db`(`foundation-juso-address-db.service`, 매일 23:45 UTC), 스크립트는
`scripts/ops/juso-address-db-cycle.sh`. 로그인·키·신청이 없다. 사람 할 일이 없다.

## 상태와 커서

`/var/lib/foundation-platform/juso-address-db/collector/state.json`:

- `base_month`: 기초로 받은 월전체(`YYYYMM`). 없으면 다음 run 이 가장 새 월전체를 받는다.
- `next_day`: 다음에 받을 일변동의 날. 파일이 커밋된 뒤에만 옮긴다.

run 마다의 기록은 `runs/<run_id>/collect/manifest.json`(받은 파일·바이트·멈춘 이유)과 `journal.log`.

## 실패했을 때

- 목록·내려받기가 HTTP 오류이거나 zip 이 아니면 run 이 실패하고 OnFailure 알림이 간다. 받은 것까지는 남고 커서는
  마지막으로 커밋한 날 뒤에 있다. 다음 일정의 run 이 그 자리부터 잇는다. 지울 파일은 없다.
- 기초를 다시 받아야 할 때(예: 기초 객체를 잃음)만 `state.json` 을 옮겨 두면 다음 run 이 새 기초부터 시작한다.
  일변동은 새 기초 달 첫날부터 다시 재생된다.
