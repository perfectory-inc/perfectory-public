---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 작업 결과 — 유닛이 성공했는데 일을 안 한 작업

[루트 ADR-0480](../../../../docs/adr/0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md).
유닛의 종료 상태가 아니라 **작업이 낸 결과**를 본다. 정의(`definitions/*.definition.json` 의 작업마다 `expected_outcome`,
생성본 `orchestration/jobs.v1.json`)가 기대를 말하고, 스케줄러의 러너(`scripts/ops/start-scheduled-job.sh`)가 말한 결과를
모두 원장에 쌓고, 자산 조정기(5분)가 원장과 기대를 비교해 `job-outcomes.prom` 에 쓴다.

| 기대 | 뜻 | 기한 |
|---|---|---|
| `done` | `changed` 또는 `unchanged` 로 끝난 실행·확인 | 일정 한 주기 + `time_limit_minutes` + `wait_limit_minutes` + 1시간 (`done_within_hours` 가 더 길면 그것) |
| `change` | `changed` 로 끝난 실행 | `changes_within_hours` (null 이면 기대 없음, 이유는 `no_change_bound_reason`) |

`waiting`·`skipped`·풀 거절·`failed`·시작조차 안 됨은 `done` 이 아니다. 실패는 이미 `FoundationScheduledJobProblem`·
`FoundationUnitFailedLong` 이 알리므로 그 둘이 울리는 동안 `done` 알림은 Alertmanager 가 누른다. 원장이 생기기 전과 작업을
켜기 전 시간은 세지 않는다(기준 시각 = 켠 때와 원장 첫 줄 중 늦은 것).

## 어디를 보나

| 무엇 | 위치 |
|---|---|
| 원장(덧붙이기만) | `/var/lib/foundation-jobs/queue/outcomes.jsonl` — 한 줄에 `job`·`at`·`outcome`·`via`(run·check)·`reason`·`invocation` |
| 조정기가 접은 요약 | `/var/lib/foundation-platform/asset-reconcile/job-outcomes.json` (읽은 바이트 위치 `offset` 포함) |
| 시계열 | `/var/lib/foundation-jobs/metrics/job-outcomes.prom` |
| 매일 표 | 준비도 보고(`readiness.md`)의 작업 행 상태 `silent`, 근거 `outcome_missed` |
| 매주 화면 | `foundation-by-pnu-screen-probe.timer`(월 23:23 UTC) → `screen-probe.prom` |

```sh
# 한 작업의 마지막 결과 열 줄
grep '"job": "data_go_kr_file_data"' /var/lib/foundation-jobs/queue/outcomes.jsonl | tail -n 10
# 지금 기한을 넘긴 기대
grep -E 'foundation_job_outcome_(met|expected)_seconds' /var/lib/foundation-jobs/metrics/job-outcomes.prom
```

## 알림별 조치

- **`FoundationJobOutcomeMissing` (`done`)**: 원장의 마지막 줄들이 무엇인지 본다.
  - `waiting <이유>` 가 이어짐 — 이유가 가리키는 것을 푼다: 풀 거절이면 시작 대기열(루트 ADR-0462)·claim 을, 공급자 계정이면
    공급자 알림(루트 ADR-0363)을, 운영자 몫이면 더니어에서 그 일을 한다. 기다림이 설계된 작업이면 `done_within_hours` 를 근거와 함께 늘린다.
  - `skipped paused` — 더니어에서 일시정지한 작업이다. 다시 켜거나, 더 안 쓰면 정의에서 `enabled: false`.
  - 줄이 아예 없음 — 스케줄러가 시작하지 않았다. 그 DAG 가 켜져 있는지, 놓친 시작(루트 ADR-0467)이 잡혔는지 본다.
- **`FoundationJobOutcomeMissing` (`change`)**: 실행은 끝나는데 쓴 것이 없다. 원천이 새 판을 냈는지(엔드포인트 카탈로그의
  `expected_cadence`, Bronze `catalog.ingestion_run`) 먼저 보고, 냈는데 안 쌓였으면 그 작업의 런북(알림의 `runbook` 라벨)대로 고친다.
  원천의 공개 주기가 실제로 바뀌었으면 `changes_within_hours` 와 `basis` 를 실측과 함께 고친다.
- **`FoundationJobOutcomeMissingLong`**: 위와 같고, 기한을 하루 넘겼다. 주인이 그날 고친다.
- **`FoundationJobOutcomesSilent`**: 한 시간 동안 어떤 작업도 결과를 남기지 않았다. Airflow 스케줄러·SSH 통로
  (`start-scheduled-job`)·러너의 `record-outcome` 오류(`cannot record the outcome`)를 본다.
- **`FoundationJobOutcomesUnread`**: 조정기가 파일을 못 쓴다. `journalctl -u foundation-asset-reconcile.service` 의
  `could not read the job outcomes` 를 본다.
- **`FoundationPlatformIngestionSourceNeverCollected`**: 카탈로그는 주기를 선언했는데 그 원천의 Bronze 실행이 한 번도 끝나지
  않았다. 수집기가 그 엔드포인트를 실제로 부르는지(설정·카탈로그 행), 부르지 않을 것이면 엔드포인트에 `collection_decision`
  을 적는다(루트 ADR-0400).
- **주간 화면 실패(`foundation-by-pnu-screen-probe.service`)**: 실패 줄이 레인과 판정을 말한다. `error` 는 엣지(그 레인 Worker),
  `missing` 은 표본 PNU 의 자기 레인이 404(서빙 모니터·굽기), `older than its freshness budget` 은 레인 신선도(루트 ADR-0444).

## 기대를 바꿀 때

정의 파일만 고치고 `python3 scripts/catalog/declared_definitions.py generate` 로 생성본을 다시 만든다.
`orchestration/tests/test_job_outcomes.py` 가 모든 작업의 선언, 주기보다 짧은 기한, 원천 카탈로그보다 이른 기한을 거부한다.
