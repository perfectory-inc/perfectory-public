# ADR 0480: 모든 작업은 유닛의 종료가 아니라 정의가 기대하는 결과로 재고, 결과가 없으면 주인을 부른다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(결과 한 줄 `changed|unchanged|waiting|failed`),
  [ADR-0442](./0442-production-readiness-is-one-table-a-day-built-from-the-definitions-sorted-by-what-users-see.md)(매일 준비도 표),
  [ADR-0443](./0443-each-collected-source-declares-its-providers-release-cadence-and-the-ingestion-alerts-hold-it-to-that.md)(원천의 공개 주기 선언),
  [ADR-0444](./0444-a-by-pnu-lane-is-stale-when-it-is-behind-its-inputs-not-when-its-manifest-is-old.md)(레인은 입력보다 뒤처졌을 때 묵은 것),
  [ADR-0445](./0445-a-failed-host-unit-names-its-owner-and-runbook-and-a-unit-that-stays-failed-calls-its-owner.md)(유닛의 주인과 런북),
  [ADR-0418](./0418-the-artifacts-the-declared-definitions-generate-are-never-committed-and-every-checkout-and-release-generates-them.md)(정의가 작업 목록을 만든다, 생성본은 커밋하지 않는다),
  [ADR-0327](./0327-a-worker-deploy-and-a-quarter-hourly-probe-read-what-users-read-twice.md)(엣지 탐침)

## Context

두 주 동안 다섯 번, 무엇인가가 "돌고 있는" 것처럼 보였는데 아무것도 만들지 않았다. 다섯 번 모두 실패한 유닛은 없었다.

| 사건 | 무엇이 "정상"으로 보였나 | 실제 결과 |
|---|---|---|
| Dagster 코드 위치 (11-06 21:16Z~11-09) | UI 가 응답하고 배포가 "observing" | 로드 6,222번 실패, 이중 실행 비교 0줄 |
| 법원경매 물건상세 | `court_auction_collect` 가 매일 `changed` | 물건상세 Bronze 0건 — `catalog.source_catalog` 에 원천 행조차 없음(2026-11-11 실측, 아직 그대로) |
| 운영 표 모양 원장 | `shape_ledger_check` 가 `waiting` | 원장을 쓰는 호스트 사슬이 설치된 적 없음, 48시간 뒤에야 실패 |
| 새 Silver 적재 10개 (11-09 07:00Z) | 각 시작이 `waiting` 으로 0 종료 | 풀 거절, 다시 시작한 것 없음 |
| data_go_kr 파일 데이터 (11-09 켬) | 작업 켜짐, 유닛 실패 없음 | 첫 시작이 건너뛰어졌고 그 뒤 결과 줄 0 — 30일 저널에 한 줄도 없음(실측) |

원인은 하나다. **우리는 과정(유닛이 성공했나)을 보고, 결과(무엇을 만들었나)를 보지 않았다.** 결과 줄(ADR-0260)은 있지만
① 유닛의 저널에만 있고, 러너가 시작하지 않고 끝낸 거절·일시정지·미룸의 줄은 Airflow 로그에만 있으며, ② `waiting`·`unchanged`
는 성공이라 아무도 그 연속을 세지 않았다. 원천 쪽 오래됨 경보(ADR-0443)는 한 번도 수집되지 않은 원천에는 시계열이 없어 맞추지 못했다
(ADR-0443 Consequences 가 적은 한계 그대로).

검토한 사례:

- **Google SRE 의 golden signals 와 데이터 파이프라인 SLO**(SRE Workbook *Data Processing Pipelines*): 파이프라인은 "실행됐나"가
  아니라 산출의 신선도·정확성·처리량으로 잰다. 산출이 없으면 실행이 성공해도 SLO 위반이다.
- **Monte Carlo·Soda·Great Expectations 의 데이터 관측**: 표마다 freshness(마지막 갱신)·volume(행 수)·schema 를 기대값과 비교하고,
  0행 적재나 갱신 없는 기간을 실행 상태와 따로 알린다. dbt 의 `source freshness` 도 같은 "선언한 주기와 비교".
- **Prometheus "임계값을 시계열로"**(Robust Perception): 대상마다 다른 기한을 규칙에 쓰지 않고 시계열로 내보내 `on()` 으로 맞춘다
  (ADR-0443 과 같은 방식). **Datadog Synthetics·Google 의 prober**: 사용자 경로를 주기적으로 실제로 따라가 본다.

가져오는 보장: 기대는 정의 한 곳에서, 결과는 실행이 끝나는 자리에서 모두 남기고, 기대와 결과의 차이를 주인에게. 관측 SaaS 는
가져오지 않는다 — 선언은 이미 저장소 JSON 이고, 경보 길(Prometheus → Alertmanager → Slack)은 이미 있다.

## Decision

1. **기대는 정의에 선언한다.** 모든 작업(`definitions/*.definition.json` 의 `jobs`, 생성본 `orchestration/jobs.v1.json`)이
   `expected_outcome` 을 갖는다.
   - `changes_within_hours`: `changed` 로 끝난 실행이 이만큼 안에 하나는 있어야 한다. 숫자에는 `basis`(엔드포인트 카탈로그의 공개 주기와
     2026-11-11 저널 실측)가 붙는다. null 이면 `no_change_bound_reason` 이 왜 기대가 없는지 말한다(입력이 바뀔 때만 바뀌는 작업은 ADR-0333
     조정기·ADR-0444 레인 뒤처짐이 이미 본다; 운영 작업; 공개일이 없는 원천).
   - 모든 켜진 작업은 따로 선언 없이 **`done`** 기대를 갖는다: `changed` 또는 `unchanged` 로 끝난 실행·확인이 일정 한 주기 +
     `time_limit_minutes` + `wait_limit_minutes` + 1시간 안에 있어야 한다(설계된 긴 기다림은 `done_within_hours`). 기다림·건너뜀·풀 거절·
     실패·시작 안 됨은 `done` 이 아니다.
   - 90개 작업 모두 선언했다: 기한 있는 작업 42개, 없음+이유 48개. `orchestration/tests/test_job_outcomes.py` 가 선언 없는 작업, 자기 주기보다
     짧은 기한, 그 Silver 레인의 원천 카탈로그 공개 주기보다 이른 기한, 공개일 없는 원천에 기한을 단 레인을 거부한다.
2. **결과는 러너가 모두 원장에 쌓는다.** `start-scheduled-job.sh` 가 자기가 말하는 모든 결과 줄(실행의 결과, 확인의 답, 시작하지 않고
   끝낸 거절·일시정지·미룸)을 `job_units.py record-outcome` 으로 `/var/lib/foundation-jobs/queue/outcomes.jsonl` 에 한 줄씩 덧붙인다
   (덧붙이기만, ADR-0462 의 시작 대기열 옆). 확인이 "할 일 없음"이면 `unchanged`(ADR-0305: 출력이 입력을 담고 있다). 차례 미룸(ADR-0138)은 DAG 에는 그대로
   `unchanged`(하류를 깨우지 않음)지만 원장에는 `skipped` 다 — 아무것도 돌지 않았다. Airflow 든 Dagster
   든(ADR-0431 proxied 도 같은 강제 명령) 모든 시작이 이 러너를 지나므로 오케스트레이터와 상관없이 같은 원장이다.
3. **조정기가 원장의 새 줄만 읽어 기대와 비교한다.** 자산 조정기(5분, ADR-0333)가 지난 tick 의 바이트 위치부터 새 줄만 읽어
   (부분 처리) 작업·결과마다 가장 새 시각만 접은 요약(`job-outcomes.json`)을 남기고, `job-outcomes.prom` 에 켜진 작업마다
   `foundation_job_outcome_met_seconds{job_id,expectation}`(마지막으로 기대를 채운 때; 없으면 켠 때와 원장 첫 줄 중 늦은 때),
   `foundation_job_outcome_expected_seconds`(기한), `foundation_job_outcome_info{unit,owner,runbook}`(유닛 파일의
   `X-Foundation-Owner`·`Documentation`, ADR-0445)를 쓴다. 판정 코드는 `scripts/ops/job_outcomes.py` 하나이고, 매일 준비도 표
   (ADR-0442)도 같은 함수로 작업 행을 `silent` 로 만든다(새 표가 아니라 기존 표의 상태).
4. **경보는 기대가 빠졌을 때 주인을 부른다.** `foundation-job-outcomes` 규칙 묶음:
   - `FoundationJobOutcomeMissing`(ticket): 기한을 넘긴 기대마다 한 알림, 주인·런북 라벨. 배포가 DAG 를 멈춘 동안은 배포의 알림 몫.
   - `FoundationJobOutcomeMissingLong`(page, `<!channel>`): 기한을 하루 넘김.
   - `FoundationJobOutcomesSilent`(page): 한 시간 동안 어떤 작업도 결과를 남기지 않음 — 스케줄러가 아무것도 결정하지 않는다.
   - `FoundationJobOutcomesUnread`: 조정기 파일이 30분 넘게 낡음.
   Alertmanager 경로 `service="job-outcome"` 은 작업·기대마다 12시간마다 다시 알린다. 실패한 작업의 `done` 은 이미 실패 알림
   (`FoundationScheduledJobProblem{kind=failed}`·`FoundationUnitFailedLong`)이 말하므로 억제한다(한 사건 한 알림, ADR-0442 §5).
   기다리고·거절되고·시작 안 된 작업에는 다른 알림이 없으므로 이것이 그 알림이다.
5. **원천 쪽: 주기를 선언했는데 한 번도 수집되지 않은 원천.** `FoundationPlatformIngestionSourceNeverCollected` 가
   `expected_max_age` 시계열은 있는데 마지막 완료 실행 시계열이 없는 원천을 하루 뒤 알린다(ADR-0443 의 한계를 닫는다).
6. **매주 합성 화면 탐침.** `by-pnu-edge-probe.py --screen`(`foundation-by-pnu-screen-probe.timer`, 월 23:23 UTC)이 각 cut-over 레인의
   gate 표본에서 서빙되는 첫 PNU(호스트에서 읽음, 저장소에 PNU 없음)마다 모든 by-PNU Worker 가 서빙하는 **모든 레인**을 한 번씩 읽는다:
   `shown`(200·레인의 content type·JSON 객체)·`absent`(404, 그 필지에 없음)·`error`·`missing`(그 PNU 의 자기 레인이 404). 레인 신선도는
   서빙 모니터·조정기 시계열로 예산과 비교한다(ADR-0444: 입력보다 뒤처졌을 때만 묵음). error·missing·예산 넘긴 shown 레인이 있으면
   실패 → `OnFailure` 로 주인 `serving`, 6시간 뒤 `FoundationUnitFailedLong`. 결과는 `screen-probe.prom`.
7. **사건마다 시험이 있다**(ADR-0001). `orchestration/tests/test_job_outcomes.py` 의 `TheFiveIncidentsWouldHaveFired` 가 다섯 사건을
   실제 정의로 재생하고, promtool 시험(`foundation-job-outcomes.rules.test.yml`, `foundation-ingestion.rules.test.yml`)이 같은 사건의
   시계열로 경보가 울림을 보인다. 러너가 줄을 남기는 것은 `test_start_scheduled_job.py` 의 `EveryOutcomeIsKept`.

## 기각한 대안

- **유닛 저널을 조정기가 5분마다 읽기.** 90개 유닛 × 저널 검색은 tick 마다 전량이고, 러너가 시작하지 않은 거절·일시정지는 유닛 저널에
  없다. 결과를 말하는 자리(러너)에서 한 번 쓰고 새 줄만 읽는다.
- **Bronze 행 수·Silver 커밋 행 수를 결과로.** 생산자마다 세는 자리가 다르고(Iceberg 스냅숏 요약, `catalog.ingestion_run`, 굽기의
  reads.jsonl), 그 모두를 하나로 모으는 것은 새 저장소다. 실행 단위의 "썼다/안 썼다"는 이미 모든 작업이 같은 어휘로 말한다(`changed`).
  원천의 0건은 5항이, 레인의 입력 뒤처짐은 ADR-0444 가 본다. 30일 실측 2,950번 Bronze 실행 중 0 객체 실행은 15번(연속지적도)뿐이었다.
- **기한을 원천 카탈로그에서 자동 유도.** 원천 무리(source group)와 작업의 관계가 하나가 아니다(같은 VWorld 무리에 일·월·연 주기가
  섞임). 작업마다 숫자를 근거와 함께 적고, 시험이 카탈로그보다 이르지 않음을 지킨다.
- **Monte Carlo·Soda 도입.** 선언·경보 길이 이미 있고, 운영 표가 R2/Iceberg·Postgres·systemd 에 흩어져 있어 SaaS 커넥터로는 결과의
  절반(거절·대기)을 못 본다.

## Consequences

- 배포 후 첫 기준 시각은 원장의 첫 줄이다: 그 전 시간은 세지 않으므로 배포 직후 오래된 작업이 한꺼번에 울리지 않고, 기한이 한 번 지나야
  울린다. 월간 레인의 `change` 기대는 배포 45일 뒤부터 의미가 있다.
- 2026-11-10 17:49Z 운영 저널(유닛+확인 유닛, 30일, 읽기 전용)을 새 판정에 재생한 계산: 켜진 작업 69개 중 `done` 기한을 넘긴 것 7개 —
  `data_go_kr_file_data`(30일 결과 0, 12.3시간 초과), `shape_ledger_check`(31.5시간, 실패 중이라 억제), `zoning_tile_bake`(25.8시간),
  `gold_kind_rebuild`(마지막 `waiting`, 3.2시간), `by_pnu_serving_bake`(0.3시간), 그리고 진행 중 배포로 멈춘 `asset_reconcile`·
  `outbox_publish`(규칙이 배포 동안 억제). 재생은 러너만 아는 줄(거절·일시정지)이 없어 실제보다 많이 셀 수 있다.
- 원천 쪽: 주기 선언 99개 중 완료 실행이 없는 원천 1개 — `courtauctiongokr__auction_item_detail`. 배포 하루 뒤 울린다(사건 2 가 아직 열려 있다).
- 비용: 결과 한 줄당 Python 한 번(수십 ms), 원장은 하루 약 600줄(약 100 KB). 조정기는 새 줄만 읽는다. 주간 탐침은 약 450 GET. 돈은 들지 않는다.
- 원장은 지우지 않는다(덧붙이기만). 크기가 문제가 되면 호스트 보존(ADR-0198)에 회전을 더한다 — 요약의 `offset` 은 짧아진 파일을 처음부터 읽는다.

개정(2026-11-11, W59): **조정기가 다루는 작업은 출력이 입력만큼 새로우면 `done` 이다.** 위 재생의 `gold_kind_rebuild`·
`by_pnu_serving_bake` 두 건은 할 일이 없던 작업이었다(잰 값, 호스트 Airflow 로그·Trino `$snapshots`·조정기 로그, 2026-11-10):

- `by_pnu_serving_bake` 의 시계 시작은 11-09·11-10 두 번 모두 차례 미룸(ADR-0138)으로 아무것도 돌리지 않았다(11-09 미룸 상대 15개,
  11-10 `gold_unit_price_rebuild`). 그동안 `gold.parcel_panel` 의 두 커밋(11-09 10:38·20:24)은 고정판만 새로 적었고 바뀐 PNU 는 0
  (`changed_keys=0`)이라 굽기 몫이 없었다. `gold_kind_rebuild` 의 11-10 시작은 패널 재생성에 막혀 `waiting` 이었고 그 뒤 입력이 바뀌지 않았다.
- 같은 시각 조정기(ADR-0333 §7)는 두 작업의 모든 자산을 `current, lag 0s` 로 보았다. 그러니 `done` 이 실행만 세면 입력이 조용한
  날마다 이 두 작업과 조정기의 다른 열세 작업이 주인을 부른다(거짓 경보).

결정: 조정기가 한 tick 에서 자기가 다루는 작업의 **모든** 자산을 읽었고 그 어느 것도 owed·실행 중·읽기 실패가 아니면, 그 작업을
그 시각 `current` 로 요약에 적는다(`asset_reconcile.tick(current=…)` → `job_outcomes.fold_current`). `done` 은 `changed`·`unchanged`·
`current` 중 가장 새것으로 잰다. `current` 는 원장 줄이 아니다(덧붙이지 않는다, 줄 수·처음·끝 시각 불변). `change` 기대에는 들지 않는다.
owed 인데 시작되지 않는 자산은 이미 조정기 자신의 `FoundationAsset*` 경보(미스케줄·뒤처짐)가 부른다. Dagster 의 신선도도 같은 뜻이다:
파생 자산은 "상류보다 늦지 않게 물질화됐는가"로 재고(freshness policy 의 `maximum_lag`, `AutomationCondition.any_deps_updated`),
상류가 조용하면 늦은 것이 아니다. 시험: `test_job_outcomes.py::TheVerdicts::test_a_reconciled_job_whose_outputs_stay_current_is_done_though_nothing_ran`
(위 재생 그대로), `test_asset_reconcile.py::Reconciler::test_a_job_is_current_only_when_every_asset_of_it_was_read_and_owes_nothing`.
