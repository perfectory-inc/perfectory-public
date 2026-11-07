# ADR 0442: 운영 준비도는 매일 정의에서 만든 표 하나로 보고, 사용자가 먼저 보는 것부터 줄 세운다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md)(작업마다 문제를 계산하는 지도),
  [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md)(레인의 서빙 신선도),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산 조정기의 뒤처짐),
  [ADR-0209](./0209-the-data-flow-map-shows-the-edge-path-from-korea-and-the-ci-time-trend.md)(호스트 점검 하나),
  [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(호스트 자원 경보)

## Context

사장의 불만은 "느리다"였다. 원인은 한 막힘을 고치면 그 뒤의 막힘이 보이는 직렬 왕복이었다. 2026-11-07 W22 가 ai-server 를
읽기 전용으로 한 번에 훑었다(11:30Z 실측).

| 무엇 | 값 |
|---|---|
| 첫 판 행 | 76 행 중 57 행이 ok 아님(그중 13 행은 검사 유닛을 안 본 오판, 고친 뒤 44 행) |
| 매시 Silver 적재 | `rt_molit_real_transaction_silver` 51 시간 무성공(11-05 08:13 이후) |
| 매일 점검 | `shape_ledger_check` 7 일 실패, `zoning_tile_bake` 11-03 이후 한 번도 성공 못 함 |
| 화면 | by-PNU 레인 8 개가 48 시간 예산을 넘김(194–492 시간) |
| 실패 유닛 | 35 개 |
| 죽은 경보 | foundation-api 의 수집 지표 쿼리가 없는 열(`bronze_object.payload_size_bytes`)을 읽어 `foundation_platform_ingestion_run_*` 가 한 번도 나오지 않음 — 수집 경보 세 개가 울릴 수 없다 |

신호는 이미 다 있었다. 지도의 문제(ADR-0195)는 작업 하나씩, 서빙 모니터(ADR-0306)는 레인 하나씩, 자산 조정기(ADR-0333)는
자산 하나씩, 호스트 경보(ADR-0385)는 자원 하나씩 답한다. **전부를 한곳에 사용자 영향 순으로 놓는 것이 없었다.** 그래서 매번
사람이 훑었고, 훑는 범위가 좁을 때마다 다음 왕복이 생겼다.

검토한 사례: Google SRE 의 Production Readiness Review(서비스마다 같은 점검표를 정기로 다시 본다), Backstage Tech Insights·
Spotify Soundcheck·Datadog Service Scorecards(카탈로그의 선언에서 점수표를 매일 계산한다). 가져오는 보장은 "선언에서
계산하고, 매일 같은 모양으로, 영향 순으로"다. 점수표 서버·플러그인은 가져오지 않는다 — 우리 선언은 이미 JSON 이다.

## Decision

1. **행은 정의에서만 나온다.** `platforms/foundation-platform/scripts/ops/readiness_report.py` 는 목록을 적지 않는다.
   - 작업: `orchestration/jobs.v1.json` 의 켜진 작업 전부. systemd 저널(`journalctl _PID=1 UNIT=…`, `job-run-times.py`
     와 같은 사건)에서 마지막 성공·실패, 실패 run 의 마지막 오류 줄, systemd 결과(exit-code·timeout·signal).
     검사 유닛(`check_service`, ADR-0277)이 "할 일 없음"으로 끝난 것도 성공이다.
   - 레인: `config/by-pnu-lane-index.v1.json` 의 레인 전부. 서빙 모니터가 쓴 manifest 나이·예산(ADR-0306 textfile),
     모니터 유닛의 결과(표본 PNU 를 실 호스트명에서 읽은 판정), 자산 조정기의 뒤처짐(`asset-reconcile.prom`), 굽기·Gold 작업.
   - 원천: foundation-api `/metrics` 의 `foundation_platform_ingestion_run_last_finished_at_seconds`.
   - 호스트: `/proc/meminfo`, `/`·`/data` 디스크, 실패 유닛, 한 번도 안 돈 타이머, `MemoryMax` 의 90% 를 넘긴 유닛.
2. **판정.** 작업은 마지막 실패가 마지막 성공보다 뒤면 `failing`, 마지막 성공이 일정 한 주기 + `time_limit_minutes` 보다
   오래면 `stale`, 지도가 입력 실패로 막혔다고 하면 `blocked`. 레인은 manifest 가 예산을 넘으면 `stale`, 굽기가 꺼져 있으면
   `not-served`. 일정 해석은 `<분> <시|*> * * *` 꼴만 읽고, 시험이 모든 작업의 일정이 그 꼴임을 지킨다.
3. **줄 순서는 사용자 영향이다.** 서빙 → 서빙을 먹이는 Gold → Silver → 수집 → 운영 → 호스트. 표에는 ok 아닌 행만 둔다.
4. **매일 한 번.** `infra/systemd/foundation-readiness-report.timer`(22:10 UTC = 서울 07:10)가 서비스를 부른다. 서비스는
   `foundation-platform` 계정, `SupplementaryGroups=systemd-journal`(저널 읽기만), `ProtectSystem=strict` 로 돌고
   `/var/lib/foundation-platform/readiness/readiness.{json,md}` 와 `readiness-report.prom`
   (`foundation_readiness_blockers{impact}`, `foundation_readiness_report_timestamp_seconds`)만 쓴다. 데이터 작업이 아니라
   호스트 점검이라 systemd 타이머다(ADR-0118 §1, 서비스 점검과 같다).
5. **새 경보는 만들지 않는다.** 각 행은 이미 자기 경보를 갖는다(지도 문제, 서빙 신선도, 호스트 자원, 유닛 실패). 이 표는
   그것들의 합이고, 같은 사건을 두 번 알리지 않는다. 표가 멈추면 유닛 실패 알림(`OnFailure`)이 온다.
6. **로드맵의 표는 이 표의 날짜 붙은 사본이다.** `docs/roadmap/production-readiness.md` §9 에 행마다 주인(진행 중 작업자 또는
   새 brief)을 붙인다. 표를 손으로 다시 훑지 않는다.

## 기각한 대안

- **`service-checks.py` 의 한 부분으로.** 매시 돌고, foundation-api 가 `ServiceChecks` 계약으로 그 파일을 읽는다. 저널을 매시
  읽을 까닭이 없고, 파일 계약을 바꿔야 한다. 같은 호스트 점검 방식(타이머·계정·쓰는 곳 하나)만 빌렸다.
- **Dagster asset check 로.** 이행 중(ADR-0431)이고 Airflow 작업도 아직 많다. 표는 둘 다를 systemd 저널로 본다.
- **Backstage 도입.** 카탈로그·점수표 서버를 새로 운영해야 한다. 선언은 이미 저장소 JSON 이다.

## Consequences

- 매일 아침 표 하나가 모든 막힘을 영향 순으로 보여 준다. 첫 실측은 호스트에서 2 초, 76 행.
- 수집 원천의 기대 주기는 105 개 중 11 개만 `source_catalog.collection_frequency` 에 있다. 그래서 원천 행은 지금 나이만 말한다.
  주기를 정의에 선언하고 수집 경보를 그 주기로 고치는 것은 따로 맡긴다(로드맵 §9).
- 판정 시험: `platforms/foundation-platform/orchestration/tests/test_readiness_report.py`.
