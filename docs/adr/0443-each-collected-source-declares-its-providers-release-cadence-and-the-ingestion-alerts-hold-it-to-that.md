# ADR 0443: 수집하는 원천마다 공급자 공개 주기를 선언하고, 수집 경보는 원천을 그 주기로 잰다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0442](./0442-production-readiness-is-one-table-a-day-built-from-the-definitions-sorted-by-what-users-see.md)(준비도 표 §9 의 13 번 행),
  [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(규칙은 promtool 로 시험),
  [ADR-0400](./0400-every-source-is-collected-or-names-why-not.md)(원천은 수집하거나 안 하는 이유를 적는다)

## Context

2026-11-07 W22 준비도 점검(ADR-0442)이 찾은 13 번 행: 수집 경보 세 개(`FoundationPlatformIngestionRunStale`·`…Failed`·`…Slow`)는
**한 번도 울릴 수 없었다.** foundation-api 의 지표 쿼리가 `catalog.bronze_object.payload_size_bytes` 를 읽었는데 열 이름은
`size_bytes` 다. `/metrics` 는 쿼리 오류를 빈 결과로 삼켜(로그도 없이) `foundation_platform_ingestion_run_*` 가 하나도 나오지 않았다.

열만 고치면 다른 문제가 드러난다. 규칙은 모든 원천에 24 시간 한 줄을 댄다. 운영 DB 실측(2026-11-07):

| 무엇 | 값 |
|---|---|
| `catalog.source_catalog` 원천 | 105 개 |
| 기대 주기가 적힌 원천(`collection_frequency`, 수집기가 쓰는 자유 문장) | 11 개 |
| 마지막 Bronze 실행이 24 시간보다 오래된 원천 | 85 개 |
| 그중 실제로 늦은 원천 | 1 개(법원경매 낙찰결과, 451 시간) |
| 마지막 실행이 3,600 초보다 길었던 원천 | 2 개(법원경매: 5,213·4,665 초 — 정상, 작업 상한 300 분) |

대부분의 공급자는 월·분기·연 단위로 공개하고(건축HUB 57 종 월간, 연속지적도 월간, 공시지가 연간, 산단 비정기), Bronze 는 새 판이
나왔을 때만 쌓인다. 그러니 Bronze 의 나이는 **공급자 주기에 비춰야만** 건강 신호다. `collection_frequency` 는 수집기가
코드에 박은 문장이라 정의가 아니고, 105 개 중 94 개가 비어 있다.

검토한 사례: Prometheus 의 "임계값을 시계열로"(Robust Perception, *Using time series as alert thresholds*: 대상마다 다른
임계값을 규칙에 쓰지 않고 지표로 내보내 `on()` 으로 맞춘다), Google SRE Workbook 의 데이터 파이프라인 신선도 SLO(데이터셋마다
기대 갱신 주기를 선언하고 그것으로 잰다), dbt 의 source freshness(`sources.yml` 의 원천마다 `warn_after`/`error_after` 를
선언하고 `loaded_at` 과 비교한다). 가져오는 보장은 "원천마다 선언한 주기, 선언은 한곳".

## Decision

1. **선언은 엔드포인트 카탈로그 하나.** `docs/catalog/public-source-endpoint-catalog.v1.json`(외부 엔드포인트 정본)의 수집하는
   엔드포인트(=`collection_decision` 이 없는 것, 121 개)마다 `expected_cadence` 를 둔다:
   `release`(continuous·daily·monthly·bimonthly·quarterly·yearly·irregular), `max_age_hours`(공개 주기 + 공개 지연 여유; irregular 은
   `null`), 선택 `max_run_minutes`(기본 60), `basis_ko`(근거: 실측한 `provider_updated_at`·`snapshot_date`, 계약의 값). 수집 안 하는
   엔드포인트는 선언하지 않는다. 카탈로그 규칙 `collected_endpoint_declares_expected_cadence`.
   원천 무리(source group)가 아니라 엔드포인트에 두는 이유: 같은 VWorld 무리 안에서도 일 변동분(토지이동)·월간(연속지적도)·연간
   (공시지가)·비정기(산단)가 섞여 있다.
2. **공개일이 정해지지 않은 원천은 나이 한계가 없다.** 법정동 코드·산단·센서스 경계·토지소유처럼 공급자가 정해진 날 없이 내는
   원천의 Bronze 나이는 건강 신호가 아니다. 그 살아 있음은 수집 작업의 일정(`FoundationScheduledJobProblem`, ADR-0195)이 본다.
3. **foundation-api 가 선언을 시계열로 낸다.** `/metrics` 가 이미지에 실린 카탈로그(`FOUNDATION_PLATFORM_SOURCE_ENDPOINT_CATALOG_PATH`)를
   읽어 `foundation_platform_ingestion_source_expected_max_age_seconds{source}`(나이 한계가 있는 원천)와
   `foundation_platform_ingestion_source_max_run_seconds{source}`(선언한 모든 원천)를 낸다. 두 엔드포인트가 한 원천을 채우면 더
   짧은 나이·더 긴 실행을 쓴다.
4. **규칙은 `foundation-ingestion` 무리로 옮기고 선언과 비교한다.** 오래됨 = `time() - max by (source)(last_finished) > on (source)
   expected_max_age`, 느림 = `max by (source)(last_duration) > on (source) max_run`, 실패는 그대로. 선언을 못 읽으면(시계열 없음)
   `FoundationPlatformIngestionCadenceUnreadable` 이 30 분 뒤 알린다 — 그렇지 않으면 위 둘이 아무것도 맞추지 못하는 같은 침묵이다.
   네 경보 모두 `foundation-ingestion.rules.test.yml` 이 promtool 로 시험한다(ADR-0385 가드가 무리 전체 시험을 요구).
5. **쿼리는 CI 의 실제 스키마에서 돈다.** `/metrics` 의 세 쿼리를 `METRICS_QUERIES` 로 내보내고,
   `services/foundation-api/tests/metrics_queries_match_schema.rs`(Postgres 통합 레인)가 마이그레이션을 다 적용한 빈 DB 에서 각각
   실행한다. PostgreSQL 은 쿼리를 해석할 때 모든 열을 확인하므로 틀린 열 이름은 거기서 실패한다. 같은 시험이 옛 열 이름을 심어
   실패하는지도 본다. 실행 중 쿼리 오류는 이제 `warn` 로그를 남긴다.
6. **선언을 지키는 시험**(`orchestration/tests/test_source_expected_cadence.py`): 수집하는 엔드포인트는 모두 선언하고 수집 안 하는
   것은 안 한다; data.go.kr 파일 데이터의 `release` 는 그 계약의 `published` 와 같다; 온비드 나이 한계는 가장 느린 수집 간격의
   두 배 이상; 법원경매 실행 한계는 작업의 `time_limit_minutes` 와 같다.

이 가드들이 막는 실제 사고: 수집 경보 세 개가 처음부터 한 번도 울리지 못한 것(없는 열), 그리고 열만 고쳤을 때 건강한 원천 85 개가
울리는 것(한 줄 임계값).

## Consequences

- 배포 뒤 계산(2026-11-07 운영 DB 기준): 오래됨 경보가 울릴 원천은 85 → 1 개(법원경매 낙찰결과, 실제로 10-19 이후 수집 안 됨 —
  법원 사이트 거부, W8c). 느림 경보는 2 → 0 개. 실패 경보는 0 개(마지막 실행이 모두 성공).
- 공급자 주기가 바뀌면 카탈로그의 `expected_cadence` 를 실측 근거와 함께 고친다. 새 원천을 수집하면 시험이 선언을 요구한다.
- `catalog.source_catalog.collection_frequency`(수집기가 쓰는 자유 문장)는 표시용으로 남는다. 경보는 읽지 않는다.
- 한계: 한 번도 수집되지 않은 원천(실행 기록 없음)은 `last_finished` 시계열이 없어 오래됨 경보가 맞추지 않는다. 그것은 ADR-0400 의
  수집 여부 판정과 준비도 표(ADR-0442)가 본다.
