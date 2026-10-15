# ADR 0240: 산업단지 경계의 새 판은 예약 작업이 덧붙이고, 읽는 쪽은 가장 새 판을 쓴다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(산단 경계는 레이크하우스에서 굽는다),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(입력이 바뀌면 시작),
  [ADR-0234](./0234-the-flow-map-says-job-missing-only-for-data-that-should-have-a-job.md)(`needs_job` 이던 칸)

## Context

사장 결정(2026-10-14): 산업단지 경계에 예약 작업을 둔다. ADR-0234 가 이 칸을 `needs_job` 으로 적었다. 확인한 것:

- `silver.industrial_complex_boundaries` 는 1,343 행, 판 하나(`vworldkr__sandan_boundary-30137-1`)다. 2026-09-01 커밋이
  적재 기록 `foundation.ingest-batch-objects=bronze/source=vworldkr__sandan_boundary/30137-1.zip` 을 가진다. 런북은 없다.
- 원천은 ZIP 하나(4 MB)이고 매일 수집 대상이 아니다(장부 마지막 수집 2026-07-02, 제공자 갱신 2026-04-07).
- Silver 적재기는 객체마다 한 번만 덧붙이고(`append_batch_once`) 앞 판을 그대로 둔다. 그런데 읽는 쪽
  (`current_official_boundaries`: 산단 서빙본 Gold 와 PostGIS 투영)은 `valid_to_utc IS NULL` 인 행을 모두 현재로 본다. 새 판이
  들어오면 단지마다 경계가 둘이 되고, 두 읽기가 "한 단지에 공식 경계 둘"로 거부한다. 즉 새 판은 지금 자동으로 들어갈 수 없다.
- 앞 판의 `valid_to_utc` 를 닫는 UPDATE 는 행을 고쳐 쓰는 것이다. 데이터는 덮어쓰지 않고 쌓는다(사장 지시). 행정경계가
  같은 문제를 "Silver 는 회차를 다 갖고, 지도는 가장 새 회차를 쓴다"로 이미 풀었다.

## Decision

1. **현재 = 가장 새 판.** `current_official_boundaries` 는 `valid_to_utc IS NULL` 이고 공식인 행 중 가장 새 `source_snapshot_id`
   (판의 가장 늦은 `valid_from_utc`, 같으면 큰 id; `newest_source_snapshot`) 의 행만 낸다. 두 읽기가 이 함수 하나를 쓴다. 앞 판의
   행은 고치지 않는다.
2. 매일 수집이 `sandan_boundary` 를 받는다(`daily_collection: source_sweep`).
3. 예약 작업 `complex_boundary_release`(`scripts/ops/complex-boundary-release.sh`, spark 3 자리, `started_by: inputs`, 대체
   09:50 UTC)가 가장 늦게 올라온 ZIP 을 읽기 키로 받아 발행기로 바꾸고 Spark 로 덧붙인다. 적재 기록이 그 ZIP 을 이미 가지면
   `unchanged`. 바뀌었으면 매시 접기의 스크립트를 FORCE 로 불러 산단 타일을 굽는다.
4. 작업은 꺼진 채 등록되고, 감독 실행은 `by-pnu-pack-operator.sh job start complex_boundary_release`(ADR-0235)다. 그래프의
   `needs_job` 은 지운다(ADR-0234 가드).

## Consequences

- 산단 경계의 새 판이 사람 손 없이 Silver·타일까지 간다. Silver 는 판을 모두 갖는다.
- 산단 프로필(`silver.industrial_complexes`, ILIS 원천)은 이 작업 밖이다 — 그 칸은 `on_event` 로 남는다. 새 경계가 프로필에 없는
  단지 코드를 가지면 적재기의 기존 집계(`orphan_boundary_count`)에 남는다.
- PostGIS 투영(`publish-industrial-complex-boundary-postgis`)은 이 작업이 돌리지 않는다. 지도는 레이크하우스 타일을 읽는다.
- 시험: `test_industrial_complex_boundaries_silver_to_postgis_handoff.py`(가장 새 판), `orchestration/tests/test_complex_boundary_release.py`
  (새 판·이미 가진 판·ZIP 없음·대답 없는 적재·Bronze 밖 키). 고르기를 이름 순으로 바꾸면 시험이 실패한다.
