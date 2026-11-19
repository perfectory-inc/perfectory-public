# ADR 0567: 필지 주변 Gold 는 DuckDB 의 정확한 최근접으로 판을 쌓는 표로 태어나고, 같은 거리는 시설의 키가 정한다

- Status: Accepted
- Date: 2026-11-14
- Builds on: [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)
  §9 차례 2(옛 표 없이 태어나는 첫 표), [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)
  §3 예외 ⓑ(값이 다른데 새 엔진이 옳은 경우), [ADR-0560](./0560-the-publisher-commits-duckdb-staged-parquet-with-iceberg-rust-fast-append-and-only-tables-whose-commit-is-an-append-move-off-spark.md)(추가 커밋),
  [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(첫 발행의 자동 승격), [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분 처리)
- Amends: [ADR-0465](./0465-a-parcel-shows-what-is-near-it-computed-in-gold-with-sedona-and-h3.md) 의 계산 엔진(Apache Sedona `ST_KNN`·H3 함수)과 표 이름(`gold.parcel_nearby`).
  문서의 모양(레지스트리 주제 `parcel_nearby_by_pnu` 판 1)과 무엇을 담는지는 그대로다.
- 결정한 사람: 사장(2026-11-12, "DuckDB 의 주변 결과를 발행한다"). 같은 거리의 순서를 키로 정하는 것은 2026-11-14 필지 계보에 승인한 것과 같은 부류다.

## Context

**Spark 생산자는 한 번도 커밋하지 못했다.** `parcel_nearby_silver_to_gold.py`(Sedona `ST_KNN` k = 1, 회전타원체 거리)의 전국 첫 빌드가
2026-11-12 19:42Z, 11-13 00:15Z, 05:21Z, 14:22Z 네 번 모두 유닛 상한 160 분에 `start operation timed out` 으로 끝났다. 자동 승격의
gold 단계였으므로 매번 패널 재구축 잠금을 160 분 쥐었고, 그동안 `gold.parcel_panel` 재구축은 `waiting` 만 반복했다(W83b #1396).
`gold.parcel_nearby` 는 아직 없고 레인은 아무것도 서빙하지 않는다.

**DuckDB 로 쟀다(W81b, 2026-11-13, ai-server 읽기 전용, 상한 걸린 컨테이너).**

| | 행 | 시간 | 최고 메모리 |
|---|---:|---:|---:|
| DuckDB 전국(시도 하나씩, 4 스레드, DuckDB 한도 5 GB) | 39,861,511 | 445.6 s | 5.01 GiB |
| Spark+Sedona 경기 | 5,208,587 | 1,766 s | 6.88 GiB |
| DuckDB 경기 | 5,208,587 | 260 s | 3.84 GiB |

**판정(경기, 같은 핀).** 두 쪽 행 5,208,587, 한쪽에만 0. 문서가 다른 필지는 모두 시설 하나의 최근접이 다른 경우이고, 다른 값 하나하나가
둘 중 하나다: ⓐ DuckDB 쪽이 더 가깝다(최대 216 m), ⓑ 같은 거리의 두 시설 가운데 Sedona `ST_KNN`(k = 1)이 아무 쪽이나 고른 동률. Spark 쪽이
더 가까운 경우는 0 이다. `ST_KNN` 은 공간 색인의 근사 이웃을 내므로 가장 가까운 시설을 놓칠 수 있고, k = 1 은 동률에서 하나를 정하지
않는다. ADR-0559 §3 의 `equal` 이 아니고, 새 엔진이 옳은 예외 ⓑ 다. 표는 발행된 적이 없으므로 새 엔진의 결과로 첫 발행을 한다(같은 절).

## Decision

1. **표는 `gold.parcel_nearby_versions` 로 태어난다(ADR-0564 §1·§2).** `gold.parcel_nearby` 의 칸 + `version_seq`·`row_state`,
   `LatestPerKey(pnu, version_seq)`, `truncate(2, pnu)`. 계약은 종류 줄의 `versions_table: true` 에서 `by_pnu_kind::derive` 가 만들고, 줄의
   `gold_table` 이 레인이 굽는 표를 이것으로 정한다. `gold.parcel_nearby` 는 칸을 물려주는 계약으로만 남고 만들지 않는다(그래프 `contract_only`).
2. **생산자는 발행기 명령 하나다: `build-parcel-nearby-versions`**(작업 `gold_parcel_nearby_rebuild`, 유닛 `foundation-gold-versions@parcel-nearby`).
   발행기 안의 DuckDB(`spatial` 확장, 릴리스가 실어 경로로 로드 — `duckdb-extensions.contract.json`)가 계산하고
   (`parcel_nearby_stage`: `nearest.sql`·`document.sql`), 발행기가 ADR-0564 §3 의 길로 붙인다(`gold_versions_commit`: 기준 커밋에서 계획,
   머리의 현재 행과 `row_digest` 로 차분, 바뀐 키만 한 번의 추가 커밋, 머리가 아직 그 스냅샷일 때만, 그 뒤 빌드·입력 태그). Spark 세션은 없다.
   표가 없으면 첫 run 이 계약대로 만든다(`LakehouseDataCommitter::create_table_if_absent`): 배포의 lakehouse 이전은 없는 표를 그 적재에 남긴다.
3. **최근접은 정확하다.** 칸(0.05°, 0.005°)마다 그 중심에서 가장 가까운 시설 + 칸의 대각선 + 2 m 안의 시설만 후보로 남긴다(삼각부등식:
   칸 안 어느 필지의 최근접도 그 안에 든다). 거리는 WGS84 회전타원체(`ST_Distance_Spheroid`)이고, 확장이 축 순서를 잘못 읽으면 서울시청 →
   부산시청이 325 km 가 아니어서 run 이 거부한다.
4. **같은 거리는 시설의 키가 정한다**(사장 2026-11-12, ADR-0559 예외 ⓑ의 기록). 가장 가까운 거리(반올림 전), 그 다음 시설의 `row_key`
   (공급자 행의 안정한 키) 오름차순이다. 같은 키의 두 현재 행이 동률이면(계약이 동률을 남기는 도시철도역) 시설의 이름 칸이 마지막에 정한다.
   그래서 같은 입력은 언제나 같은 문서를 낸다.
5. **부분 처리(ADR-0314, scope `changed-keys`).** 입력·서빙 판·코드 판이 기준 커밋의 것과 같으면 "할 일 없음". 시설 파일 다섯·고속도로
   파일·법정동 변경 표가 움직이면 모든 시도를 다시 계산한다(어느 필지의 최근접도 바뀔 수 있다; 이 파일들은 14 ~ 12,004 행이고 한 해 몇 번
   바뀐다). 필지 경계·상가·place id 가 움직이면 닿은 시도만 다시 계산한다(`changed_sidos`: 경계나 place id 가 다른 필지, 상가가 떠나거나
   들어온 H3 칸의 두 고리 안 필지). 어느 쪽이든 붙는 것은 `row_digest` 가 달라진 키뿐이다.
6. **관문(커밋 전, 모두 DuckDB).** 형식이 틀린 PNU·섹션 없는 행 0, 서빙 필지마다 문서 하나(ADR-0272), 현재 행이 기준보다 종류 Gold 의
   `max_row_loss_fraction`(1%) 넘게 줄면 거부, 한 커밋의 변경이 현재 행의 50% 를 넘으면 코드나 서빙 판이 움직인 경우가 아닌 한 거부
   (ADR-0346 의 설명된 큰 변경). 지문과 현재 행 수는 기준 커밋의 값에 더하고 빼서 갱신한다(ADR-0564 §5).
7. **첫 발행은 자동 승격이 한다(ADR-0435).** 컷오버의 gold 단계는 레인의 표가 발행기가 짓는 `_versions` 표면 Spark 재구축 대신 그 레인 작업의
   유닛(`foundation-gold-versions@<lane>`)을 시작하고 끝까지 기다린다(같은 유닛이라 스케줄러가 돌리는 run 과 겹치지 않는다). 번들 Gold 와의
   대조(parity)는 없다. 입력 준비(`gold_input_readiness.py`)는 그래프가 그 표에 잇는 표 전부를 기다린다.
8. **Spark 생산자는 지운다.** `parcel_nearby_silver_to_gold.py` 와 그 시험을 지우고 엔진 장부 줄을 `moved` 로 바꾼다(판정 행 5,208,587).
   Spark 재구축 계약은 생산자가 Spark 를 떠난 종류를 짓지 않는다(`gold_rebuild.producer_left_spark`).

## Consequences

- 레인의 문서 `source.table` 은 `gold.parcel_nearby_versions` 다. 문서 스키마는 그대로다(레지스트리 주제 판 1). 그 필드 목록은 Rust 시험이
  레지스트리 스키마와 대조한다(`a_document_carries_exactly_the_registry_s_fields`).
- 릴리스가 `spatial`(108 MB) 확장을 싣는다. 모든 DuckDB 단계가 같은 네 확장을 로드한다.
- H3 칸(해상도 9)과 두 고리는 발행기가 h3o(H3 라이브러리의 Rust 구현, BSD-3, Sedona `ST_H3CellIDs` 와 같은 H3 v4)로 센다.
  DuckDB 의 커뮤니티 h3 확장은 경로로 로드할 때 서명이 인정되지 않아 `allow_unsigned_extensions` 를 켜야만 로드된다(릴리스 빌드의 오프라인
  로드가 거부했다, 2026-11-14). 서명 검사를 끄는 대신 칸 계산을 발행기로 옮겼다.
- 유닛 상한 60 분, 메모리 상한 8G(측정 최고 5.01 GiB 의 1.5 배 위). DuckDB 버퍼는 유닛에 남은 메모리의 5 분의 3 이다.
- 풀은 spark 3 칸 그대로다: 엔진과 오케스트레이션을 한 PR 에서 바꾸지 않는다(작업 이동 W82b). 메모리 계약의 슬롯 계산이 Spark 작업과
  겹치지 않게 지킨다.
- 굽기는 판을 쌓는 표를 그 규칙대로 읽는다(ADR-0564 §4·§5): 팩 굽기의 스캔은 읽은 행에 계약의 현재 행 규칙을 적용하고
  (`LakehouseTableContract::current_rows`), Gold 의 행 수는 카탈로그가 기록한 `foundation.current-records` 다
  (`load_snapshot_record_count`). 반영한 스냅샷과 새 스냅샷 사이의 변경 집합은 발행기 명령 `export-gold-versions-change-set` 이
  `version_seq` 범위로 낸다(지운 키는 반영한 스냅샷이 서빙하던 것만). Spark `by_pnu_panel_delta.py` 는 이 레인에서 돌지 않는다.

## 하지 않은 것, 확인하지 못한 것

- 전국 판정은 경기 한 시도다(W81b). 나머지 시도는 같은 SQL 이고, 전국 run 은 이 PR 의 첫 운영 run 이 낸다.
- 동률 규칙은 Sedona 결과와 견주지 않았다: Sedona 의 동률은 순서가 없어 견줄 기준이 없다.
- 설계 검토: 이 세션에는 `/egm:egm-review` 명령이 없다.
