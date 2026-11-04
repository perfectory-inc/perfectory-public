# ADR 0413: 필지 앵커 마커 타일 파이프라인을 은퇴시키고, 나중에 기존 타일 bake 의 클러스터 단위로 다시 만든다

- Status: Accepted
- Date: 2026-11-04
- Amends: [FP-ADR-0008](../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md)
  (PNU 앵커 PBF 마커 타일 계약), [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §8,
  [ADR-0194](./0194-retire-the-five-postgres-copies-that-r2-already-holds.md) §3
- Builds on: [ADR-0200](./0200-the-outbox-publisher-is-split-into-lane-library-crates.md) (출판기 레인 크레이트)

## Context

사장 지시(2026-11-04): 바퀴를 다시 만들지 말고, 덕지덕지 붙은 것을 모두 걷어내고, 근본만 고친다.

`crates/publisher/foundation-publisher-parcel-anchor` 는 필지 마커 앵커를 손으로 짠 경로로 만들었다.
앵커 JSONL artifact 내보내기, 앵커 PBF 타일 굽기(줌별·집계), R2 Silver 에서 앵커를 다시 만드는 스트리밍 재구성,
그리고 그 증거를 검사하는 `check-postgis-anchor-pbf-regional-proof`·`check-regional-data-serving-load` 가 있었다.
2026-11-04 전 트리(`git grep`, 모든 언어, `orchestration/jobs.v1.json`·DAG·systemd·scripts·`.github/workflows` 포함)를 확인했다.

| 무엇 | 부르는 곳 |
|---|---|
| 명령 `export-parcel-marker-anchor-artifacts`, `build-parcel-marker-anchor-pbf-artifacts`, `build-parcel-marker-anchor-aggregate-pbf-artifacts`, `rebuild-parcel-marker-anchors-streaming` | 시험뿐. 정기 작업·스크립트·CI 없음 |
| 명령 `check-postgis-anchor-pbf-regional-proof`, `check-regional-data-serving-load` | 시험뿐. 증거 파일은 역시 시험만 부르는 두 관문(`check-national-data-collection-rollout-approval`, `check-bounded-live-ingestion-gate`)이 읽었다 |
| `gold/parcel-marker-anchors/`·`gold/parcel-marker-anchor-*` 객체 | 어떤 bake·작업도 읽지 않는다. R2 감사·정리 코드만 접두사를 안다 |
| 웹 지도 | `products/gongzzang/apps/web/lib/map/marker-tile-style.ts` 가 층 id 상수만 둔다. 그 층은 지도에 없다 |

`check-regional-data-serving-load` 는 앵커 타일 라우트(`/map/v1/marker-tiles/parcel_anchor/…`)를 날 TCP 로 찔러 보는 검사였다.
그 대상이 죽은 파이프라인이므로, 날 TCP 를 reqwest 로 바꾸는 대신 검사째 지운다.

`parcel_boundary_handoff` 는 같은 크레이트에 있었지만 마커와 무관하고 살아 있다.
`rebuild-postgis-parcel-boundary-mirror-national` 이 쓰고, 그 표는 `parcel_boundary_postgis_publish` 가 읽는다.

마커 클러스터 타일은 표준 도구가 이미 한다. tippecanoe 의 `--cluster-distance`(점을 거리로 묶고 `point_count` 를 단다)는
Mapbox·Felt 가 쓰는 방식이고, 이 저장소의 타일 bake(`bake-lakehouse-tiles`, `foundation-publisher-tiles/src/lakehouse_tile_bake.rs`)는
이미 고정된 tippecanoe 컨테이너로 굽는다.

## Decision

1. 크레이트 `foundation-publisher-parcel-anchor` 와 그 명령 4개, 검사 명령 2개(`postgis_anchor_pbf_regional_proof_check.rs`,
   `regional_data_serving_load_check.rs`)를 지운다. 두 관문의 `PRIOR_EVIDENCE`·`next_gates` 와
   `silver_gold_national_promotion_execution` 의 `next_gates`, `national-data-normalization-contract.v1.json` 의
   앵커 `derived_outputs` 에서 그 이름을 뺀다. 그 명령들만 쓰던 "큰 스택 스레드" 경로(`command_requires_expanded_stack`)도 지운다.
2. 살아 있는 `parcel_boundary_handoff` 는 `services/foundation-outbox-publisher/src/` 로 옮긴다(내용은 그대로).
3. 필지 마커는 나중에 **기존 타일 bake 의 한 단위**로 다시 만든다: 필지 대표점 층을 tippecanoe `--cluster-distance` 로 굽는다.
   새 크레이트·새 PBF 인코더·새 집계 코드를 만들지 않는다. 그때 그 단위는 bake 의 부분 처리 규칙(root ADR-0314)을 따른다.
4. 이번에 지우지 않고 남기는 것(각자 후속 PR):
   - Postgres 미러에서 앵커를 다시 만드는 경로 — 명령 `rebuild-parcel-marker-anchors`, `POST /catalog/v1/parcel-marker-anchors:rebuild`,
     `RebuildParcelMarkerAnchors`·`PgParcelMarkerAnchorRebuilder`. ADR-0133 §8·ADR-0194 가 이미 지우라고 했다.
     OpenAPI 와 생성된 Gongzzang·Dawneer 클라이언트를 같이 바꿔야 해서 따로 한다.
   - DB 참조 마커 타일 라우트(`/map/v1/marker-tiles/…`, 기본 꺼짐)와 표 `catalog.parcel_marker_anchor`·`catalog.parcel_marker_anchor_generation_run`.
     표를 지우는 마이그레이션은 그 자체로 ADR 이 필요하다(AGENTS.md §스키마·제약 변경).
   - 이벤트 `catalog.parcel_marker_anchor.snapshot.published.v1` 의 스키마·수신자 계약과 Gongzzang 의 앵커 가져오기.
     생산자는 이제 없다. 소비자 쪽 정리는 Gongzzang 이 한다.
   - R2 감사·정리 코드의 `gold/parcel-marker-anchor*` 접두사. 남은 옛 객체를 지우려면 필요하다.

## Consequences

- 지운 코드: 크레이트 5,247줄(옮긴 `parcel_boundary_handoff` 485줄 제외)과 검사 두 개.
  워크스페이스 멤버가 하나 줄고, 출판기 실행 파일이 그 크레이트를 더는 링크하지 않는다.
- 국가 수집 출시 관문(`check-national-data-collection-rollout-approval`)은 앵커 증거 두 개를 더는 요구하지 않는다.
  그 증거를 만들 생산자가 없으므로, 남겨 두면 관문은 영원히 통과할 수 없었다.
- 웹 지도에 마커 층이 없으므로 사용자에게 보이는 변화는 없다.
- 마커가 다시 필요해지면 3항대로 bake 에 단위 하나를 더한다. 그 PR 이 이 ADR 을 인용한다.
