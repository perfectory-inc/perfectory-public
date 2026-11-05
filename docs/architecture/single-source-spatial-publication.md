---
status: current
owner: foundation
doc_type: architecture
last_reviewed: 2026-11-05
---

<!-- public-repository-safety: reviewed-public-contract -->

# 단일 출처 공간 데이터 공개 아키텍처

**상태:** 카탈로그 원장 모델은 구현되어 쓰인다. 공개 서빙 경로(Martin·5초 polling·첫 slice)는
이후 ADR이 대체했다 — 아래 [이후 바뀐 것](#이후-바뀐-것).
**범위:** Gongzzang이 소비하는 Foundation 소유 공개·기준 폴리곤 레이어

행정 코드·이름과 PNU 변경은 [행정구역 경계와 필지 식별자 버전 관리 계약](./administrative-boundary-versioning.md)을
따른다. 원자성은 `publication_unit` 단위로 보장한다. 여러 단위의 법적 동시성이 필요한 생산자는 두
단위를 하나의 완전한 매니페스트 후보로 준비해야 하며, 클라이언트는 부분 타일 소스를 조합하지 않는다.

이 문서가 아직 정본인 것은 **카탈로그 원장과 상태 모델**이다. 타일을 어떻게 굽고 내보내는지는
아래 ADR과 [필지 타일 굽기 런북](../../platforms/foundation-platform/docs/runbooks/parcel-tile-bake.md)이 소유한다.

## 1. 결정

각 publication unit(`catalog.vector_tile_publication_unit`)은 한 시점에 완전한 활성 release를 정확히
하나 선택한다. release의 source 종류는 둘이다(`ServingSourceKind`,
`platforms/foundation-platform/crates/catalog/catalog-domain/src/serving_publication.rs`).

- `DYNAMIC`: PostGIS serving 투영에서 렌더링하는 release.
- `STATIC`: 불변·release 주소 PMTiles 하나를 가리키는 release.

같은 단위의 두 표현을 동시에 내보내지 않는다. 클라이언트에서 feature를 지우거나 겹치는 조합은 하지
않는다. 바뀐 타일만 갈아 끼우는 조합은 서버에서 타일 단위로만 한다
([ADR-0111](../adr/0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md)).

## 2. 핵심 불변식

모든 `(publication_unit, serving_generation)`에 대해 다음을 보장한다.

1. tile 응답에는 각 논리 feature의 현재 표현이 0개 또는 1개만 있다.
2. 후보 source를 검증한 뒤에만 source 전환이 보인다.
3. 오래된 build가 더 최신인 리비전을 덮지 못한다 — build는 `superseded`로 끝난다.
4. rollback은 과거에 검증한 완전 source를 선택하며 서로 다른 source를 재조합하지 않는다.
5. PMTiles·PostGIS·cache·runtime manifest는 projection이지 정본 geometry나 공개 권위가 아니다.
   정본은 R2 + Apache Iceberg다.

### 리비전·적재·release의 세 원장

| 원장 | 답하는 질문 | 근거 |
| --- | --- | --- |
| `catalog.publication_revision` | 이 **단위**의 데이터가 어느 canonical 스냅샷인가 | [ADR-0017](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md) |
| `serving_postgis.spatial_projection_load` | 그 리비전이 **언제·몇 행으로** materialise 됐는가 | [ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) |
| `catalog.vector_tile_release` | 어느 적재 또는 아카이브를 **서빙**하는 불변 release인가 | [ADR-0013](../adr/0013-release-uniqueness-admits-both-source-kinds.md) |

리비전은 단위에 스코프된다. release와 적재 모두
`(data_revision, publication_unit_id, canonical_iceberg_snapshot_id)`로 참조하므로 **다른 단위의
리비전을 지명하는 것은 쓸 수 없다.** `catalog.administrative_boundary_revision`은 행정경계
**사실** 원장이며, 발행 리비전이 계보로 그것을 가리킨다 — 그 반대가 아니다.

## 3. 권위

| 사실 | 권위 | 파생 사본 |
|---|---|---|
| 정본 geometry 리비전·feature 식별자·계보 | Foundation R2 + Apache Iceberg | PostGIS 투영, PMTiles, MVT 속성 |
| 단위별 활성 release·제공 세대·rollback 이력 | Foundation Catalog 트랜잭션·CAS 상태 | 공개 매니페스트 |
| 전역 runtime manifest 세대 | Catalog 전역 매니페스트 포인터 | R2의 create-only 매니페스트 사본과 포인터 객체 |

R2에는 Catalog 매니페스트 상태의 두 projection이 있다. create-only
`gold/vector-tiles/manifests/{manifest_id}.json` 사본과 덮어쓰는
`gold/vector-tiles/runtime-manifest.json` 포인터다. 쓰는 쪽과 타일 엣지가 내는 경로는
[ADR-0254](../adr/0254-the-runtime-manifest-is-an-r2-object-the-tile-edge-serves.md)가 소유한다. 어느
것도 운영자가 활성 source를 직접 고르는 장소가 아니다.

## 4. 공개 상태 모델

활성 source와 build job은 서로 다른 사실이다.

```text
publication_unit
  data_revision                 # 공개용으로 선택한 정본 리비전
  serving_generation            # 활성 release가 바뀔 때마다 증가
  active_release_id             # 불변 완전 source 기술자
  fallback_release_id?          # 같은 data_revision의 검증된 source
  optimistic_version

runtime_manifest
  manifest_generation           # 전역 — 어느 단위가 바뀌어도 증가
  current_version               # 불변 매니페스트 UUID, ETag로 쓴다
  unit_selections[]             # 단위 -> release + serving_generation 스냅샷

runtime_manifest_pointer
  singleton                     # 정확히 한 행
  active_manifest_id

vector_tile_build_job
  status = planned | running | validated | promoted | superseded | failed
```

포인터는 Catalog compare-and-swap 함수 `catalog.promote_vector_tile_runtime_manifest`로만 전진한다.
최신 정의는 마이그레이션 디렉터리의 마지막 `CREATE OR REPLACE`가 정본이다.

`data_revision`은 공개 feature 내용이 바뀔 때 바뀐다. `serving_generation`은 같은 데이터라도
Catalog가 다른 완전 release를 선택할 때마다 바뀐다
([ADR-0014](../adr/0014-serving-generation-tracks-one-unit-source-selection.md)). 두 값은 의도적으로
다르다. build가 고정한 입력 release가 build 도중 교체되면 그 build는 `superseded`가 되고 승격되지
않는다.

### 4.4 Rollback

Serving rollback과 data rollback은 다른 작업이다.

- **Serving rollback**은 같은 `data_revision`의 보존·검증된 완전 release를 예상 활성 release CAS로
  다시 선택한다.
- **Data revert**는 과거 PostGIS 상태를 가리키지 않는다. 이전 변경을 의도적으로 되돌린 새 정본
  리비전을 만들고 일반 공개 흐름을 따른다. 이력은 추가 전용이다.

## 이후 바뀐 것

이 문서가 2026-07에 적은 공개 서빙 경로는 아래 결정이 대체했다. 각 사실은 그 ADR이 소유한다.

| 원래 내용 | 지금의 정본 |
|---|---|
| Martin(dynamic·static 두 배포)이 공개 타일을 낸다 | [ADR-0111](../adr/0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md) — 공개 서빙은 `foundation-tile-gateway` Worker, Martin은 굽기 도구로만 |
| 전국 규모는 출시 범위가 아니다 | [ADR-0110](../adr/0110-serve-all-map-tiles-from-r2-static-pmtiles.md), [ADR-0133](../adr/0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) — 전국 PMTiles를 굽는다 |
| 편집은 단위 전체를 dynamic으로 되돌린다 | [ADR-0111](../adr/0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md), [ADR-0323](../adr/0323-parcel-tiles-patch-only-the-tiles-changed-parcels-touch.md) — 바뀐 타일만 패치 |
| 매니페스트 v1/v2 공존 | [ADR-0225](../adr/0225-the-tile-manifest-has-one-version-and-v1-retires-in-four-steps.md) — v2 하나 |
| API가 Postgres에서 매니페스트를 답한다 | [ADR-0254](../adr/0254-the-runtime-manifest-is-an-r2-object-the-tile-edge-serves.md) — R2 객체, 엣지가 낸다 |
| 굽기 절차 | [ADR-0253](../adr/0253-the-parcel-tile-bake-runs-in-two-halves-and-is-rehearsed-before-an-edition-is-served.md), [ADR-0374](../adr/0374-a-tile-unit-may-be-born-from-its-first-lakehouse-bake.md) |
| 런타임 계약 | [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) |

첫 slice의 단계별 구현 계획(구 `docs/guides/single-source-spatial-publication-implementation*.md`)은
지운 코드를 서술하던 2026-07 계획서라 저장소에서 지웠다. 이력은 git에 있다.
