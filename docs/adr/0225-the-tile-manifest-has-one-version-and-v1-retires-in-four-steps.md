# ADR 0225: 타일 매니페스트는 한 버전(v2)만 남기고, v1 은 네 단계로 은퇴한다

- Status: Accepted
- Date: 2026-10-14
- Related: [FP-ADR-0004](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) (v2 런타임 매니페스트), [FP-ADR-0008](../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md) (마커 앵커가 v1 에 남는다고 적은 곳), [ADR-0021](./0021-an-unread-surface-is-deleted-not-migrated.md) (아무도 읽지 않는 표면은 지운다), [ADR-0194](./0194-retire-the-five-postgres-copies-that-r2-already-holds.md) (`parcel_marker_anchor` 0행)

## Context

타일 매니페스트가 두 버전으로 살아 있다. 같은 질문("지금 어느 타일을 그리나")에 답하는 계약이 둘이라는 뜻이다.

| | v1 (`schema_version` 1) | v2 (`schema_version` 2) |
|---|---|---|
| 계약 타입 | `VectorTileManifestResponse` | `VectorTileRuntimeManifestResponse` |
| 둘을 가르는 곳 | `VectorTileManifestDocument` (`foundation-contracts` `catalog.rs`) | 같은 곳 |
| 라우트 | `GET /catalog/v1/vector-tiles/manifest`, `manifest:promote`, `manifest:rollback` | `GET /catalog/v1/vector-tiles/runtime-manifest` |
| 저장 | `catalog.vector_tile_manifest`, `catalog.vector_tile_artifact` | `catalog.vector_tile_runtime_manifest*`, `vector_tile_publication_unit`, `vector_tile_release*` |
| 타일 주소 | 평평한 PBF 객체 접두사(`{object_key_prefix}`) | 단위별 완전한 소스(Martin `dynamic_postgis` 또는 `static_pmtiles`) |

### 운영 실측 (2026-10-14, ai-server 읽기 전용, AWS 는 보지 않음)

- `GET /catalog/v1/vector-tiles/manifest` → **404** `active vector tile manifest`.
- `catalog.vector_tile_manifest` **0행**, `catalog.vector_tile_artifact` **0행**, `catalog.parcel_marker_anchor` **0행**.
- `GET /catalog/v1/vector-tiles/runtime-manifest` → 200, `manifest_generation` 13, 단위 셋
  `admin`·`complex`·`parcels` 모두 `static_pmtiles`.
- `/metrics` 의 `foundation_api_http_requests_total` 에 v1 경로 호출은 이 조사의 확인 호출 1건뿐이다(마지막 재시작 이후).

### 코드 실측

- v1 을 쓰는 명령줄 생산자는 마커 승격 명령 둘뿐이다: `promote-parcel-marker-anchor-pbf-manifest`,
  `promote-parcel-marker-anchor-runtime-manifest` (`parcel_marker_anchor_pbf_manifest_promote.rs`). 예약 작업·DAG 에
  걸린 적이 없고, 운영 v1 표가 0행이므로 **운영에서 돈 적이 없다.** 나머지 v1 생산자는 HTTP `manifest:promote` 하나다.
- 웹(`products/gongzzang/apps/web`):
  - 필지·행정경계·산단은 v2 단위를 먼저 읽고, 없을 때만 v1 로 내려간다(`listing-map-runtime.ts`).
  - **마커는 v1 만 읽는다**(`marker-tile-style.ts` 가 `parcel_anchor_aggregate`·`parcel_anchor` 를 v1 `artifacts` 에서
    찾는다). 운영 v1 이 404 이므로 마커 층은 지금 그려지지 않는다.
  - `PNU ?? pnu` 는 v1 필지 클릭(`registerLegacyParcelClick`)과 `app/dev-x9-test` 두 곳에만 있다. v2 `parcels` 단위는
    `feature_id_property` 로 소문자 `pnu` 하나만 낸다.
- v2 에 마커를 넣는 길이 v1 과 다르다.
  - v2 소스는 Martin `dynamic_postgis` 와 `static_pmtiles` 둘뿐이다. 평평한 PBF 객체 접두사는 v2 소스가 될 수 없다.
  - 단위의 첫 발행은 동적이어야 한다(`PromoteTileLayerStaticCommand` 의 `expected_active_release_id` 가 필수).
    동적 승격은 그 단위의 `serving_postgis.spatial_projection_load` 성공 행을 요구한다(`vector_tile_runtime_promote.rs`).
  - `UnitPromotion` 은 한 단위에 한 레이어다. v1 은 마커 두 레이어(집계 z0–11, 정확 z12 이상)를 한 매니페스트에 담았다.

## Decision

### 1. 불변식: 타일 매니페스트 버전은 하나다

네 단계가 끝나면 `schema_version` 1 을 쓰는 생산자, 읽는 소비자, 그 라우트와 타입이 저장소에 없다. 지도에 무엇을
그릴지는 `GET /catalog/v1/vector-tiles/runtime-manifest`(v2)만 답한다.

### 2. 순서와 각 단계의 관문

한 단계가 한 PR(또는 한 묶음)이다. 앞 단계가 운영에서 확인되기 전에는 다음 단계를 병합하지 않는다.

| 단계 | 하는 일 | 운영 관문(다음 단계로 가는 조건) | 되돌리기 |
|---|---|---|---|
| 1. 마커가 v2 로 | 1a v1 마커 승격 명령 둘을 지우고, v1 승격 유스케이스를 만드는 곳이 HTTP 경로 하나뿐임을 시험으로 고정한다(이 PR). 1b 마커를 v2 단위 둘(`parcel_anchor_aggregate` z0–11, `parcel_anchor` z12 이상, FP-ADR-0008 의 줌 상수)로 첫 동적 발행: 앵커 투영 적재와 `spatial_projection_load` 기록, Martin 소스, `promote-parcel-anchor-aggregate-runtime`·`promote-parcel-anchor-runtime`. 1c 같은 두 단위를 `bake-lakehouse-tiles` 로 정적 승격. 1d 웹 `marker-tile-style.ts` 가 v2 단위를 먼저 읽는다. **v1 대체 경로는 2단계까지 남긴다.** | runtime-manifest 에 두 마커 단위가 `static_pmtiles` 로 있고, `tiles.perfectory.io` 에서 두 단위의 표본 타일이 200 | 1a: PR 되돌리기(운영에서 쓰인 적 없음). 1b·1c: 단위 rollback-serving 또는 웹이 아직 안 읽으므로 그대로 둬도 무해. 1d: 웹 PR 되돌리기 |
| 2. 읽는 쪽은 v2 만 | 웹의 `fetchVectorTileManifest`·`VectorTileManifestSchema`·`buildVectorTileSource` 와 `listing-map-runtime.ts` 의 v1 대체 분기, `marker-tile-style.ts` 의 v1 경로를 지운다. 서버 쪽에는 v1 을 읽는 운영 코드가 없다(`VectorTileManifestDocument` 는 계약 시험만 쓴다) | 웹 출시 뒤 ai-server `/metrics` 에서 v1 GET 경로 카운터가 7일 동안 확인 호출 외에 늘지 않는다 | 웹 PR 되돌리기. v1 라우트는 이 단계에서 아직 살아 있다 |
| 3. v1 라우트·타입 삭제 | `GET manifest`·`manifest:promote`·`manifest:rollback`, v1 계약 타입과 `VectorTileManifestDocument` 의 V1 갈래, v1 승격·되돌리기 유스케이스와 저장소 구현, OpenAPI 두 사본과 pin 해시, dawneer 생성 타입(`catalog.d.ts`), 트래픽·인증 등록부와 gongzzang 호출 행렬의 v1 경로, 마커 계약의 `runtime_manifest_endpoint`(→ runtime-manifest), 로컬 시드 `local_vector_tile_manifest.sql`, k6 `foundation-read-smoke.js`, `.env.example` 주석, v1 전용 평평한 PBF 빌드(`parcel_marker_anchor_pbf_artifact_build*`). v1 표 DROP 은 별도 마이그레이션 PR 이다 | 지운 release 가 운영에 깔렸고 `/readyz` 200, runtime-manifest 200. DROP 마이그레이션은 v1 두 표가 여전히 0행일 때만 | 서버 PR 되돌리기(자동배포가 이전 release 로). 표는 DROP 전까지 남아 있으므로 데이터 손실 없음. DROP 이후는 pgBackRest 복원(내용은 0행) |
| 4. `PNU ?? pnu` 삭제 | `listing-map-runtime.ts` 와 `dev-x9-test` 의 대문자 `PNU` 대체를 지운다. 2단계에서 v1 분기가 사라지면 남는 자리는 `dev-x9-test` 뿐이다 | runtime-manifest 의 `parcels` 레이어 `feature_id_property` 가 `pnu` | 웹 PR 되돌리기 |

### 3. 운영 확인은 ai-server 와 Cloudflare 에서만 한다

AWS 의 운영 공짱(웹·API)은 보지도 바꾸지도 않는다. 단계 사이 확인은 모두 읽기 전용이다.

- ai-server 호스트에서 `curl http://127.0.0.1:18080/catalog/v1/vector-tiles/manifest`(1–2단계는 404 여야 한다),
  `.../runtime-manifest` 의 단위·소스 종류.
- ai-server `/metrics` 의 `foundation_api_http_requests_total{route="/catalog/v1/vector-tiles/manifest"}`.
  카운터는 재시작에 초기화되므로 출시 직후와 7일 뒤 두 번 읽고 재시작 여부를 함께 적는다.
- 운영 DB(`foundation`) 의 `catalog.vector_tile_manifest`·`catalog.vector_tile_artifact` 행 수.
- 마커 단위 타일은 `tiles.perfectory.io` 에서 `curl` 표본.

웹 단계(1d·2·4)는 병합해도 공짱 웹 운영 배포가 있어야 켜진다. 그 배포는 사용자가 한다. 그 전까지 운영 웹은 지금처럼
v2 를 먼저 읽고, 운영 v1 이 404 이므로 v1 분기는 아무것도 그리지 않는다.

### 4. FP-ADR-0008 의 "두 anchor layer 는 고정된 schema-v1 manifest 에 남는다"를 대체한다

마커 앵커의 출시 경로는 v2 publication unit 이다. FP-ADR-0008 의 나머지(PNU 앵커, 줌 경계, bbox 금지)는 그대로다.

## 기각한 대안

### 마커를 v1 그대로 두고 v1 을 영구 유지한다

두 버전을 함께 두면 매니페스트를 읽는 모든 곳(웹·dawneer·서버)이 두 갈래를 계속 시험하고 지켜야 한다. 운영 v1 은
0행이라 그 비용으로 지키는 것이 없다.

### v1 을 지금 한 번에 지운다 (ADR-0021 그대로)

운영에서 읽히는 것이 없으므로 가능은 하다. 그러나 웹 출시는 AWS 쪽이라 우리가 시점을 정하지 못하고, 아직 출시되지 않은
웹 빌드가 v1 을 부를 수 있다. 서버를 먼저 지우면 그 빌드의 마커·대체 분기가 404 대신 다른 실패를 낸다. 웹이 v2 만
읽는 것을 먼저 운영에서 확인한다(2단계 관문).

### 평평한 PBF 마커 객체를 v2 의 새 소스 종류로 받는다

v2 소스 합집합에 세 번째 종류를 더하면 웹·Worker·스키마가 모두 새 갈래를 배워야 한다. 마커는 점 데이터라 다른 단위와
같은 PMTiles 굽기로 낼 수 있다.

### 마커 두 레이어를 한 v2 단위에 담는다

`UnitPromotion`·동적 승격·`bake-lakehouse-tiles` 가 모두 한 단위에 한 레이어를 전제한다. 단위 둘이 기존 경로를 그대로 쓴다.

## Consequences

- 1a 이후 v1 매니페스트를 새로 쓸 수 있는 곳은 HTTP `manifest:promote` 하나다.
  `catalog-application/tests/v1_manifest_promotion_has_one_caller.rs` 가 그 밖에서 v1 승격 유스케이스를 만드는 파일을
  찾으면 실패한다. 3단계에서 그 경로와 함께 이 시험도 지운다.
- 1b 는 ADR-0194 와 만난다. ADR-0194 는 PostGIS 사본을 줄이는 방향이고, 1b 의 동적 첫 발행은 앵커 투영을 PostGIS 에
  둔다. 첫 발행 직후 1c 로 정적판에 옮기므로 PostGIS 앵커 투영은 굽기 입력과 되돌리기 대상으로만 남는다. 1b 를 시작할 때
  운영 `parcel_marker_anchor` 가 아직 0행이면 ADR-0194 §5.3(앵커를 R2 Silver 에서 다시 만든다)을 먼저 돌린다.
- 남은 부채: 2단계의 웹 출시 시점은 사용자 결정이다. 7일 관측은 그 출시 뒤부터 센다.

> 2026-10-19 주석: 2단계(웹이 v1 을 읽지 않음)를 1b 보다 먼저 냈다. 표의 "1b 는 웹이 아직 안 읽으므로 무해"는 틀렸다:
> 웹은 등록부에 없는 단위가 하나라도 있으면 runtime-manifest 전체를 버리고 v1 으로 내려갔고, 운영 v1 은 활성본이 없어
> 404 라 1b 가 마커 단위를 올리는 순간 지도가 비었을 것이다. 그래서 웹은 등록부에 없는 단위를 건너뛰고(나머지 단위는
> 그린다) v1 요청을 지웠다. 앵커 마커 층은 운영에서 그려진 적이 없으므로(v1 0행) 잃는 것은 없고, 1b·1c 로 v2 단위가
> 생기면 1d 가 등록부에 두 단위를 더한다. `NEXT_PUBLIC_TILES_MANIFEST_URL` 은 v1 이 아니라 CSP `connect-src` 에 타일
> 호스트를 넣는 데만 남는다.
