---
status: current
owner: foundation
doc_type: architecture
last_reviewed: 2026-07-30
---

<!-- public-repository-safety: reviewed-public-contract -->

# 단일 출처 공간 데이터 공개 아키텍처

**상태:** 승인된 방향, 구현 대기
**날짜:** 2026-07-24
**범위:** Gongzzang이 소비하는 Foundation 소유 공개·기준 폴리곤 레이어
**구현 순서:** [단일 출처 공간 데이터 공개 구현 안내서](../guides/single-source-spatial-publication-implementation.md)

행정 코드·이름과 PNU 변경은 [행정구역 경계와 필지 식별자 버전 관리 계약](./administrative-boundary-versioning.md)을
따른다. 원자성은 `publication_unit` 단위로 보장한다. 필지와 향후 행정구역 폴리곤에 영향을 주는
통합은 하나의 매니페스트가 두 단위를 함께 승격하기 전까지 서로 다른 검증된 리비전으로 보일 수
있다. 여러 단위의 법적 동시성이 필요한 생산자는 두 단위를 하나의 완전한 매니페스트 후보로
준비해야 하며, 클라이언트는 부분 타일 소스를 조합하지 않는다.
**최신성 SLO:** 활성 릴리스와 제공 세대가 커밋된 뒤 이미 열려 있는 지도에 공개 변경이 5초 안에
반영되어야 한다.

## 1. 결정

각 publication unit은 항상 완전한 active tile source를 정확히 하나만 가진다.

- `DYNAMIC`: Martin이 Foundation PostGIS serving mirror에서 현재 단위 전체를 렌더링한다.
- `STATIC`: Martin이 전용 private serving-derivative R2 bucket의 불변·버전 주소 PMTiles 하나를 제공한다.

출시 시 publication unit은 `parcels`, `complex`, `buildings` 같은 하나의 논리 계층이다.
같은 단위의 static과 dynamic 표현을 동시에 렌더링하지 않는다. 향후 ADR은 측정 결과가 근거가 될
때만 단위를 겹치지 않는 Web Mercator partition으로 줄일 수 있다.

이는 제안되었던 Foundation 전역 `static base + feature delta - tombstone` 조합을 폐기한다.
Martin composite source는 tile payload를 합칠 뿐 feature 제거·중복 제거·교체 우선순위를 제공하지 않는다.
그 의미를 Gongzzang에 구현하면 소비자가 Foundation 공개 정책을 복제하게 되고, custom gateway에
구현하면 별도 tile engine을 만들게 된다.

## 2. 문제와 핵심 불변식

핵심 문제는 PMTiles에 이미 들어간 폴리곤 하나를 숨기는 방법이 아니다. 불변 R2 우선 정본 이력을
보존하면서 논리적으로 현재인 지도 하나를 제공하고 즉시 수정을 허용하는 방법이다.

모든 `(publication_unit, serving_generation)`에 대해 다음을 보장한다.

1. tile 응답에는 각 논리 feature의 현재 표현이 0개 또는 1개만 있다.
2. 한 단위의 모든 tile은 하나의 완전한 active source에서 읽는다.
3. 후보 source를 검증한 뒤에만 source 전환이 보인다.
4. 오래된 static build가 더 최신인 dynamic 리비전을 덮지 못한다.
5. rollback은 과거에 검증한 완전 source를 선택하며 서로 다른 source를 재조합하지 않는다.
6. PMTiles·PostGIS·Martin cache·runtime manifest는 projection이지 정본 geometry나 공개 권위가 아니다.

Dynamic Martin URL은 의도적으로 안정적이며 query가 없다. Catalog의
`vector_tile_runtime_manifest_pointer`만 runtime selector이고 `serving_postgis.*_current` view가
선택된 release의 **projection 적재**를 경유해 join한다 —
`release.postgis_projection_revision = publication.projection_load_id`. 즉 타일이 잘려 나오는 행은
선택된 release가 **지명한 바로 그 적재**의 행이지, "그 리비전 아래 지금 저장된 무엇이든"이 아니다.
Static PMTiles route는 불변·release 주소이므로 CDN cache가 같은 path에서 다른 release 바이트를
반환할 수 없다.

### 리비전·적재·release의 세 원장

| 원장 | 답하는 질문 | 근거 |
| --- | --- | --- |
| `catalog.publication_revision` | 이 **단위**의 데이터가 어느 canonical 스냅샷인가 | [ADR-0017](../adr/0017-a-data-revision-belongs-to-the-unit-it-revises.md) |
| `serving_postgis.spatial_projection_load` | 그 리비전이 **언제·몇 행으로** materialise 됐는가 | [ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) |
| `catalog.vector_tile_release` | 어느 적재를 **서빙**하는 불변 release인가 | [ADR-0013](../adr/0013-release-uniqueness-admits-both-source-kinds.md) |

리비전은 단위에 스코프된다. release와 적재 모두
`(data_revision, publication_unit_id, canonical_iceberg_snapshot_id)`로 참조하므로 **다른 단위의
리비전을 지명하는 것은 쓸 수 없다.** `catalog.administrative_boundary_revision`은 행정경계
**사실** 원장이며, 발행 리비전이 계보로 그것을 가리킨다 — 그 반대가 아니다.

이 불변식으로 이중 렌더링, 오래된 geometry 부활, feature 단위 억제 garbage collection, static build
중 변경 유실을 제거한다.

## 3. Authority and SSOT

SSOT는 물리 저장소 하나가 아니라 사실마다 권위가 하나라는 뜻이다.

| Fact | Authority | Derived copies |
|---|---|---|
| Canonical geometry revision, feature identity, public lifecycle, and lineage | Foundation R2 + Apache Iceberg, mutated only through the Foundation Catalog contract | PostGIS mirror, PMTiles, MVT properties |
| Per-unit active release, serving generation, and serving rollback history | Foundation Catalog transaction/CAS state | Published manifest, Martin routes |
| Global runtime-manifest generation and ETag | Foundation Catalog global manifest pointer | create-only R2 manifest history, mutable R2 pointer projection, Gongzzang poll state |
| Tile bytes while dynamic | Complete PostGIS serving mirror through Martin | Martin/CDN caches |
| Tile bytes while static | Immutable PMTiles artifact selected by Catalog | Martin/CDN range cache |
| Listing visibility and marker state | Gongzzang listing write model | Gongzzang marker projections |

승인된 공개 변경은 serving generation을 공개하기 전에 Foundation 정본 리비전에 영속 등록하고
PostGIS로 투영해야 한다. PostGIS는 Foundation 정본 데이터에서 재구성 가능해야 하며 변경의 유일한
복사본이 되면 안 된다. 미승인 preview는 staff 전용 workspace를 사용할 수 있지만 공개 tile 계약
범위 밖이다.

R2에는 Catalog manifest 상태의 두 projection이 있다. create-only
`gold/vector-tiles/manifests/{manifest_id}.json` 이력과 재생성 가능한 no-cache
`gold/vector-tiles/runtime-manifest.json` active pointer다. 어느 것도 운영자가 active source를 직접
고르는 장소가 아니다. 변경 가능한 pointer는 R2 ETag compare-and-swap(`If-Match`, bootstrap에서는
`If-None-Match: *`)로 갱신하며 DB 확인 뒤 무조건 덮어쓰지 않는다.

## 4. 공개 상태 모델

active source와 build job은 서로 다른 사실이다.

```text
publication_unit
  data_revision                 # canonical data revision selected for public use
  serving_generation            # increments for every active-release change
  active_release_id            # immutable complete-source descriptor
  fallback_release_id?         # validated source for the same data_revision
  optimistic_version

runtime_manifest
  manifest_generation           # global, increments when any unit changes
  current_version               # immutable manifest UUID used as ETag
  unit_selections[]              # immutable unit -> release + serving generation snapshot

runtime_manifest_pointer
  singleton                     # exactly one checked row
  active_manifest_id
  optimistic_version

tile_build_job
  input_release_id
  input_data_revision
  frozen_source_snapshot_id
  status = QUEUED | BUILDING | VALIDATED | FAILED | SUPERSEDED | PROMOTED
  candidate_artifact_id
```

pointer는 Catalog compare-and-swap 함수
`catalog.promote_vector_tile_runtime_manifest(expected_manifest_id, next_manifest_id)`으로만 전진한다.
이 함수는 singleton을 잠그고 완전한 manifest와 단위별 연속 generation을 요구한 뒤 원장의 active
selection을 갱신하고 같은 트랜잭션에서 pointer를 바꾼다.

`BUILDING`은 세 번째 serving source가 아니다. 정적 build 중에도 단위는 완전한 dynamic source를
계속 제공한다.

각 불변 release에는 `data_revision`, 정본 Iceberg snapshot, PostGIS projection generation, source 종류,
버전 tile URL, 검증 증거, 선택적 PMTiles artifact를 기록한다. dynamic release가 변경 가능한 PostGIS
행을 이력으로 만들지는 않는다. 정본 리비전은 Iceberg에서 계속 재구성할 수 있어야 한다.

`data_revision` changes when public feature content changes. `serving_generation` changes whenever
Catalog이 같은 데이터의 동적→정적 전환을 포함해 다른 완전한 릴리스를 선택한다.
두 값은 의도적으로 다르다. `manifest_generation`은 전역 값이며 어느 publication unit이 바뀌어도
증가하므로 하나의 poll token으로 독립적으로 바뀐 단위를 모호함 없이 감지할 수 있다.

v2 상태는 정규화된 publication-unit, release, release-layer, immutable-manifest,
manifest-unit-selection, singleton-pointer, build-job, refresh-observation table을 사용한다. 기존
schema-v1 `vector_tile_manifest`·`vector_tile_artifact`와 flat-MVT 제약은 바꾸지 않는다. 물리적
분리로 v1 의미를 실수로 재사용할 수 없고 마이그레이션 중 레거시 endpoint/event/object 바이트를 보존한다.

### 4.1 Public edit

1. 예상 active release, Catalog 선택 Iceberg snapshot, data revision, optimistic version을 읽는다.
   새 변경은 선택된 snapshot에서 branch해야 하며 선택되지 않은 Iceberg `main` head에서 시작하지 않는다.
2. 선택된 snapshot에서 release 범위 Iceberg WAP branch를 만들고 후보를 기록·검증한다. branch에는
   명시적 보존 기간을 두며 `main`과 격리한다.
3. 하나의 Foundation DB 트랜잭션에서 singleton runtime-manifest pointer를 먼저 잠근 다음 영향받은
   publication unit을 잠근다. 예상 active release/version을 확인하고 후보 리비전을 완전한 PostGIS
   serving projection에 적용한다. 새 불변 dynamic release를 기록·선택하고 `serving_generation`을
   증가시키며 R2 manifest projection용 outbox event를 기록한다. 같은 트랜잭션에서 pointer를 잠근 채
   모든 unit selection을 읽어 새 불변 전역 manifest를 만들고 `manifest_generation`을 증가시킨다.
4. Catalog runtime-manifest endpoint는 커밋된 트랜잭션의 새 완전 release를 직접 읽는다.
5. outbox publisher는 같은 manifest를 boot/distribution용 R2에 비동기로 투영한다.
6. 1~4단계가 영속화되고 dynamic source가 준비된 뒤에만 public success를 반환한다.

모든 public edit는 단위가 이미 dynamic이어도 새 release를 만든다. 단위 row lock, 전역 pointer lock,
예상 active release, branch base snapshot이 동시 변경을 직렬화한다. 모든 공개 트랜잭션은 다음 고정
순서로 lock을 얻는다.
`runtime_manifest_pointer -> publication_unit -> release rows`. 어떤 code path도 unit lock을 먼저
얻지 않는다. 선택된 WAP snapshot은 3단계가 커밋될 때만 Catalog 권위 정본 리비전이 된다. 활성화에
실패하면 선택되지 않은 branch로 남고, 이후 변경은 Catalog가 선택한 snapshot에서만 branch하므로
실패한 후보가 다음 공개 리비전에 섞이지 않는다.

reconciler는 현재 Catalog가 선택한 snapshot의 ancestry를 따라갈 때만 Iceberg `main`을
fast-forward한다.
`main`이 따라잡고 보존된 release가 더 이상 필요 없어질 때까지 선택된 branch를 모두 보존한다.
선택되지 않은 branch는 제한된 audit 보존 기간 뒤 만료된다. 이는 Apache Iceberg의 표준
[Write-Audit-Publish/branch mechanism](https://iceberg.apache.org/docs/latest/branching)이며 custom
table format이 아니다.

공개 backend를 구현하기 전에 제한된 live capability probe로 선택한 Iceberg REST Catalog provider가
정확한 snapshot에서 branch 생성, branch 쓰기·읽기·보존, `main` fast-forward를 지원하는지 증명한다.
Cloudflare R2 Data Catalog은 provider이지 table-format SSOT가 아니다. 현재 beta 구현이 표준 Iceberg
계약을 충족하지 못하면 provider 결정을 먼저 하고, 임의 R2 pointer로 branch를 흉내 내면 안 된다.
정본 Parquet/Iceberg 데이터는 호환되는 다른 Iceberg REST Catalog 뒤에서 R2에 계속 둘 수 있다.

PostGIS가 unavailable이거나 따라잡지 못했으면 변경을 pending으로 받을 수는 있지만 public visible로
보고하면 안 된다.

dynamic tile URL은 안정적이고 query가 없으며 명시적으로 cache하지 않는다. Catalog runtime manifest
pointer가 Martin의 안정 source 뒤에 있는 완전한 커밋 PostGIS 리비전을 선택한다. `serving_generation`은
manifest identity이자 refresh token이며 URL selector가 아니다. Martin의 mutable tile cache와 CDN은
삭제된 geometry를 5초 SLO보다 오래 보존하면 안 된다.

### 4.2 예약 또는 운영자 요청 정적 공개

1. active dynamic release `R`, `data_revision`, 정본 Iceberg snapshot, projection generation을 캡처한다.
2. 정확히 `R`만을 위한 build 범위 고정 PostGIS snapshot을 만든다. 변경 중인 live mirror에 여러
   `martin-cp` pass를 실행하지 않는다.
3. `R`과 고정 snapshot에 묶인 build job을 만들고 dynamic tile 제공은 계속한다.
4. `martin-cp`로 MBTiles를 bulk render한다.
5. source layer, 안정 identity, zoom 범위, feature 수, 예상 누락을 검증한다.
6. 불변 PMTiles artifact 하나로 변환·검증한다.
7. 전용 private serving-derivative R2 bucket의 versioned key에 create-only로 업로드한다.
8. `martin-static`이 설정된 R2 PMTiles prefix에서 새 불변 object를 발견할 때까지 기다린다. 그 뒤
   version 주소 source route, HTTP Range read, production 형태 URL의 decoded MVT를 검증한다.
9. 단위가 여전히 입력 release `R`을 선택하고 `data_revision`이 같으며 optimistic version이 build
   입력과 일치할 때만 CAS한다.
10. 같은 `data_revision`의 static release를 만들고 선택한다. `R`을 같은 데이터 fallback으로 보존하고
    unit `serving_generation`과 전역 `manifest_generation`을 증가시킨 뒤 Catalog에서 완전 manifest를 공개한다.

1단계 뒤 어떤 변경이든 `R`을 교체하면 9단계가 build를 `SUPERSEDED`로 표시하고 절대 승격하지 않는다.
scheduler는 최신 리비전에서 debounce 후 재시도할 수 있다. 변경을 멈추거나 잃지 않는다.

Martin은 같은 pinned Martin image를 독립적으로 설정한 두 deployment로 사용한다.

- `martin-dynamic`은 안정적이고 명시적인 PostGIS source를 가지며 static release 때문에 재시작하지 않는다.
- `martin-static`은 전용 private serving-derivative R2 bucket에 대해서만 Martin 1.12의
  `pmtiles.paths` remote-prefix discovery를 사용한다. 별도 bucket 범위 읽기 전용 R2 credential이
  bucket list/read 권한을 준다. 설정 prefix는 source discovery 범위일 뿐 IAM 경계가 아니다.

모든 archive filename은 정확히 `{publication_unit}-{release_id}.pmtiles`이며 Martin이 발견하는 source ID는
release 주소 filename stem이다. discovery는 불변 route만 추가하고 파일을 제자리에서 덮어쓰지 않는다.
publisher는 예상 source가 나타날 때까지 Martin catalog를 poll하고 대표 tile을 decode한 뒤에만 CAS
승격을 호출한다. 저장소 설정은 제한된 reload 간격을 고정하고 proof가 pinned Martin source-ID 규칙을
기계적으로 검증한다. 이름이 지정된 `pmtiles.sources` URL은 시작 시 snapshot되므로 사용하지 않는다.

Martin은 S3 호환 PMTiles object-store source로 Cloudflare R2를 지원하므로 bucket에 public `r2.dev`
endpoint나 custom domain이 필요 없다. Cloudflare CDN은 public Martin MVT route 앞에 있고 Martin은
인증된 R2 origin client다. 직접 public/custom-domain PMTiles는 명시적으로 승인한 대안이지 기본값이 아니다.

이는 Martin이 문서화한
[S3-compatible PMTiles source and remote-prefix hot reload](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
경로를 재사용하며 공개 경로에 custom watcher, Docker socket, service restart를 만들지 않는다. local
fallback도 감시하는 local directory로 같은 계약을 사용한다. 이름이 있는 `pmtiles.sources` 항목은
계속 startup snapshot이고 prefix polling은 `pmtiles.paths`만 제공한다.

### 4.3 정적 공개 이후 다음 변경

정적 tile이 public read를 처리하는 동안에도 PostGIS mirror는 warm 상태로 따라잡는다. 변경 흐름은
새 data revision과 dynamic release를 만들고 public success 전에 그 완전 source를 원자적으로 선택한다.
client는 static과 dynamic 형태를 함께 표시하지 않는다.

### 4.4 Rollback

Serving rollback과 data rollback은 다른 작업이다.

- **Serving rollback**은 잘못된 tile source가 발생하면 같은 `data_revision`의 보존·검증된 완전 release를
  expected-active-release CAS로 선택해 복구한다. 첫 slice는 warm dynamic mirror가 같은 data revision을
  계속 표현하므로 static→dynamic rollback을 증명한다.
- **Data revert**는 변경 가능한 과거 PostGIS 상태를 가리키지 않는다. Foundation은 이전 변경을
  의도적으로 되돌린 새 정본 리비전을 만들고 PostGIS로 투영한 뒤 일반 public-edit 흐름을 따른다.
  이력은 추가 전용이다.

rollback은 오래된 archive와 최신 feature tombstone을 짝지우지 않으며 infrastructure 복구의 부작용으로
business data를 조용히 바꾸지 않는다.

## 5. 매니페스트 v2 계약

현재 수용한 v1 계약은 전역 `tiles_url_template`, 물리적 `object_key_prefix`,
`flat_tile_count`, `flat_tile_total_bytes`를 사용하는 개별 flat MVT 객체를 설명한다.
이 필드는 Martin PMTiles 경로에 재사용해서는 안 된다.

Foundation이 소유하는 매니페스트 v2는 공개 단위마다 제공 전송 방식을 한 번만
명시한다. 계층마다 release·source·generation 식별자를 복제하지 않는다. 정확한
Rust DTO가 실행 가능한 계약 SSOT로 남고 OpenAPI/TypeScript 소비자를 생성한다.

```json
{
  "schema_version": 2,
  "current_version": "0196e7e0-3c20-7000-8000-000000000052",
  "manifest_generation": 108,
  "refresh_after_seconds": 4,
  "published_at": "2026-07-24T00:00:00Z",
  "publication_units": {
    "parcels": {
      "data_revision": "0196e7e0-3c20-7000-8000-000000000061",
      "serving_generation": 42,
      "active_release_id": "0196e7e0-3c20-7000-8000-000000000062",
      "canonical_iceberg_snapshot_id": "70000000000000001",
      "source": {
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "https://tiles.example.com/parcels/{z}/{x}/{y}",
        "cache_policy": "no_store"
      },
      "layers": {
        "parcels": {
          "source_layer": "parcels",
          "feature_id_property": "pnu",
          "tile_min_zoom": 11,
          "tile_max_zoom": 16,
          "render_min_zoom": 11,
          "render_max_zoom": 18,
          "feature_filter_properties": {
            "pnu": "pnu"
          }
        }
      },
      "lineage": {
        "source_record_id": "0196e7e0-3c20-7000-8000-000000000064",
        "source_file_asset_ids": [
          "0196e7e0-3c20-7000-8000-000000000065"
        ]
      }
    }
  }
}
```

`source`는 닫힌 tagged union이다. `dynamic_postgis`는 Martin source id·URL 템플릿과 cache policy를
담는다 — projection 적재 id는 **읽기 계약에서 제거됐다**([ADR-0016](../adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md) §7).
브라우저는 `serving_generation`만 비교하며 그 값을 쓴 적이 없다. `static_pmtiles`는 immutable PMTiles object key·file-asset UUID·SHA-256·byte size·release 주소 Martin source를 담는다. 유효한 variant는 정확히 하나이며 v2에서는
v1 flat-object field를 금지한다.

`current_version`, `data_revision`, `active_release_id`, projection revision과 lineage ID는
UUID다. `manifest_generation`과 `serving_generation`은 `9007199254740991` 이하의 양의 정수다.
`canonical_iceberg_snapshot_id`는 JSON number가 아닌 양의 십진 **문자열**이다. 운영 Iceberg
snapshot ID가 JavaScript의 안전한 정수 범위를 넘을 수 있기 때문이다.

`manifest_generation`은 전역 poll/change token일 뿐이다. unit의 `source`와 `serving_generation`이
runtime을 선택한다. feature content가 바뀌면 `data_revision`이 바뀌고, 같은 data를 동적에서
정적으로 바꿀 때도 `serving_generation`이 바뀐다.

Dynamic PostGIS source ID는 안정적인 명시적 Martin configuration name이다. URL에는 query가 없고
`no_store`를 사용하며 query parameter가 아니라 Catalog pointer가 commit된 revision을 선택한다.
선언되지 않은 generation별 Martin source ID는 금지한다. Static source ID는 불변 release 주소가
붙은 PMTiles filename stem이다. 브라우저의 Mapbox source identity는 logical unit name(`parcels`)로
유지하고, static release에서는 server route segment만 바꾼다. client는 filename에서 파생한
identity를 검증한 뒤 기존 logical source URL을 retarget한다.

각 계층은 하나의 정본 소문자 `feature_id_property`를 선언한다. PostGIS 뷰, PMTiles
생성기, TileJSON `vector_layers[].id`, Martin 소스, Mapbox `promoteId`, 계약 테스트가
모두 같은 값을 사용한다. 대문자 `PNU`처럼 증명에만 쓰는 별칭은 운영 식별자가 아니다.

첫 v2 공개 단위는 `parcels` 하나뿐이다. 제한된 이전 기간에는 기존 v1 매니페스트가
`parcel_anchor_aggregate`와 `parcel_anchor`를 계속 제공한다. Gongzzang은 v1의
`parcels` 산출물을 무시하고 v2 필지를 읽는다. 따라서 필지 폴리곤 전환이 두 앵커
소스를 바꾸지 않으며 단위를 중복 등록하지 않는다. `complex`, 두 앵커 단위,
`admin`, `buildings`는 각 생성자·소비자 동등성이 확인된 뒤에만 v2로 이전한다.
미래 단위에 여러 MVT 계층을 허용하는 경우에도 하나의 완전한 Martin 소스로 항상
함께 빌드·검증·전환되어야 한다.

소비자 이전이 끝날 때까지 제한된 기간 동안 매니페스트 v1을 지원한다. 고정된
Gongzzang 소비자 계약이 수용하기 전에는 Catalog가 v2 매니페스트를 절대 발행하지
않는다. 양쪽은 정확히 `schema_version`으로 분기하며 `1`이나 `2`가 아닌 값은
fail closed 처리한다.

이전 기간에는 두 스키마가 서로 다른 projection을 사용한다. 기존
`gold/manifest.json`은 v1으로 동결하고 Catalog live endpoint는
`GET /catalog/v1/vector-tiles/runtime-manifest`. Each v2 `current_version` is also the
`manifest_id` in create-only `gold/vector-tiles/manifests/{manifest_id}.json`, while the rebuildable
active v2 pointer is `gold/vector-tiles/runtime-manifest.json`. Overwriting the v1 key with v2 would
remove the anchor sources on a fresh page load and is forbidden.

## 6. 5초 활성 지도 갱신

Gongzzang은 지도가 표시되고 마운트된 동안 4초마다 겹치지 않는 조건부 Catalog
runtime-manifest 확인을 한 번 실행한다. Schema v2는 `refresh_after_seconds`를 `4`로
고정한다. 5초 SLO 중 1초를 매니페스트 조회, 소스 교체, 첫 새 타일에 남긴다.

1. `ETag`/`If-None-Match` 또는 동등한 revision 응답을 사용해 변경 없는 polling을 작게 한다.
2. 전역 `manifest_generation`이 바뀌면 같은 Catalog 응답의 완전한 매니페스트를 검증한다.
   비동기 R2 매니페스트 projection을 기다리지 않는다. 단위별
   `serving_generation`을 비교해 영향을 받은 소스를 찾는다.
3. 지원되는 Naver 내부 mapbox-gl 소스로 영향을 받은 벡터 소스만 교체하거나 다시
   지정한 뒤 타일을 강제로 갱신한다.
4. 소스 교체에 필요하면 의존 스타일 계층을 결정적인 순서로 다시 등록한다.
5. 컴포넌트가 해제되거나 페이지가 숨겨지면 polling을 멈추고 다시 보일 때 즉시 확인한다.
6. 초기 polling 단계만 무작위화해 요청을 분산한다. 요청은 겹치지 않게 하고 전송·서버
   실패 뒤에는 제한된 지수형 backoff를 사용한다.

안정 상태 예산은 표시 중인 지도 하나당 조건부 매니페스트 요청 초당 최대 `0.25`다.
v2를 켜기 전에 Foundation은 이 경로를 익명 읽기 전용 공개 계약으로 등록하고,
metric label과 edge/CORS 정책을 묶고, 배포가 선언한 동시 표시 지도 출시 예산의
두 배로 부하 probe를 통과해야 한다.

백엔드 상태 머신 구현을 시작하기 전에 기존 Naver SDK 브라우저 probe가 다음 중 하나의
소스 갱신 경로를 증명해야 한다.

1. Preferred: `getSource(id).setTiles(...)` changes a vector source URL and causes fresh tile requests
   while source-layer, zoom, and `promoteId` remain unchanged.
2. Fallback: `removeLayer`/`removeSource` followed by deterministic re-registration preserves camera
   and interaction state and meets the SLO.
3. 마지막 제한 fallback은 controlled Naver map 재초기화로 camera/selection state를 보존하고
   SLO를 만족한다.

실제 번들 SDK가 어느 것도 지원하지 않으면 이 설계는 차단하고 아키텍처 검토로
돌아간다. service worker나 사용자 정의 MVT compositor를 숨은 우회책으로 추가하지 않는다.

새 매니페스트가 잘못되었거나 후보 소스가 준비되지 않으면 클라이언트는 현재 등록된
소스 설명을 유지한다. 변경할 수 없는 static URL은 계속 정확한 이전 release를 반환한다.
유지한 dynamic URL은 가용성 fallback일 뿐이다. 안정적인 Martin 소스가 마지막으로
커밋된 완전한 projection을 읽으므로 더 최신 dynamic 바이트를 반환할 수 있으며 과거
rollback이 아니다. Foundation은 projection이 준비되기 전에 dynamic release를 발행하지
않는다. 소스 전환 실패는 관측 가능해야 하며 이전 static 타일과 새 dynamic 타일을
섞어서는 안 된다.

5초 SLO는 운영자가 그리기를 시작하거나 백그라운드 정규화 작업이 시작할 때가 아니라
Foundation이 Catalog 트랜잭션에서 활성 release와 완전한 runtime manifest를 커밋할 때
시작한다. 목표는 이동 24시간 구간에서 전환의 최소 99%를 5초 안에 완료하는 것이다.
출시 전에는 반복 브라우저 통합 probe가 매번 이 제한을 충족해야 한다. 측정은 이미 열린
지도가 변경된 각 단위의 새 `serving_generation` 타일을 성공적으로 읽을 때 끝난다.

## 7. Cache Contract

- Static PMTiles object key와 Martin source URL은 불변이며 version 주소를 사용한다.
- Static tile과 HTTP Range response는 장기 불변 cache를 사용할 수 있다.
- 가벼운 Catalog runtime-manifest 응답은 `no-cache, must-revalidate`를 사용하고 조건부
  요청을 지원한다. 따라서 매 polling마다 현재 revision을 확인하면서 변경이 없는 응답은 작게 유지한다.
  schema-v2 polling 간격은 정확히 4초이며 소스 갱신과 첫 새 타일에 1초를 남긴다.
  이 예산을 놓치면 측정 결과에 따라 간격을 줄여야 한다.
- Dynamic tile은 출시 시 `no-store`를 사용하거나 origin·Martin·CDN·browser·polling 지연의 합이
  5초 이내라는 측정 결과가 있는 cache 설정을 사용한다.
- Dynamic Martin URL은 query가 없고 `no-store`를 사용한다. runtime-manifest pointer가 완전한
  PostGIS revision을 선택한다. Business rollback은 새 revision을 만들거나 완전한 static release를
  선택하며 URL query를 historical selector로 사용하지 않는다.
- PMTiles object는 제자리에서 덮어쓰지 않는다.
- Promotion purges or expires only the small mutable manifest/revision pointer, not immutable tile
  objects.

## 8. Ownership and Boundaries

- Foundation은 canonical public/reference geometry, feature identity, publication state, Martin
  source readiness, PMTiles build, R2 upload, validation, promotion, rollback과 manifest를 소유한다.
- Gongzzang validates and consumes the published manifest. It owns only active-map refresh and
  product presentation.
- Dawneer may later provide staff controls for edit, approval, publish-now, and rollback, but calls
  Foundation APIs. Its UI state is not publication authority.
- Gongzzang listing markers retain their separate Gongzzang-owned dynamic contract. This design does
  not reuse listing tables or tombstone state.
- Cross-area integration remains published HTTP contracts/events; no cross-area database access is
  introduced.

## 9. Failure Handling and Observability

Required state-transition evidence:

- data revision·serving generation·active release/source·같은 data의 fallback release
- PostGIS projection generation과 readiness
- build input release/snapshot·duration·result·validation report·supersession reason
- immutable R2 object key·checksum·size·upload precondition 결과
- Martin source readiness·decoded feature count·source layer·identity sample·Range 동작
- CAS promotion/rollback 결과
- manifest projection lag
- client generation-poll lag, source-reload success/failure, and time to first tile at the new
  generation.

동적 unit을 선택했지만 PostGIS projection이 뒤처졌거나, 정적 unit을 선택했는데 정확한 Martin/R2
artifact를 읽을 수 없으면 readiness는 실패해야 한다. build failure가 나도 active dynamic source는
바뀌지 않는다.

## 10. Mechanical Guards

Tests must make these regressions impossible:

1. Manifest publication unit은 정확히 하나의 active source를 선택한다.
2. 모든 manifest layer는 하나의 Martin source와 예상 MVT source layer에 매핑되며, slice
   설정에 선언되지 않은 source는 노출되지 않는다.
3. 정적·동적 producer는 동일한 canonical feature identity를 내보낸다.
4. 빌드 중 편집이 발생하면 오래된 빌드는 승격할 수 없다.
5. 실패한 빌드나 R2 업로드는 active source를 변경할 수 없다.
6. 동적→정적 전환은 Martin URL로 예상 feature를 디코드한 뒤에만 발생한다.
7. 정적→동적 편집은 동적 projection이 뒤처진 상태에서 공개 성공을 보고할 수 없다.
8. 브라우저 revision 변경은 source를 교체하며 두 버전을 모두 등록한 채 남기지 않는다.
9. Malformed/unready new manifests retain the current source descriptor; tests distinguish exact
   immutable-static retention from the non-historical latest-projection behavior of a dynamic route.
10. 증명은 하나의 sample unit에 대해 추가·수정·삭제를 실행하고 duplicate·gap·부활한
    feature가 없는지 확인한다.
11. Two outbox publishers interleaved as `A reads -> B publishes newer -> A writes` cannot regress
    the R2 runtime pointer; A's stale ETag fails and reconciliation selects Catalog's current manifest.
12. Two different publication units activated concurrently preserve both selections and produce an
    ordered global manifest sequence; a partial or lost unit selection is impossible.

가드는 `cargo xtask verify foundation`과 `cargo xtask verify gongzzang`을 통해 실행한다.
워크플로우가 두 번째 검증 경로를 만들면 안 된다.

## 11. 규모 확장

출시 단위는 사용자 정의 타일 조합 없이 즉시 정확성을 보장하는 가장 작은 구조인
완전한 계층이다. 샤딩을 추가하기 전에 다음을 수집한다.

- Martin/PostGIS p95 and origin CPU;
- dynamic cache-miss rate and cost;
- PMTiles size and rebuild duration;
- edit frequency and percentage of each layer affected;
- superseded build frequency;
- active-map refresh success and latency.

측정된 임계치를 넘은 경우에만 후속 ADR이 고정되고 겹치지 않는 partition 소유권을
도입할 수 있다. 각 partition은 동일한 단일 활성 소스 불변식을 사용한다. 수정은
이전·새 geometry를 모두 가로지르는 모든 partition을 dynamic으로 표시한다. feature 단위
static/dynamic 혼합은 계속 금지한다.

직접 flat MVT 객체, DuckDB/GeoParquet 제공, 서버 측 사용자 정의 MVT 조합은 출시 경로가
아니다. 선택한 구조가 SLO나 비용 목표를 달성할 수 없다는 운영 증거가 있을 때만
재검토한다.

## 12. ADR Reconciliation

권위 문서가 이제 다음과 같이 일치한다.

1. Root ADR 0006 defines object-storage-first serving and the single-complete-source invariant.
2. Foundation ADR 0004 owns edit publication, active releases, strict manifest v1/v2 semantics, and
   authenticated private-R2 Martin serving.
3. Foundation ADR 0006은 R2/Iceberg 정본 데이터와 재구성 가능한 PostGIS/PMTiles serving projection의
   권위로 남는다.
4. Gongzzang ADR 0036 owns strict consumer dispatch and source replacement; historical ADRs 0016 and
   0021 are superseded.
5. 증명 runbook은 로컬 v1 adapter와 운영 v2 contract를 구분한다.

## 13. Delivery Boundary

첫 구현은 `parcels` 공개 단위를 사용하는 일반 vertical slice 하나다.

- canonical revision 하나와 완전한 PostGIS mirror 하나
- backend state-machine 구현 전 검증된 Naver mapbox-gl source-reload path 하나
- 실패한 activation이 이후 public history에 들어갈 수 없는 격리된 Iceberg WAP candidate 하나
- 전용 proof R2 path의 immutable PMTiles candidate 하나
- restart 없이 create-only R2 object를 노출하는 static-Martin remote-prefix discovery cycle 하나
- dynamic edit and 5-second open-map refresh;
- validated CAS promotion to static;
- a concurrent edit that mechanically blocks stale promotion;
- same-data static-to-dynamic serving rollback;
- generic contracts and tests capable of adding `complex`, `buildings`, and future Foundation polygon
  layers without copying publication logic.

지역 partition, 사용자 정의 타일 compositor, feature tombstone, 전국 규모 출시,
Dawneer 관리자 UI는 구현하지 않는다.
