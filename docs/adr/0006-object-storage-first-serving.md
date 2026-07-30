---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-28
---

# ADR 0006: 기준 데이터는 객체 저장소 우선, 개인화·거래 데이터는 Postgres

- Status: Accepted
- Date: 2026-07-21
- Amended: 2026-07-24 (single-source spatial publication and manifest v2)

## Context

foundation-platform 파이프라인은 원시 공공데이터(Bronze, Cloudflare R2 약 257GiB)를
수집하고 R2의 Iceberg로 Silver/Gold를 처리하며 gongzzang 제품이 사용하는 산업용 부동산
카탈로그(필지·건물·산단)를 제공한다. 2026-07-21 감사 결과 파이프라인은 조각별로
end-to-end 구현되어 있지만 정본 Postgres catalog 테이블은 사실상 비어 있었다. 선택된
Iceberg revision을 제공 저장소로 넘기는 "마지막 구간"이 실제 실행되지 않은 상태였다.

Postgres를 정본으로 만들거나 batch로 안정된 타일의 상시 원천으로 쓰지 않고, 다음의
현대적
**object-storage-first**(zero-disk/diskless라고도 함) 방식을 채택한다. 객체 저장소와 edge
cache, 사전 생성 산출물에서 읽기를 제공해 전통적인 serving DB를 최소화한다.
이는 2025–2026년에 확립된 패턴이다(Cloudflare Workers/KV, WarpStream, Turbopuffer,
Quickwit; AWS S3 Tables·Snowflake·Databricks에서 진행되는 Iceberg-on-object-storage 수렴).
카탈로그가 batch로 갱신되는 read-mostly reference data이고 이미 Cloudflare R2(egress 무료)를
사용하므로 우리에게 특히 잘 맞는다.

추측이 아니라 실제 코드에 근거한 결정이다.

- 지도 renderer는 **Naver Maps GL**(mapbox-gl bundling)이며 URL-first로 구동한다. Foundation
  catalog vector tile은 이미 manifest로 주소를 지정하는 정적 R2/CDN object로 설계되어
  있으므로(GZ-ADR-0036) R2 제공을 위해 renderer를 바꿀 필요가 없다.
- gongzzang은 다음 두 point lookup으로만 catalog를 소비한다.
  (`catalog/v1/parcels/by-pnu/{pnu}`, `.../buildings`) — tiny, PNU-keyed, immutable-ish.
- gongzzang **listing search**는 인증되고 viewer별(`is_bookmarked`)이며, live mutable row를
  exact-count·pagination·정렬로 조회하므로 구조적으로 relational이고 사전 렌더링할 수 없다.

## Decision

**reference/spatial 읽기는 object-storage-first로 제공하고 개인화·거래성 읽기는 Postgres에
둔다.** 접근 방식별 기준은 다음과 같다.

| Access pattern | Store / engine |
|---|---|
| Canonical spatial feature history | Catalog-selected `silver.*` SCD2 Apache Iceberg snapshots on Cloudflare R2 |
| Curated consumer projections (Gold) | Rebuildable Iceberg tables/artifacts derived from the selected Silver snapshot |
| Map tiles — static basemap (parcels/complex/admin/buildings) | Immutable, versioned PMTiles serving derivatives in a dedicated private serving-derivative R2 bucket, read by Martin through the S3-compatible API and fronted by Cloudflare CDN |
| Map tiles — Foundation units with a newly approved edit | A complete, reconstructible PostGIS serving projection rendered by Martin |
| Map tiles — dynamic Gongzzang listing markers | Existing Gongzzang `ST_AsMVT`/PostGIS path remains in place; the Martin slice is additive, not its migration |
| Catalog point-lookups (parcel/building by PNU) | Pre-rendered JSON on R2/CDN, or Cloudflare KV, keyed by PNU |
| Heavy / ad-hoc analytics | Trino over Iceberg (existing); DuckDB for light/embedded |
| Bronze → Silver → Gold processing | Spark (existing) |
| gongzzang listing search + personalized card feed | Postgres + PostGIS (existing) |
| Sessions, tile cache, rate-limit, JTI denylist | Redis (existing) |

판단 규칙은 **"모두에게 같고 batch 갱신"이면 R2, "사용자별 실시간"이면 Postgres**다.

**타일 제공 엔진 — Martin(Rust, MapLibre).** 모든 `(publication_unit, serving_generation)`에
대해 Foundation은 완전한 Martin source 하나만 선택한다: `DynamicPostgis` XOR
`StaticPmtiles`. 단위에 새 승인 content가 있으면 Martin이 완전한 PostGIS projection을 제공하고,
예약 publication 뒤에는 불변 PMTiles release 하나를 제공한다. 브라우저는 static base를 edit
overlay·tombstone 계층·feature suppression list와 조합하지 않는다. publication unit별로 정적
또는 동적 source를 독립적으로 선택할 수 있다.

Cloudflare CDN이 반복되는 static 읽기를 흡수한다. 다음 편집을 즉시 노출할 수 있도록
PostGIS는 warm 상태의 완전한 projection으로 유지하지만, 이는 선택된 R2/Iceberg Silver
SCD2 snapshot과 감사된 편집 ledger에서 재구성한 serving projection이다. 정본 geometry의
유일한 사본은 아니다.
Gongzzang의 기존 listing `ST_AsMVT` endpoint와 `filter_hash`/marker-delta contract는 별도의
product-owned runtime path이며 이 결정으로 제거하지 않는다.

정본·원천 공간 데이터와 제공 파생물은 서로 다른 비공개 R2 보안 영역이다.
Canonical·Bronze·lakehouse·recovery·backup data는 static-tile derivative bucket을 공유하지
않는다. Martin은 별도 bucket-scoped read-only credential로 S3 호환 API를 통해 derivative
bucket만 읽는다. 표준 R2 API token은 bucket-scoped이며 object-key prefix는 discovery/create-only
관례일 뿐 IAM 경계가 아니다. 공개 `r2.dev` URL이나 bucket-bound custom domain은 증명 전용 또는
명시적으로 승인된 대안이며 운영 기본값이 아니다.

지원하는 static 빌드 chain은 정확히 다음과 같다.

`PostGIS snapshot → martin-cp → MBTiles → mbtiles validate → pmtiles convert → pmtiles verify → R2`

`martin-cp`는 PMTiles가 아니라 MBTiles를 쓴다. `mbtiles diff/apply-patch`는 MBTiles build
artifact를 최적화하거나 동기화할 수 있지만 PostGIS를 증분 변경하거나 원격 PMTiles를 patch하지
않는다. 새 불변 PMTiles version publication도 생략하지 않는다.

**공개 수명주기.** Foundation이 정본 geometry와 타일 공개를 소유한다. 공개 편집은 먼저
Iceberg Write-Audit-Publish branch에 commit하고 완전한 pointer-selected PostGIS source로
projection한 뒤 Martin으로 decode하고 compare-and-swap으로 선택한다. active release commit 직후
동적 source가 보인다. 승인은 debounced static build를 queue하며 관리자는 대기 시간을 건너뛰도록
**Publish now**를 요청할 수 있다. 예약된 retry/reconciler가 실패 job을 복구한다.

빌드는 선택된 projection generation을 동결하고 완전한 단위 하나를 렌더링·검증한 뒤 새 불변
PMTiles object를 create-only로 업로드한다. Promotion은 Martin이 정확한 R2 object를 읽고
decode할 때까지 기다린 뒤 input dynamic release가 여전히 active일 때만 완전한 static release를
선택한다. 따라서 동시 편집이 있으면 candidate는 `SUPERSEDED`가 된다. 추가·수정·삭제 모두
동일한 whole-unit switch를 따르므로 overlay 뒤에 오래된 static feature가 남지 않는다.
Rollback은 같은 data revision에서 보존·검증된 다른 불변 release를 선택한다. business data를
되돌리면 기존 release를 바꾸지 않고 새 canonical revision을 만든다.

**매니페스트 계약.** GZ-ADR-0036 schema v1은 제한된 legacy flat-PBF 계약으로 남는다.
`object_key_prefix`는 물리 tile prefix이고 `flat_tile_count`/
`flat_tile_total_bytes`는 개별 object 수치다. 이 field를 PMTiles나 Martin route에 재사용하지
않는다. Schema v2는 layer를 publication unit 아래에 묶고 다음을 담는다.

- an exact `schema_version: 2`;
- a global, JavaScript-safe `manifest_generation` used only as a poll/change token;
- an exact `refresh_after_seconds: 4` launch polling interval;
- a per-unit UUID `data_revision`, JavaScript-safe `serving_generation`, immutable
  `active_release_id`, and canonical Iceberg snapshot ID encoded as a positive decimal string;
- one tagged `source` value, `dynamic_postgis` or `static_pmtiles`, never both;
- one stable dynamic or release-addressed static Martin tile URL and the unit's complete MVT layer metadata; and
- transport-specific PostGIS projection or PMTiles object/checksum/size lineage.

운영 client는 schema version으로 정확히 분기하고 전체 매니페스트를 검증한 뒤
`serving_generation`이 바뀐 unit만 교체한다. `manifest_generation`은 tile source를 선택하지
않는다. Static release는 새 불변 Martin route/cache identity를 사용한다. Dynamic Martin URL은
안정적이고 query가 없다. `vector_tile_runtime_manifest_pointer`만 source selector이며
`serving_postgis.*_current` view는 그 pointer를 정확히 하나의 commit된 `data_revision`과 join한다.
Parcel identity는 canonical lowercase `pnu`로 수렴한다. 증명 전용 uppercase `PNU`는 두 번째
production contract가 아니다. 정확한 rollback은 cache-busting query string이 아니라 완전한
pointer switch다.

첫 v2 이전 단위는 `parcels` 하나뿐이다. 기존 schema-v1 endpoint·저장 모델·event,
`NEXT_PUBLIC_TILES_MANIFEST_URL` 의미와 `gold/manifest.json` bytes는 frozen 상태로 둔다. 이
v1 bytes에는 parcels와 두 anchor artifact가 계속 들어 있고, v2-aware runtime은
`parcel_anchor_aggregate`와 `parcel_anchor`만 등록한다. V2는 별도 Catalog endpoint
`/catalog/v1/vector-tiles/runtime-manifest` and R2 projection
`gold/vector-tiles/runtime-manifest.json`, plus create-only history at
`gold/vector-tiles/manifests/{manifest_id}.json`에 create-only history를 기록한다. 이 제한된
migration 동안 Gongzzang은 v1 parcel artifact를 무시하고 v2 parcels를 읽으므로 publication unit이
두 active source를 갖지 않는다. anchor·complex·admin·building unit은 별도의 producer/consumer
parity가 확인된 뒤에만 이동한다. Concurrent outbox worker는 ETag compare-and-swap으로만 변경
가능한 v2 R2 pointer를 갱신하며 check-then-unconditional-overwrite는 금지한다.

**Deferred** 항목은 규모가 필요해질 때까지 미룬다. 데이터가 R2의 open format에 남으므로
추가해도 migration 없이 engine을 교체할 수 있다. high-QPS analytics serving은 ClickHouse /
Apache Pinot, 한국어 full-text search는 Meilisearch / OpenSearch-with-Nori를 후보로 둔다.

이는 **거의 전부 기존 스택**이다. R2, Iceberg, Spark, Trino, Postgres, Redis를 이미 사용한다.
추가 구성요소는 **Martin**(경량 Rust 타일 서버) 하나이며 PostGIS MVT와 로컬·원격 PMTiles를
모두 제공하기 때문에 선택했다. 핵심 변화는 인프라를 쌓는 것이 아니라 제공 *패턴*이다.

## 결과

- **비용**: R2/CDN이 안정 상태 static 읽기를 담당하므로 PostGIS가 상시 지도 traffic을
  처리하지 않아도 된다. 즉시 source 전환과 validation을 위해 완전한 warm PostGIS projection은
  유지하며, static publication은 compute/load를 줄일 뿐 Foundation geometry storage를 0으로
  만들지는 않는다.
- **"빈 정본 테이블" 문제의 해법**: 공간 파이프라인의 마지막 구간은
  "Silver snapshot 선택 → Gold/PMTiles/JSON 파생"이며 "Postgres를 canonical copy로 만들기"가
  아니다.
기존 client는 URL-first다. 수용된 manifest v2 contract는 production publication 전에
Foundation과 Gongzzang 양쪽에서 검증한다.
- **정직한 경계**: 인증된 listing search와 개인화 feed는 Postgres에 남긴다.
  Object-storage-first에서는 canonical Postgres geometry나 steady-state static tile을 위한
  PostGIS origin load가 없다. Foundation은 즉시 편집·validation·static rebuild를 위해 완전한
  warm derived PostGIS projection 하나를 계속 운영한다.
- **Foundation 단위 타일 제공은 Martin으로 표준화**: 같은 오픈소스 engine이 완전한 static
  PMTiles와 완전한 dynamic PostGIS source를 제공한다. Gongzzang listing tile serving 교체는
  별도의 향후 결정이다.
- **가역성**: 정본 데이터가 R2의 개방 형식(Iceberg/PBF/JSON)에 남으므로 serving engine
  선택(R2, KV 대 static JSON, Trino 대 DuckDB 대 이후 ClickHouse/Pinot)은 data migration 없이
  교체할 수 있다.
- **증명 우선 출시**: 기존 Postgres catalog와 listing tile 경로는 제거하지 않는다.
  하나의 `industrial_complex`로 production promotion 전에 PostGIS/Martin과 PMTiles/Martin
  lane을 실행한다. 증명의 real-R2 branch는 전용 test credential로 실제 실행해 `REAL R2`를
  보고한 경우에만 증거로 인정한다.

## References

- [Single-source spatial publication architecture](../architecture/single-source-spatial-publication.md)
- [Administrative boundary and parcel identity versioning](../architecture/administrative-boundary-versioning.md)
- [Foundation ADR 0004 - Vector tile publication contract](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)
- GZ-ADR-0036 — vector tile runtime contract (legacy flat-object v1 and single-source v2)
- [Martin file sources](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
  — Rust PostGIS / PMTiles / MBTiles serving, S3-compatible R2, and remote-prefix polling
- ADR-0004 — verification SSOT (same "one definition" discipline, applied to serving)
- Internal foundation pipeline audit, 2026-07-21
