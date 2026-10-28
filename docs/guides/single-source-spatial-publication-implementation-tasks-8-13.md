---
status: proposed
owner: foundation
doc_type: guide
last_reviewed: 2026-07-28
---

<!-- public-repository-safety: reviewed-public-contract -->

# 단일 출처 공간 데이터 공개 구현 안내서 — Task 8–13

[구현 안내서](./single-source-spatial-publication-implementation.md)의 뒷부분이다. 진행 현황·파일·책임 지도·Task 1–7은
그 문서에 있다.

## Task 8: Build One Frozen PMTiles Release and Discover It Through Martin

정적 빌드는 변경 가능한 공개 PostGIS mirror를 읽지 않는다.

**Files:**
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_build.rs`
- Create: `platforms/foundation-platform/crates/publisher/foundation-publisher-tiles/src/tile_derivative_object_storage.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/{main.rs,main_command_tests.rs,r2_layout.rs}`
- Modify: `scripts/tiles/{compose.yaml,martin-dynamic.yaml,martin-static.yaml,tiles-slice-proof.sh}`
- Test: `platforms/foundation-platform/services/foundation-api/tests/tiles_slice_harness_contract.rs`

- [ ] **Step 1: Write the frozen-build concurrency test**

release R20에서 격리된 build PostGIS database를 시작하고 `martin-cp`를 실행한 뒤 live DB에서
edit R21을 activate한다. 결과 archive가 R20과 저장된 `canonical_iceberg_snapshot_id`만 정확히
담고 혼합되지 않는지 decode해 확인한다. frozen input에 superseded SCD2 row를 넣고 archive에
들어가지 않음을 증명한다.

- [x] **Step 2: Write the R2 storage-boundary tests**

다음 항목을 검증한다.

- `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_*` 설정이 Lakehouse·PostgreSQL 복구 네임스페이스와 분리되어 있는지;
- 전용 비공개 파생 버킷 허용 목록이 있는지;
- 생성 전용 `If-None-Match: *`를 사용하는지;
- 변경할 수 없는 정확한 `{publication_unit}-{release_id}.pmtiles` 파일명을 사용하는지;
- 삭제·덮어쓰기 명령이 없는지;
- 파생 버킷에 읽기 전용으로 범위가 제한된 별도 Martin 자격 증명을 사용하는지. prefix는 IAM 경계가 아니다.

- [ ] **Step 3: Implement the isolated build database**

Task 2와 같은 안전한 control/data 경계를 따른다. Rust command는 input release, data revision,
정확히 frozen된 Iceberg snapshot, 예상 current-row contract digest, pinned tool image digest,
bounded zoom과 output path를 담은 checksum-addressed secret-free build plan을 낸다.
호스트 `tiles-slice-proof.sh`/Compose 실행기가 일회용 컨테이너에서 계획을 실행한다.
publisher 컨테이너는 Docker를 호출하지 않고 Docker socket도 받지 않는다. 별도 Rust
명령이 실행 receipt와 산출물을 검증한 뒤 빌드 결과를 기록한다.

실행 명령 표면은 다음과 같다.

```text
foundation-outbox-publisher plan-spatial-tile-build --unit parcels
foundation-outbox-publisher record-spatial-tile-build-result --receipt-json <path>
foundation-outbox-publisher promote-spatial-tile-build --build-id <uuid> --expected-release <uuid>
```

Compose 실행기는 일회용 PostGIS 데이터베이스를 만들고 정확히 동결된 snapshot에서 계약이
소유한 current-row predicate가 선택한 행만 가져온 뒤 명시적인 Martin 뷰를 적용하고 모든
zoom pass를 실행한다. 증명에서 고정한 PostGIS/Martin 이미지를 재사용한다. builder에
실시간 serving 데이터베이스의 DDL 권한을 주지 않는다.

- [x] **Step 4: Implement the standard OSS build chain**

```text
frozen PostGIS
  -> martin-cp
  -> MBTiles validate
  -> go-pmtiles convert
  -> go-pmtiles verify
  -> MVT identity/feature validation
```

호스트 실행기는 이 고정된 OSS 도구를 실행한다. Rust는 계획/receipt 검증과 상태 전환을
소유하며 MVT 인코딩이나 Docker socket을 통한 프로세스 제어를 구현하지 않는다.

이 slice의 모든 빌드 입력과 경로는 단일 `parcels` source다. 쉼표로 연결한
`parcels,parcel_anchor_aggregate,parcel_anchor` `martin-cp` input이나 composite polygon URL을
사용하지 않는다. 기존 anchor source는 direct legacy source로 남을 수 있지만 `parcels`
publication unit이나 archive의 구성원이 아니다. Foundation polygon artifact의 source URL에 `,`가
있으면 거부하고 PMTiles TileJSON에 `parcels` source layer만 정확히 있는지 확인하는 guard를 추가한다.

- [ ] **Step 5: Make mutable dynamic tiles impossible to serve from an old cache key**

`martin-dynamic`에 `cache: disable`을 설정한다. 그렇지 않으면 계약된 Martin이 기본으로 in-process
tile cache를 켠다. Dynamic v2 URL은 안정적이고 query가 없으며 Catalog runtime-manifest pointer가
완전히 commit된 PostGIS revision을 선택하고 source는 `no_store`를 사용한다. generation이 바뀌면
tile URL이 아니라 manifest ETag와 client source-selection generation이 바뀌어야 한다. CDN 경계의
dynamic route에는 `Cache-Control: no-store`를 반환한다. proof는 add/modify/delete 전후에 같은
z/x/y를 fetch하고 purge 없이 새 bytes를 확인해야 한다. Static Martin은 불변 cache를 유지한다.
Dynamic `martin_source_id`는 안정적인 명시적 unit source(`parcels`)로 남기며 generation마다
설정되지 않은 source ID를 만들지 않는다. query parameter를 historical release selector로 취급하지 않는다.

- [x] **Step 6: Configure Martin remote-prefix discovery**

Replace the named static source with:

```yaml
pmtiles:
  paths:
    - ${FOUNDATION_TILE_DERIVATIVE_PMTILES_PREFIX}
  reload_interval: 5s
```

환경변수로 R2 S3-compatible endpoint와 read-only credential을 설정한다. local mode에서는 감시하는
local directory를 사용한다. 업로드 filename과 예상 Martin source ID가 각각
`{publication_unit}-{release_id}.pmtiles`와 그 stem과 같아야 하며, 이 파생 규칙을 pinned Martin
image 계약으로 기계적으로 테스트한다. 정확한 image identity는
`static-release-toolchain.contract.json`만 소유한다.

- [ ] **Step 7: Upload, discover, decode, then mark validated**

create-only 업로드 뒤 제한된 timeout으로 Martin catalog를 polling해 예상 source ID가
나타날 때까지 기다린다. 대표 z/x/y 타일을 가져와 소문자 `pnu`를 디코드하고 feature
ID/count를 dynamic release와 비교한 뒤에만 static 후보를 검증됨으로 기록한다.

- [x] **Step 8: Run local and real-R2 proof modes**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

로컬 출력에는 `DYNAMIC cache isolation OK`와 `STATIC prefix hot reload OK`가 포함되어야
한다. 완전한 테스트 인증 정보가 있으면 `REAL R2`, 고유 객체 키, discovery 증거, 디코드한
일치 feature가 포함되어야 한다. 운영/lakehouse/recovery 버킷은 업로드 전에 거부되어야 한다.

- [x] **Step 9: Commit**

```bash
git add platforms/foundation-platform/services/foundation-outbox-publisher \
  scripts/tiles platforms/foundation-platform/services/foundation-api/tests
git commit -m "feat(foundation): publish immutable PMTiles releases"
```

## Task 9: Expose the Atomic Runtime Manifest and ETag

**Files:**
- Modify: `platforms/foundation-platform/services/foundation-api/src/routes/{catalog.rs,catalog_openapi.rs,catalog_tests.rs,mod.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/state.rs`
- Modify: `platforms/foundation-platform/crates/foundation-contracts/src/catalog.rs`
- Modify: `platforms/foundation-platform/docs/openapi/catalog.v1.json`
- Modify: `platforms/foundation-platform/infra/cloudflare/foundation-runtime-manifest-edge-policy.v1.json`
- Modify: `platforms/foundation-platform/docs/architecture/traffic-auth-policy-registry.v1.json`
- Modify: `platforms/foundation-platform/services/foundation-api/src/routes/tests/cors_and_labels.rs`
- Test: `platforms/foundation-platform/services/foundation-api/tests/runtime_manifest_edge_policy.rs`
- Modify: `platforms/foundation-platform/crates/foundation-outbox/src/vector_tile_manifest.rs`
- Modify: `platforms/foundation-platform/crates/foundation-outbox/src/object_storage/{requests.rs,file.rs,r2.rs,tests.rs}`
- Test: `platforms/foundation-platform/crates/foundation-outbox/tests/{vector_tile_manifest_pointer.rs,publish_roundtrip.rs}`

- [x] **Step 1: Write failing API tests**

Assert:

- `GET /catalog/v1/vector-tiles/runtime-manifest` returns one complete v2 manifest;
- `ETag` is a standards-compliant quoted entity tag containing the immutable `current_version`;
- matching `If-None-Match` returns 304 with no body;
- a per-unit activation changes global `manifest_generation`, `current_version`, and ETag;
- response uses `Cache-Control: no-cache, must-revalidate`;
- database manifest is visible immediately even if R2 pointer projection is delayed.
- every manifest is written create-only to
  `gold/vector-tiles/manifests/{current_version}.json` before the mutable v2 pointer moves;
- retrying an immutable write accepts identical bytes/checksum but fails closed on any mismatch;
- a delayed stale event may finish its immutable object but cannot move the mutable pointer;
- the interleaving `publisher A reads ETag -> publisher B writes newer -> A attempts write` rejects
  A's stale ETag and leaves or repairs the pointer to Catalog's current manifest;
- the default-disabled v2 capability gate cannot serve v2, emit a v2 public event, or project v2 to
  R2 until explicitly enabled;
- legacy `gold/manifest.json` bytes는 바뀌지 않고 parcel과 두 anchor artifact를 계속 설명한다.
  제한된 parcel-v2 migration 중에는 v2-aware Gongzzang consumer만 해당 v1 parcel artifact를 억제한다.
- CORS preflight는 `If-None-Match`를 허용하고 response는 browser JavaScript에 `ETag`를 expose한다.
- traffic/auth registry는 정확한 GET route를 anonymous `public_contract`로 선언하고 bounded
  canonical metric label 하나를 고정하며 edge policy를 요구하고 service identity는 적용하지 않는다.
- 저장소에 고정한 Cloudflare edge policy는 실행 가능한 deployment input이다. 정확한 route expression,
  CORS/ETag 동작, IP별 429 rate limit과 64 requests/second zero-error p95 load gate를 포함한다.
- the deployment contract fails if the route is enabled while that edge policy is missing, points at
  another path, omits rate limiting, or disagrees with the traffic/auth registry;
- 128 declared concurrent visible maps produce at most 32 requests/second, and the endpoint passes a
  prelaunch 64 requests/second conditional-GET probe with zero errors and p95 below one second.

- [x] **Step 2: Implement the atomic read endpoint**

활성 전역 매니페스트와 모든 공개 단위 release 설명을 하나의 database snapshot에서 읽는다.
응답 일부를 R2에서 조립하지 않는다. `FOUNDATION_TILE_RUNTIME_MANIFEST_V2_ENABLED=false`를
fail-closed 기본값으로 둔다. 꺼져 있는 동안 새 runtime endpoint는 v2를 publish하지 않고 승인된
v1 endpoint는 변하지 않는다. Task 10의 Gongzzang parser·provider-contract snapshot·SHA pin이
반영된 뒤에만 켠다.

기존 CORS layer가 `header::IF_NONE_MATCH`를 허용하고 `header::ETAG`를 expose하도록 바꾼다.
비기본 허용 origin을 사용하는 route test로 조용한 회귀를 막는다. route는 anonymous read-only
public contract로 등록하며 Foundation service token을 browser JavaScript에 절대 보내지 않는다.
route를 켜기 전에 Cloudflare deployment adapter로
`infra/cloudflare/foundation-runtime-manifest-edge-policy.v1.json`을 적용한다. adapter는 정확한
path expression, origin allow-list binding, ETag/conditional 동작, IP별 `120 requests / 10 seconds`
제한과 분산 64 requests/second probe를 확인해야 한다. 적용된 edge rule 없는 registry 선언은
유효한 launch 상태가 아니다. 초기 deployment budget은 동시 visible map 128개이며 설정이나 측정
사용량이 이를 넘기기 전에 더 큰 budget을 다시 load-test한다.

- [x] **Step 3: Add the additive v2 outbox projection**

Task 4에서 정의하고 byte-test한 v2 event를 소비하며 여기서 두 번째 payload를 정의하지 않는다.
v1 projection bytes는 그대로 둔다. 각 event마다 정확한 불변 Catalog manifest를 읽고
`If-None-Match: *`로 `gold/vector-tiles/manifests/{manifest_id}.json`에 쓴다. 이미 object가 있으면
bytes/checksum이 같아야 한다. 이어 event ID/generation을 active Catalog pointer와 비교하고 아직
active일 때만 `gold/vector-tiles/runtime-manifest.json`을 쓴다. 오래된 event는 audit history는
보존하지만 변경 가능한 pointer를 되돌릴 수 없다.

pointer write는 storage-port compare-and-swap로 만든다. object와 opaque ETag를 읽고 replacement에는
`If-Match`, bootstrap에는 `If-None-Match: *`를 사용하며 `412 Precondition Failed`를 typed conflict로
매핑한다. conflict가 나면 Catalog와 R2를 다시 읽고 현재 active manifest만 retry하거나 event가
오래됐으면 건너뛴다. `OverwriteAllowed`로 모델링하지 않고 단일 process나 pre-write database
check에 의존하지 않는다. file adapter도 key-scoped lock 아래 같은 fenced semantics를 구현해
약한 local substitute로 test가 통과하지 않게 한다.

publisher는 v2 byte를 legacy `gold/manifest.json`에 절대 쓰지 않는다. HTTP endpoint가 사용하는
동일한 typed fail-closed 공개 capability를 transaction 시점 event 발행과 publisher projection
양쪽에 적용한다(심층 방어). 비활성 상태에서는 HTTP와 R2가 v1 bytes를 유지하고 v2 event가
없어야 한다. Task 10 뒤 capability를 켜면 다음 transition/reconcile가 현재 v2 state를 publish할
수 있지만 legacy v1 pointer는 byte-for-byte 그대로여야 한다.

- [x] **Step 4: Regenerate and verify OpenAPI**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  "$RUST_TOOLCHAIN_IMAGE" \
  cargo run --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  --locked --quiet -p foundation-api --bin export-catalog-openapi -- \
  /workspace/platforms/foundation-platform/docs/openapi/catalog.v1.json
```

기대 결과: 생성 산출물이 커밋된 OpenAPI 계약 테스트와 일치한다.

- [x] **Step 5: Run tests and commit**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p foundation-api -p foundation-outbox vector_tile
git add platforms/foundation-platform/services/foundation-api \
  platforms/foundation-platform/crates/foundation-contracts \
  platforms/foundation-platform/crates/foundation-outbox \
  platforms/foundation-platform/docs/openapi/catalog.v1.json \
  platforms/foundation-platform/docs/architecture/traffic-auth-policy-registry.v1.json
git commit -m "feat(foundation): publish atomic tile runtime manifest"
```

## Task 10: Implement the Gongzzang v2 Consumer and Four-Second Poll

**Files:**
- Modify: `products/gongzzang/apps/web/lib/map/vector-tile-manifest.ts`
- Create: `products/gongzzang/apps/web/lib/map/foundation-vector-layer-registry.ts`
- Modify: `products/gongzzang/apps/web/lib/config/layer-ids.ts`
- Create: `products/gongzzang/apps/web/lib/map/foundation-vector-source-refresh.ts`
- Modify: `products/gongzzang/apps/web/lib/map/listing-map-runtime.ts`
- Modify: `products/gongzzang/apps/web/components/listings/listing-map.tsx`
- Modify: `products/gongzzang/apps/web/proxy.ts`
- Modify: `products/gongzzang/apps/web/tests/unit/map/vector-tile-manifest.test.ts`
- Create: `products/gongzzang/apps/web/tests/unit/map/foundation-vector-source-refresh.test.ts`
- Modify: `products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json`
- Modify: `products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
- Modify: `products/gongzzang/docs/architecture/platform-integration/allowed-call-matrix.v1.json`
- Modify: `products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json`

- [x] **Step 1: Write strict manifest fetch tests**

Cover:

- schema version 1·2만 정확히 허용하고 version 3은 거부한다.
- the existing `NEXT_PUBLIC_TILES_MANIFEST_URL` and
  `/catalog/v1/vector-tiles/manifest` resolver remains schema-v1-only for anchors, while v2 always
  resolves from `NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL` plus the exact
  `/catalog/v1/vector-tiles/runtime-manifest` path;
- a v2 parcel response suppresses only the legacy v1 parcel artifact, never either v1 anchor;
- ETag retained and sent as `If-None-Match`;
- 304 returns the existing manifest without reparsing;
- v2 materializes each publication unit's own tile URL;
- `//` cannot be produced;
- invalid UUID/current version, source layer, identity, generation, or
  `refresh_after_seconds != 4` rejects the update.

- [x] **Step 2: Write atomic source-refresh tests**

Task 1에서 선택한 capability와 같은 fake mapbox bridge를 사용한다. 다음을 확인한다.

- only publication units whose per-unit generation changed are retargeted;
- parcel source retains lowercase `promoteId: "pnu"`;
- the `parcels` v2 switch does not retarget or duplicate the existing direct legacy anchor sources;
- no old and new source remain together;
- 실패한 새 source는 current source descriptor를 유지한다. immutable static retention은 정확한
  이전 release를 반환하며, 이전 dynamic URL은 최신 commit projection으로 가는 비역사적 route임을
  명시적으로 테스트한다.
- cleanup stops timers and aborts fetches.

- [x] **Step 3: Consolidate the layer registry**

source ID, source-layer 기대값, `promoteId`, style dependency group을
`foundation-vector-layer-registry.ts`에 모은다. runtime 문자열 중복을 제거한다. product design이
생기기 전에는 building style을 추가하지 않으며, 향후 registry entry를 추가할 때 publication-state
copy가 필요하지 않게 만든다.

- [x] **Step 4: Implement conditional polling**

마운트되고 표시되는 동안 4초마다 polling하되 진행 중 요청은 최대 하나로 한다. 초기 phase에서는
visibility restore 즉시 확인하고 hide/unmount 시 abort하며 실패 뒤 bounded exponential backoff를
사용한다. steady-state 상한이 visible map당 `0.25` requests/second이고 timer가 겹치지 않음을
fake-timer test로 증명한다. freshness에는 Catalog endpoint를 직접 사용하고 R2는 boot/distribution
projection으로만 둔다.

- [x] **Step 5: Implement the proven reload strategy**

Task 1에서 증명한 전략만 사용한다. 일반 static/dynamic 전환에서는 style metadata를
보존한다. `source_layer`, zoom 또는 `feature_id_property`가 바뀐 매니페스트는 완전히
검증한 뒤 다시 등록해야 한다.

- [x] **Step 6: Run unit and full web tests**

```bash
pnpm -C products/gongzzang/apps/web test
pnpm -C products/gongzzang/apps/web probe:naver --grep "vector source reload"
```

기대 결과: 모든 Vitest 테스트가 통과하고 live probe가 5초 안에 두 번째 타일 URL을 확인한다.

- [x] **Step 7: Advance the Gongzzang provider-contract pin**

생성된 Foundation OpenAPI byte를 정확히 복사한다.
`platforms/foundation-platform/docs/openapi/catalog.v1.json` into Gongzzang's
`crates/foundation-platform-client/openapi/catalog.v1.json`, update the pin's lowercase SHA-256, and
run `catalog_contract_pin`. The snapshot, pin, strict Zod parser, and source-refresh tests must land
before any environment enables Foundation's v2 capability gate. The existing contract-pin test is the
mechanical checksum guard; the Foundation flag is the fail-closed rollout gate. No intermediate task
commit is independently deployable with v2 enabled.

browser→Foundation runtime-manifest GET을 Gongzzang의 명시적 allowed-call matrix에 CORS, conditional
request, timeout, bounded polling, no service credential을 갖춘 anonymous public contract로 추가한다.
일치하는 route-exposure-policy entry를 `planned`에서 `active`로 올리고, 같은 commit에서 diagnostic
launch source를 legacy v1 R2/CDN manifest에서 Foundation Catalog v2 runtime endpoint로 바꾼다.
contract test는 정확한 path·status·exposure·no-credential control이 Gongzzang 두 policy file,
Foundation traffic/auth registry와 OpenAPI에서 일치함을 요구해야 한다.

- [x] **Step 8: Commit**

```bash
git add products/gongzzang/apps/web \
  products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json \
  products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json \
  products/gongzzang/docs/architecture/platform-integration/allowed-call-matrix.v1.json \
  products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json
git commit -m "feat(gongzzang): refresh complete Foundation tile sources"
```

## Task 11: Prove the Complete State Machine End to End

**Files:**
- Modify: `scripts/tiles/{tiles-slice-proof.sh,fixture.sql,compose.yaml}`
- Create: `scripts/tiles/vector-tile-runtime-manifest-v2.local.json`
- Preserve unchanged: `scripts/tiles/vector-tile-manifest.local.json`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/{tiles_slice_contract.rs,tiles_slice_harness_contract.rs}`
- Create: `platforms/foundation-platform/infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql`
- Preserve unchanged: `platforms/foundation-platform/infra/db/seeds/local_vector_tile_manifest.sql`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/local_vector_tile_seed_contract.rs`
- Create: `products/gongzzang/apps/web/tests/probes/foundation-vector-source-publication.probe.ts`
- Modify: `products/gongzzang/apps/web/playwright.probes.config.ts`

- [ ] **Step 1: Make the contract test fail on mixed sources**

manifest v2를 parse하고 각 publication unit이 정확히 하나의 release/source를 선택하는지 검증한다.
manifest unit, Martin catalog source, source layer, feature ID, canonical data revision과 정확한
Iceberg snapshot을 대조한다. Foundation polygon unit의 comma-separated/composite URL을 거부하고
`parcels` PMTiles archive가 `parcels` vector layer 하나만 노출하는지 요구한다. HTTP tile template은
Compose service가 loopback으로 publish한 URL에만 허용하고 같은 manifest가 production HTTPS publish
gate에서 거부되는지 증명한다. 변경하지 않은 v1 local manifest는 `parcel_anchor_aggregate`와
`parcel_anchor`에만 읽고 v2 parcels가 active가 되면 v1 parcel artifact가 억제됨을 증명한다.
v2 fixture와 seed를 별도 file로 두어 legacy contract를 덮어쓰거나 의미를 바꾸지 못하게 한다.
하네스는 먼저 fail-closed 기본값이 v2를 publish하지 않는지 증명한 뒤 opt-in한다.
`FOUNDATION_TILE_RUNTIME_MANIFEST_V2_ENABLED=true`는 Task 10 consumer/pin test가 통과한 뒤에만 사용한다.

- [ ] **Step 2: Add add/modify/delete fixtures**

안정적인 필지 ID 세 개를 사용한다.

- add a new parcel;
- modify an existing parcel so old and new footprints differ;
- delete an existing parcel.

기대 결과: 동적 응답은 새 대상 집합만 포함하고 이전 geometry는 포함하지 않는다.

- [ ] **Step 3: Prove stale-build rejection**

Start static build at R30, activate R31 before promotion, and assert:

```text
STATIC promote rejected expected_release=R30 current_release=R31
DYNAMIC tile OK generation=R31
```

R30 archive를 decode해 내부가 일관되고 혼합되지 않았는지 증명한다.

- [ ] **Step 4: Build and promote the current release**

R31을 build하고 create-only로 upload한 뒤 Martin prefix discovery를 기다려 decode하고 CAS-promote한다.
dynamic R31과 static R31의 feature ID와 예상 geometry hash가 일치하는지 확인한다.

- [ ] **Step 5: Prove same-data serving rollback**

static R31에서 보존된 dynamic R31로 rollback하고 global manifest generation은 증가하되 data
revision과 feature set은 바뀌지 않는지 확인한다.

- [ ] **Step 6: Prove the five-second client path**

같은 Catalog/PostGIS database에 연결된 migration된 Foundation API를 Compose harness에 붙인다.
Playwright의 기존 `webServer` 경계로 Gongzzang을 그 API와 두 Martin service에 대해 실행한다.
local Foundation base URL, v2 manifest URL과 Naver client ID는
`playwright.probes.config.ts`를 통해 명시적으로 전달하며 production credential을 만들지 않는다.
새 Playwright spec은 다음을 수행해야 한다.

1. dynamic R31으로 실제 Naver map을 열고 Catalog manifest generation이 갱신된 parcel source를
   선택할 때까지 기다린다.
2. page를 연 채 harness의 Rust control container에서 `promote-spatial-tile-build`를 호출한다.
3. API response 또는 보존된 evidence에서 Catalog commit timestamp/generation을 읽는다.
4. static R31 source URL의 첫 network request와 성공한 source-data event를 관찰한다.
5. 이전·새 parcel source가 함께 존재하거나 경과 시간이 5초를 넘으면 실패한다.

이 테스트를 mocked mapbox bridge로 통과시키지 않는다. mock은 Task 10 단위 테스트가
소유한다. 반복 실행하며 모든 prelaunch 증명은 5초 안에 끝나야 한다. 다음 정보만 담은
비식별 checksum 주소
`target/spatial-tile-publication/refresh-observations/<id>.json` record containing only generation,
commit time, first-tile time, duration, outcome, and test environment. Task 12 ingests the same schema;
screenshots, credentials, feature IDs, and camera coordinates are forbidden.

- [ ] **Step 7: Run twice for idempotency**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
pnpm -C products/gongzzang/apps/web exec playwright test \
  --config playwright.probes.config.ts foundation-vector-source-publication.probe.ts
```

Expected both runs exit 0 with unique artifacts and no leaked containers. With R2 credentials, run
real mode twice and retain evidence; never overwrite or delete previous evidence.

- [ ] **Step 8: Commit**

```bash
git add scripts/tiles platforms/foundation-platform/infra/db/seeds \
  platforms/foundation-platform/services/foundation-api/tests \
  products/gongzzang/apps/web/tests/probes/foundation-vector-source-publication.probe.ts \
  products/gongzzang/apps/web/playwright.probes.config.ts
git commit -m "test: prove single-source tile publication lifecycle"
```

## Task 12: Add Scheduling, Readiness, Metrics, and Operator Runbook

**Files:**
- Create: `platforms/foundation-platform/scripts/tiles/reconcile-spatial-tile-publication.sh`
- Create: `platforms/foundation-platform/infra/systemd/foundation-spatial-tile-publication.service`
- Create: `platforms/foundation-platform/infra/systemd/foundation-spatial-tile-publication.timer`
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_refresh_observation.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/{main.rs,main_command_tests.rs}`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-application/src/ports.rs`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-infrastructure/src/{sqlx_repository.rs,lib.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/{state.rs,routes/mod.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/routes/tests/health_and_metrics.rs`
- Modify: `platforms/foundation-platform/infra/observability/prometheus/foundation-api.rules.yml`
- Modify: `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/deploy_contract.rs`
- Create: `scripts/tiles/active-map-refresh-soak.sh`

- [ ] **Step 1: Write command and deployment contract tests**

Require one Rust command SSOT:

```text
foundation-outbox-publisher reconcile-spatial-tile-publication
foundation-outbox-publisher publish-spatial-tiles-now --unit parcels
foundation-outbox-publisher record-spatial-tile-refresh-observation --evidence-json <path>
```

shell/systemd 래퍼는 이 명령과 `plan-spatial-tile-build`가 출력한 정확히 고정된 Compose
argv를 실행한다.
`plan-spatial-tile-build`; it contains no publication decisions and rejects any unrecognized service,
image digest, mount, or output path. The deployed Rust publisher remains Docker-socket-free.

- [ ] **Step 2: Implement debounce, nightly reconcile, and publish-now**

명령은 다음을 보장한다.

- build 중에도 dynamic serving을 유지한다.
- queued change를 coalesce한다.
- stale job을 superseded로 표시한다.
- Martin discovery/validation을 기다린다.
- Task 6 CAS를 통해서만 promote한다.
- idempotency key로 안전하게 재시도한다.

- [ ] **Step 3: Persist and export operational evidence**

관측 명령은 Task 11 증거 schema/checksum을 검증하고 미래 timestamp나
negative duration을 거부하고 `catalog.vector_tile_refresh_observation`에 idempotent하게 insert한다.
Build·discovery·promotion·projection timing은 publication/build ledger에 계속 보존하며 command-
style publisher가 in-process Prometheus endpoint를 제공한다고 가장하지 않는다.

Foundation API의 기존 `/metrics` query path가 database ledger에서 cumulative counter와 histogram
bucket을 만들게 확장한다. build result/duration, superseded build, projection lag, Martin discovery
lag, promotion conflict, manifest projection lag, synthetic active-map refresh duration/outcome을
포함한다. 이 database-to-API path가 단일 scrape boundary다.

- [ ] **Step 4: Add readiness without making `/readyz` perform remote tile I/O**

Readiness fails when:

- dynamic is active and projection generation lags;
- static is active and the reconciler's bounded background probe has not recently decoded the exact
  Martin route;
- Catalog runtime manifest와 active release가 일치하지 않는다.

세 경우 모두에 repository/state method와 readiness test를 추가한다. `/readyz`는 transaction으로
기록된 readiness evidence를 읽으며 각 health request마다 R2/Martin을 fetch하거나 MVT를 decode하지 않는다.

- [ ] **Step 5: Add the prelaunch rolling SLO guard**

`active-map-refresh-soak.sh`가 disposable browser proof를 반복 호출하고 Rust command를 통해 각
observation을 기록한 뒤 rolling 24-hour result를 계산한다. test fixture는 성공률이 99% 미만이거나
prelaunch sample 하나라도 5초를 넘으면 script가 실패하게 해야 한다. export한 observation
counter/histogram에 `success_ratio_24h < 0.99`와 5초 위반을 검사하는 Prometheus rule을 추가한다.

이것은 합성 prelaunch/deployment 증거이며 실제 사용자 monitoring이라고 거짓 주장하는 자료가
아니다. 같은 metric이 표준 Gongzzang OpenTelemetry/Sentry RUM 경계에서 공급될 때까지 runbook은
production SLO claim을 차단한다. 일반 frontend observability platform 구축은 이 spatial slice의
범위 밖이다.

- [ ] **Step 6: Update the runbook**

Document:

- local·real-R2 command
- 전용 private serving-derivative bucket과 분리된 write/read credential
- WAP candidate retention/reconciliation;
- dynamic edit, nightly schedule, publish-now, static promotion, and same-data rollback;
- failure recovery without tombstones;
- exact stop conditions for future partitioning;
- truthful statement that real R2 is unproven when credentials/evidence are absent.
- v2 rollout order: deploy the dual-version Gongzzang consumer and exact OpenAPI pin first, then enable
  the Foundation capability gate;
- the dynamic cache invariant (`martin-dynamic cache: disable`, query-free route, and `no-store`);
- the difference between prelaunch synthetic evidence and a later production RUM SLO.

- [ ] **Step 7: Run deployment contract tests and commit**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p foundation-api deploy_contract
git add platforms/foundation-platform/scripts/tiles \
  platforms/foundation-platform/infra \
  platforms/foundation-platform/crates/catalog \
  platforms/foundation-platform/services/foundation-api \
  platforms/foundation-platform/services/foundation-outbox-publisher \
  platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md \
  scripts/tiles/active-map-refresh-soak.sh
git commit -m "feat(foundation): operate spatial tile publication"
```

## Task 13: Run the Authoritative Verification and Review

**Files:**
- Verify all changed files.

- [x] **Step 1: Run formatting/diff/secret checks**

```bash
git diff --check
scripts/ci/gitleaks-scan.sh
```

기대 결과: whitespace 오류와 커밋된 secret이 없다.

- [x] **Step 2: Run Foundation verification in the pinned Rust container**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo xtask verify foundation
scripts/verify/integration.sh foundation
```

기대 결과: PASS.

- [x] **Step 3: Run Gongzzang verification in the pinned Rust container**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo xtask verify gongzzang
pnpm -C products/gongzzang/apps/web test
```

기대 결과: PASS.

- [ ] **Step 4: Run the local proof twice and real R2 when configured**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
```

전용 테스트 인증 정보가 있을 때:

```bash
scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

예상 결과: dynamic add/modify/delete, stale-build rejection, static R2 prefix discovery, matching
feature, same-data rollback과 5초 active-map refresh가 모두 통과한다.

- [ ] **Step 5: Request code review**

reviewer가 테스트 녹색 상태뿐 아니라 아키텍처 불변식도 확인하게 한다.

- [x] **Step 6: Confirm a clean branch**

```bash
git status --short --branch
git log --oneline --decorate -15
```

기대 결과: `feat/spatial-publication-state-machine`이 깨끗하고 `main`에는 커밋이 없다.
