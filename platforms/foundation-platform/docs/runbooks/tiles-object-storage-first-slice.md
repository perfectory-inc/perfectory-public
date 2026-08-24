---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# 객체 저장소 우선 타일 단계

## 범위와 증거 상태

이 런북은 출시 전 Foundation 단계 하나를 다룬다. 한 산업단지의 세 필지를 두 Martin 경로로
Mapbox Vector Tile로 제공한다.

- **Dynamic:** 명시적 PostGIS view → Martin → MVT.
- **Static:** 같은 view → `martin-cp` → MBTiles → PMTiles → Martin → MVT. 로컬 파일 또는 검증 전용
  R2 HTTP Range를 사용할 수 있다. 운영은 아래의 인증된 S3 호환 R2 경로를 사용한다. Martin 1.12의
  `pmtiles.paths`가 운영 검색 지점이며 private R2 S3 endpoint와 읽기 전용 자격증명을 사용하고
  release PMTiles만 허용한다.
  실제 R2 증명은 private S3 경로를 직접 사용하며 public HTTP Range는 선택적인 레거시 호환 확인이다.
- **소비자:** 검사된 로컬 매니페스트가 기존 Gongzzang Naver Maps/mapbox-gl 통합이 렌더러 변경 없이
  읽을 수 있는 Martin URL로 해석된다.

증명은 z11 집계 응답과 z14 필지/정확 anchor 응답을 대표값으로 검사한다. archive는 광고한 z0~z16
전체 zoom으로 렌더링하고, 압축을 푼 각 zoom을 다시 디코드해 z0~11 집계 전용, z12~13 정확 anchor
전용, z14~16 필지+정확 anchor 범위를 강제한다. 누락·추가 layer, 잘못된 feature 수, 잘못된
pnu/complex 식별자, 렌더링 불가능한 점·폴리곤, 대표 타일의 dynamic/static 식별자 또는 MVT 바이트
불일치는 모두 거부한다.
v2 필지 layer는 정본 소문자 `pnu`만 내보낸다. 레거시 fixture view의 대문자 `PNU` alias는 고정된
v1 증명 계약에만 남긴다. 집계 렌더링은 style zoom 12 미만에서 끝나므로 z11까지 보이고 z12의
정확 anchor가 시작될 때 빈 구간이 없다.

가장 최근 실제 R2 실행은 정본
`foundation-platform-tile-derivatives-prod` bucket에서 유일한
`tiles-slice-proof/<run-id>/` prefix만 사용해 검증했다. Martin은 인증된 S3 origin으로 private
PMTiles를 읽고 pnu `9999900000000000001`을 포함한 dynamic/static 일치 feature 7개를 디코드했다.
업로드는 `If-None-Match: *`를 사용하며 harness는 객체를 덮어쓰거나 삭제하지 않는다. 로컬 lane은
오프라인 재현용으로 남긴다. 이는 정확성 slice이지 운영 rollout이나 전국 규모 부하 테스트가 아니다.

## 소유권과 저장 모델

Foundation은 정본 필지·건물·산업단지 geometry, 계보, 승인, 정적 타일 빌드·공개·rollback을 소유한다.
Gongzzang은 공개된 HTTP/manifest 계약만 소비하며 Foundation 객체를 쓰지 않는다.

R2는 불변 bytes를 보관하지만 canonical data와 serving derivative는 서로 다른 private security
zone이다. canonical/source geometry는 lakehouse bucket에 남긴다. 별도의 private serving-
derivative bucket에는 공개 가능한 불변 PMTiles serving release만 둔다.
각 release에는 불변 PMTiles archive, TileJSON, manifest가 포함된다. PMTiles는 serving 파생물이지
편집 가능한 geometry 원본이 아니다. PostGIS는 Catalog가 선택한 R2/Iceberg snapshot과 감사된 공개
입력에서 재구성 가능한 완전한 warm serving projection이며 유일한 정본은 아니다. Static serving은
PostGIS의 지속적인 타일 렌더링 부하만 줄이고 warm projection 자체를 제거하지 않는다.

Foundation Catalog metadata는 active release, data/serving generation, 계보, 승인, rollback 이력의
권위다. R2에는 불변 바이트를 둔다. 표준 R2 token은 bucket 범위로 분리한다. Martin은 derivative
bucket 읽기 전용 자격증명을, publisher는 별도 쓰기 자격증명을 갖는다. release prefix는 검색과
create-only key를 제한할 뿐 IAM 경계가 아니다.

수명 주기는 다음과 같다.

1. Catalog가 선택한 Iceberg snapshot에서 branch하고 Iceberg WAP으로 승인 변경을 기록·검증한 뒤
   해당 단위의 **완전한** PostGIS projection을 준비한다.
2. 같은 dynamic Martin source를 디코드하고 원자적으로 선택한다. 단위는 하나의 완전한 dynamic source로
   즉시 보인다.
3. publication unit을 key로 debounced 정적 공개를 queue한다. debounce 값은 UI나 이 문서가 아니라
   publisher 설정이 소유한다.
4. 관리자가 **Publish now**를 선택하면 debounce를 건너뛴다.
5. 선택된 PostGIS generation을 고정하고 완전한 불변 archive를 다시 만들어 검증한 뒤 create-only로
   업로드한다.
6. Martin이 archive를 발견·디코드한 뒤 전체 단위를 `DynamicPostgis`에서 `StaticPmtiles`로 CAS한다.
   이전 source와 새 source를 동시에 렌더링하지 않는다.
7. 누락·실패·미승격 승인 버전에 대해 매일 retry/reconciliation을 실행한다.

추가·수정·삭제는 같은 완전 소스 전환을 사용한다. Foundation overlay, tombstone, client feature
억제 계약은 없다. active-release CAS를 잃은 정적 빌드는 `SUPERSEDED`가 되어 오래된 geometry를
되살릴 수 없다.

이 slice는 scheduler나 admin UI를 설치하지 않는다. 운영 scheduler는 `Asia/Seoul` 날짜마다 한 번
nightly reconciliation을 실행하고 마지막 성공 시각과 lag를 노출해야 한다. 정확한 시각과 debounce
기간은 배포 설정이 소유한다. 출시 시 zero-downtime은 필수가 아니지만 검증·순서·소스 완전성·rollback
정확성은 필수다.

## 사전 조건

- Windows 호스트의 Bash(Git Bash 또는 동등한 표준 shell)에서 저장소 루트부터 실행한다.
  PowerShell harness를 추가하지 않는다.
- Compose v2가 포함된 Docker Engine이 준비되어야 한다.
- harness는 저장소 contract test가 검증한 digest-pinned PostGIS, Martin, Protomaps PMTiles,
  Rust image만 pull한다.
- R2 실행에서 shell tracing(`set -x`)를 켜지 않는다. harness는 R2 변수를 읽기 전에 상속된
  xtrace를 끄고 curl credential을 stdin으로 전달하며
  disables user curl configuration at the executable boundary. Callers must still keep credentials
  and presigned URLs out of surrounding job logs.

하네스는 고유한 Compose project와 일회용 PostGIS 저장소를 사용하고 종료 시 container를 정리한다.
저장소에 기록된 모든 migration을 production `foundation-migrate` SQLx runner로 적용한 뒤
`scripts/tiles/fixture.sql`을 실행하며 개발자나 production database는 수정하지 않는다.
`sqlx::Migrator::run`이 migration SSOT다. embedded migration set은 dirty ledger·누락 version·
checksum drift를 거부한 뒤 pending migration을 적용한다. proof·disposable integration harness·
Foundation CI는 같은 runner를 호출하며 SQLx의 private-ledger나 migration-count logic을 복제하지
않는다. API build script는 migration directory 자체를 감시하므로 이미 embedded된 file이 바뀌지
않아도 migration file이 추가·삭제되면 cached `foundation-migrate`를 다시 build한다.

v2 로컬 fixture는 추가 방식이다. `infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql`은
one complete `parcels` dynamic release and never rewrites the frozen v1 seed. The stable dynamic
Martin URL is query-free; `serving_postgis.parcel_boundary_current` follows the one runtime-manifest
pointer to the selected `data_revision`. Martin's dynamic cache is disabled with the supported
`cache: disable` setting.

## 공식 행정 경계 공간 경로

경계 생성기는 타일 전환과 의도적으로 분리한다.

```text
official GeoJSON (EPSG:4326)
  -> write-official-administrative-boundary-source-snapshot
  -> write-administrative-spatial-scope-registry
  -> publish-administrative-boundary-postgis
  -> complete admin dynamic release + runtime-manifest CAS
  -> Martin /admin/{z}/{x}/{y}
```

소스 writer는 Polygon/MultiPolygon geometry와 해시를 보존한다. 레지스트리는 누락되거나
or changed geometry hash. The PostGIS publisher requires an existing Catalog revision, source record,
and `status=ready` registry evidence; it appends only to
`serving_postgis.administrative_unit_boundary_publication`, never to an ad-hoc table. It creates
stable units using `scope:{scope-kind}:{canonical-code}` and appends official name/code facts when
the source supplies a name. It does not fabricate sigungu/sido names from a bbox-only row.

Required publisher inputs are supplied only through the environment:

```bash
export DATABASE_URL='postgres://...'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM=1
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION='<revision UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID='<positive decimal>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID='iceberg:<snapshot-id>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID='<source-record UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY='gold/admin-boundaries/<snapshot>.geojson'

foundation-outbox-publisher publish-administrative-boundary-postgis
```

Martin은 `admin` source로 설정하지만 현재 view는 runtime-manifest pointer와 join한다.
따라서 projection만 publish해서 승인되지 않은 revision이 노출될 수 없다. 운영자는 완전한
`admin` dynamic release를 만들고 모든 publication unit을 담은 manifest를 기존 CAS function으로
promote해야 한다. 저장소의 publisher는 projection을 검증한 뒤 이 전체 작업을 수행한다. 새
release/manifest UUID와 현재 manifest UUID(첫 manifest일 때만 비워 둠), 실제 local/CDN Martin URL을
입력한다.

```bash
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM=1
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION='<revision UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID='<positive decimal>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID='<source-record UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID='<file-asset UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID='<current manifest UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID='<new release UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID='<new manifest UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE='http://127.0.0.1:3110/admin/{z}/{x}/{y}'

foundation-outbox-publisher promote-administrative-boundary-runtime
```

명령은 parcels와 기존 모든 공개 단위를 포함한 다음 완전한 manifest를 만들고 database CAS
function을 호출한다. 브라우저는 기존 MapLibre bridge로 v2 `admin` unit을 읽을 수 있으며 해당
release가 promote될 때까지 legacy v1 `admin` artifact를 fallback으로 유지한다.

### 산업단지 경계 승격

`complex` 단위도 같은 절차를 따른다. 세 가지가 다르다.

1. **revision UUID는 운영자가 정하지 않는다** — `publish-industrial-complex-boundary-postgis`가
   canonical snapshot 하나에 revision 하나를 발급하므로, 발행이 남긴
   `serving_postgis.spatial_projection_load` 행에서 적재 id와 함께 읽는다.
2. **`BRONZE_OBJECT_ID`가 추가로 필요하다.** 이 단위의 revision은 폴리곤을 꺼낸 수집 객체를
   이름한다(루트 ADR-0046). 값은 발행 명령의 성공 줄 `bronze_object_id=` 또는
   `catalog.publication_revision.bronze_object_id`에서 읽는다 — 새로 만들지 않는다.
3. **`SOURCE_RECORD_ID`는 그것과 다른 행이다.** 이쪽은 승격이 만드는 release의 계보 기록이고,
   `catalog.vector_tile_release.source_record_id`가 아직 요구한다. 수집 객체가 아니다.

```bash
psql "$DATABASE_URL" -At -F '|' -c "SELECT load.id, load.data_revision, revision.bronze_object_id
  FROM serving_postgis.spatial_projection_load AS load
  JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = load.publication_unit_id
  JOIN catalog.publication_revision AS revision ON revision.id = load.data_revision
 WHERE unit.unit_key = 'complex' AND load.status = 'succeeded'
 ORDER BY load.started_at DESC LIMIT 1;"

export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CONFIRM=1
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID='<load UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION='<revision UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID='<positive decimal>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_BRONZE_OBJECT_ID='<bronze-object UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID='<source-record UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID='<file-asset UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID='<current manifest UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID='<new release UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID='<new manifest UUID>'
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE='http://127.0.0.1:3110/complex/{z}/{x}/{y}'

foundation-outbox-publisher promote-industrial-complex-boundary-runtime
```

`EXPECTED_MANIFEST_ID`를 비워 두지 말 것. 이미 pointer가 있는 배포에서 그것을 생략하면 CAS
전제가 "pointer 없음"이 되어 승격이 거부된다 — 그리고 두 승격이 겹칠 때 나중 것이 앞의 것을
조용히 덮는 것을 막는 것이 이 변수다.

### 폐기 가능한 종단 간 smoke 증명

공식 정부 경계 snapshot이 없으면 예약 좌표를 사용하는 synthetic fixture로 저장소의 증명을
실행한다.

```bash
bash scripts/tiles/boundary-slice-proof.sh
```

일회용 PostGIS와 Martin 컨테이너를 시작하고 synthetic 법정동·시군구 source snapshot을
작성한 뒤 레지스트리를 검증하고 PostGIS geometry를 발행하며 CAS runtime manifest를 승격하고
결과 Martin MVT를 디코드한다. 같은 컨테이너로 `complex` 단위도 끝까지 통과시키며, 거기서는
**승격 전후 대비**까지 확인한다 — 적재는 끝났지만 아무 manifest도 그것을 고르지 않은 상태에서
Martin이 자른 타일에는 `complex` 레이어가 없고, 승격 뒤 같은 타일에는 두 폴리곤이 들어 있다.
이 fixture는 의도적으로 비공식 데이터이므로 운영에 승격하지 않는다. 실제 release 전에는 검증된
공식 source snapshot으로 교체한다.

## Local PMTiles fallback

R2 증명 변수가 export되어 있지 않은지 확인한 뒤 증명을 두 번 실행한다.

```bash
for name in \
  R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_TILES_TEST_BUCKET_NAME \
  R2_ENDPOINT R2_TILES_READ_BASE_URL R2_TILES_READ_URL R2_TILES_OBJECT_KEY; do
  unset "$name"
done

scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
```

두 실행 모두 exit code 0이어야 한다. 중요한 출력은 다음과 같다.

```text
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected pnu=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (LOCAL PMTiles fallback)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

각 실행은 `target/tiles-slice-proof/<run-id>/` 아래에 로컬 증거를 보존한다. dynamic/static
PBFs and response headers, canonical identity dumps, unpacked logical tiles, and
`tiles-slice-proof/local/foundation-static.{mbtiles,pmtiles,tilejson.json}`. These generated files
are proof output, not source-controlled artifacts. The deterministic proof archive contains 17
logical MVT entries with 3,214 total logical tile-payload bytes; the checked proof manifest records
those compatibility statistics and fails if they drift.

증명 뒤 저장소 검증 SSOT와 전체 웹 모음을 실행한다.

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc \
  cargo xtask verify foundation

docker run --rm -v "$PWD:/workspace" -w /workspace \
  rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc \
  cargo xtask verify gongzzang

pnpm -C products/gongzzang/apps/web test
```

static 빌드 chain은 의도적으로 명시적이다. zoom 범위를 제한한 `martin-cp` 세 번이 하나의
MBTiles 파일에 append되어 각 계층이 선언한 tile zoom 범위에서만 존재한다.

```text
PostGIS snapshot
  -> martin-cp aggregate z0-11 (new MBTiles)
  -> martin-cp exact anchors z12-13 (append)
  -> martin-cp parcels + exact anchors z14-16 (append)
  -> composite Martin TileJSON vector-layer metadata
  -> mbtiles validate
  -> pmtiles convert
  -> pmtiles verify
  -> Martin
```

`martin-cp`는 PMTiles를 쓰지 않는다. `mbtiles diff/apply-patch`는 MBTiles build/sync artifact에만
작동하며 local 또는 remote PMTiles archive를 in-place로 갱신하지 않는다.

## Real R2 proof mode

정본 운영 형태의 증명은 Foundation tile-derivatives 버킷과 버킷 범위 publisher/read
쌍을 사용한다. 하네스는 고유한
`tiles-slice-proof/<run-id>/` prefix and rejects any other bucket through the checked-in R2
connection contract. Standard R2 API-token scoping is bucket-level, so the prefix is a second
create-only guard, not an IAM boundary. Lakehouse, Bronze, recovery, backup, and other data buckets
are never valid tile targets.

legacy 증명 전용 HTTP 경로는 공개 원격 PMTiles 읽기를 증명하기 위해 HTTPS Range URL을
의도적으로 요구한다. 선택 사항이다. 운영 형태 경로가 정본이다. Martin은 인증된
S3 호환 접근으로 비공개 derivative 버킷을 읽으므로 공개 R2 URL이나 R2 CORS 정책이
필요하지 않다.

모든 값은 환경이나 secret manager에서 공급하며 이 저장소의 파일에 넣지 않는다.

반복 가능한 정본 실행에서는 타일 인증 정보가 무시된
`platforms/foundation-platform/.env.local` profile (or the normal CI secret manager). The canonical
mode is explicit and opt-in:

```bash
TILES_SLICE_USE_CANONICAL_TILE_R2=1 scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
TILES_SLICE_USE_CANONICAL_TILE_R2=1 scripts/tiles/tiles-slice-proof.sh
```

스크립트는 `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_*` namespace만 읽고 create-only
업로드에는 publisher 키를 사용하며 별도 Martin 읽기 전용 키를 운영 설정에 주입한다.
인증값은 출력하지 않는다. 이전 `FOUNDATION_PLATFORM_R2_TILE_PROOF_*` tuple은 선택적인
공개 HTTP readback 경로에 남겨 둔다.

```bash
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID='<Cloudflare account ID>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID='<R2 test access key ID>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY='<R2 test secret access key>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET='<dedicated bucket containing tiles-slice-proof>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT="https://${FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID}.r2.cloudflarestorage.com"
export FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL='<HTTPS r2.dev or bound test custom-domain bucket URL>'

unset R2_TILES_READ_URL R2_TILES_OBJECT_KEY

scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

preflight는 Docker나 R2 요청을 하지 않는다. 저장소의 보호 버킷
SSOT is missing/empty, rejects every declared production/recovery bucket, and applies the same
3-63 character lowercase-letter/digit/hyphen rule as the Foundation lakehouse registry (including
the no-leading/trailing/double-hyphen constraint).

부분적인 R2 설정은 오류다. 로컬 경로에서는 모든 변수를 unset하고, 원격 경로에서는 전체
set. An exported-but-empty R2 variable also counts as partial configuration and fails rather than
silently selecting local fallback. The endpoint must be the account's exact R2 S3 endpoint. With
`R2_TILES_READ_BASE_URL`, the harness creates
`tiles-slice-proof/<run-id>/foundation-static.pmtiles` and appends that key to the base URL. The
base URL must be HTTPS and contain no query or fragment.

그 외 조건이 맞는 query 없는 읽기 URL은 상호 배타적인 exact-URL 모드를 사용한다.

```bash
unset R2_TILES_READ_BASE_URL
export R2_TILES_OBJECT_KEY='tiles-slice-proof/<unique-run-id>/foundation-static.pmtiles'
export R2_TILES_READ_URL='<exact query-free HTTPS read URL for that key>'

scripts/tiles/tiles-slice-proof.sh
```

경로는 정확히 `R2_TILES_OBJECT_KEY`로 끝나야 한다. Martin
1.12 must receive a query-free HTTP PMTiles source. Setting both read modes, omitting both, or
supplying a key outside `tiles-slice-proof/` fails before upload.

운영 publisher/serving 경계에서는 일반 `R2_*` 환경을 재사용하지 않는다. Rust
Rust preflight command is:

```bash
foundation-outbox-publisher validate-tile-derivative-r2
```

산업단지 경계의 운영 정적 발행은 아래 단일 명령으로만 수행한다. 값은 사전에 secret/env
주입기로 제공하고, 명령 기록에는 이름만 남긴다. 이 명령은 활성 dynamic TileJSON의 bounds와
zoom을 읽어 `martin-cp -> mbtiles validate -> pmtiles convert -> pmtiles verify`를 실행하고,
create-only 업로드의 exact GET 재해시와 static Martin MVT 바이트 일치를 통과한 뒤에만 build
ledger를 검증 완료로 기록하고 CAS pointer를 전환한다.

```bash
env \
  DATABASE_URL="${DATABASE_URL:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLISH_CONFIRM="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLISH_CONFIRM:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_DYNAMIC_MARTIN_BASE_URL="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_DYNAMIC_MARTIN_BASE_URL:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_STATIC_MARTIN_BASE_URL="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_STATIC_MARTIN_BASE_URL:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLIC_TILES_BASE_URL="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLIC_TILES_BASE_URL:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_MARTIN_CONFIG_PATH="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_MARTIN_CONFIG_PATH:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_WORK_ROOT="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_WORK_ROOT:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_OPERATOR_STAFF_ID="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_OPERATOR_STAFF_ID:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_BUILD_IDEMPOTENCY_KEY="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_BUILD_IDEMPOTENCY_KEY:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PROMOTE_IDEMPOTENCY_KEY="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PROMOTE_IDEMPOTENCY_KEY:?}" \
  FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_TOOL_TIMEOUT_SECONDS="${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_TOOL_TIMEOUT_SECONDS:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID:?}" \
  FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY:?}" \
  foundation-outbox-publisher publish-industrial-complex-boundary-static-release
```

`martin-cp`와 `mbtiles`는 Martin `1.12.x`, `pmtiles`는 `1.31.x`가 PATH에 있어야 한다.
명령은 도구 부재·버전 불일치·timeout을 모두 실패로 기록한다. 재시도에는 같은 build/promote
idempotency key를 사용하며, 새 시도일 때만 새 key를 발급한다. 이 명령은 overwrite/delete 경로를
제공하지 않는다.

다음 환경을 요구한다: `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID`, `..._ENDPOINT`,
`..._BUCKET`, 쓰기용
`..._WRITER_ACCESS_KEY_ID`/`..._WRITER_SECRET_ACCESS_KEY`, and separate Martin
`..._READER_ACCESS_KEY_ID`/`..._READER_SECRET_ACCESS_KEY`. The bucket must be a dedicated tile/derivative
bucket and the immutable prefix is fixed to `gold/vector-tiles/releases`. Release objects are
derived mechanically as `gold/vector-tiles/releases/{publication_unit}-{release_id}.pmtiles`;
callers cannot supply an arbitrary key. The publisher uses the writer credential only for the
create-only PUT and the separate reader credential for the exact full-GET rehash; Martin receives
that same read-only credential. Never use a prefix as an IAM boundary or point either credential at a
lakehouse, Bronze, recovery, or backup bucket.

### Production bucket naming

Lakehouse 버킷과 같은 세 부분 규칙인 `<owner-service>-<purpose>-<environment>`을 사용하되,
serving derivative는 보존 기간·인증 정보·CDN 노출이 Bronze/Silver/Gold lakehouse 데이터와
다르므로 별도 버킷에 둔다. 운영 버킷은 다음과 같다.

```text
foundation-platform-tile-derivatives-prod
```

해당 Lakehouse 버킷은 `foundation-platform-lakehouse-prod`로 유지하며 타일 버킷 대신
사용하지 않는다. 미래 환경도 같은 형태를 따른다. 예를 들면
`foundation-platform-tile-derivatives-staging` and `foundation-platform-tile-derivatives-ci`.
프로비저닝은 인프라/계정 자동화로 수행하고 이 문서에 기록한다. 운영자가 이름이 다른
버킷을 조용히 만들고 secret만 바꾸면 안 된다.

정본 하네스는 `If-None-Match: *`로 업로드하고 인증된 HEAD를 수행하며
the ETag, content length, and checksum metadata to match the local archive. Static Martin then
discovers that private R2 prefix and repeats the decoded feature comparison. The optional public
HTTP lane additionally performs a full public readback SHA-256 comparison, a full GET, and a
`206 Partial Content` Range check. Success contains:

```text
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected pnu=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (REAL R2 via Martin S3 origin)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

고유한 증명 archive는 증거로 R2에 의도적으로 남긴다. 하네스는
allowlist of non-secret response fields (status, ETag, content length, and checksum metadata), plus
the optional public readback/Range evidence and `r2-evidence.txt`. The PutObject response body is
discarded instead of being written to disk. Raw response headers and unverified public-readback or
Range bodies are deleted by the EXIT cleanup on both success and failure. The evidence file records
the dedicated prefix, exact key, local checksum, byte count, and ETag.
Preserve those files with the proof timestamp and command result. The harness provides no R2 delete
path. Any later retention cleanup is a separate, explicitly approved operation against an exact
recorded test key; it must never target a broad prefix or any production bucket.

## What the proof adapter does not mean

GZ-ADR-0036 schema v1 describes individual PBF objects:

- `object_key_prefix` is a physical R2 flat-tile prefix.
- `flat_tile_count` is the number of flat tile objects.
- `flat_tile_total_bytes` is their total object payload size.

이 slice는 PMTiles 객체 하나를 사용하고 Martin은
`/foundation_static/{z}/{x}/{y}`. Its checked manifest is intentionally marked
`proof-adapter-not-adr-0036-production`: `object_key_prefix` is a Martin route source ID and the
compatibility `flat_*` values describe archive entries/payloads, not R2 object statistics.

That is sufficient to prove the existing client's URL-first behavior, but it is not a production
GZ-ADR-0036 manifest. Foundation ADR-0004와 Gongzzang ADR-0036은 이제 공개 단위와
tagged `DynamicPostgis`/`StaticPmtiles` 소스를 정의한다. 운영 전 생성자·소비자·drift
테스트가 수용된 v2 계약을 구현해야 한다. schema v1을 조용히 재정의하지 않는다.

동결된 v1 fixture view에는 정본 소문자 `pnu` 옆에 증명 전용 대문자 `PNU` 호환 별칭이
남아 있다. v2 publication view와 Gongzzang runtime은 정본 `pnu`를 직접 사용하며 이 별칭은
v2 Martin source나 운영 identity 계약의 일부가 아니다.

static manifest v2 경로는 release 주소를 사용한다. 증명 URL
`/foundation_static/{z}/{x}/{y}`를 다른 archive에 재사용하면 이전 매니페스트와 CDN 항목이
새롭거나 오래된 콘텐츠를 가리킬 수 있다. Dynamic 경로는 안정적이고 query가 없으며
`serving_postgis.*_current` view follows the Catalog runtime-manifest pointer.
브라우저는 `parcels`라는 논리적 Mapbox 소스 하나를 계속 소유한다. static 승격은
the validated Martin URL to the source ID derived from `{publication_unit}-{release_id}.pmtiles`.

## Production promotion checklist

승격은 새 변경 불가능 release 설명과 PMTiles 객체를 선택한다. 현재 archive를 덮어쓰거나
이전 매니페스트를 변경하지 않는다.

1. **Deploy the v2 contract first.** Foundation and Gongzzang must both pass strict v1/v2 contract
   tests before Catalog may publish schema v2. Unknown schema versions fail closed. Keep the legacy
   v1 endpoint and `gold/manifest.json` bytes unchanged for the two anchor sources; publish v2 only
   from `/catalog/v1/vector-tiles/runtime-manifest` and
   `gold/vector-tiles/runtime-manifest.json`. Publish every v2 manifest create-only to
   `gold/vector-tiles/manifests/<manifest-uuid>.json` before moving that mutable pointer.
2. **Select canonical input.** Record the exact Catalog-selected Iceberg snapshot as a decimal
   string and the UUID `data_revision`. A build never follows an arbitrary Iceberg `main` head.
3. **Freeze the complete unit.** Materialize a build-scoped PostGIS snapshot for the active dynamic
   release. Never run separate `martin-cp` passes against a mutating live projection.
4. **Build and validate once.** Run
   `PostGIS -> martin-cp -> MBTiles -> mbtiles validate -> pmtiles convert -> pmtiles verify`.
   Decode representative and boundary tiles, stable identities, zoom coverage, expected omissions,
   and every required MVT source layer.
5. **Create the immutable release.** Upload with a create-only precondition to the dedicated private
   serving-derivative bucket, for example
   `gold/vector-tiles/releases/<publication-unit>-<release-uuid>.pmtiles`. Persist the immutable
   release, source lineage, file assets, checksum, byte size, bounds, zooms, and layer IDs in
   Catalog. Never put canonical source data in this bucket.
 6. **격리된 인증 정보 사용.** 정본 Lakehouse `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*` 어댑터는
    금지한다. 타일 publisher는 버킷 범위 쓰기 인증을, Martin은 별도의 버킷 범위 읽기 전용
    인증을 사용한다. 둘 다 Bronze, lakehouse, recovery 버킷에 접근할 수 없다.
7. **Stage Martin from private R2.** Deploy the checked-in
    `scripts/tiles/martin-static-production.yaml`; inject `TILES_R2_PMTILES_PREFIX` as the
    derivative bucket's `s3://` release prefix, `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT`,
    `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION`, and the read-only
    `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID` /
     `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY` through environment/secrets. Do not
     use a named `pmtiles.sources` URL for scheduled discovery: named sources are startup snapshots.
8. **Verify the production-shaped route.** Wait for the expected release-addressed Martin source,
   then verify TileJSON layer IDs, authenticated R2 reads, health/readiness, and decoded MVT through
   the public Martin/CDN hostname. The R2 bucket itself needs no public domain.
9. **Prove CDN behavior.** Repeat identical MVT requests through the public hostname and retain
   `CF-Cache-Status`/`Age` evidence. CDN caches the immutable Martin MVT route; it does not need
   direct access to the PMTiles object. Keep semantic decode checks separate from cache checks.
10. **Configure browser policy.** Allow only the real Gongzzang origins on the Martin/CDN MVT
    endpoint. R2 CORS is irrelevant to the default server-side Martin S3 read.
11. **Compare and swap.** Only if the publication unit still selects the build's input dynamic
    release/data revision, atomically register/select `StaticPmtiles`, increment
    `serving_generation`, create a new immutable manifest UUID, increment global
    `manifest_generation`, and emit the outbox projection event. The Catalog transaction calls
    `catalog.promote_vector_tile_runtime_manifest(expected_manifest_id, next_manifest_id)`; this
    database CAS rejects stale writers and incomplete manifests before changing the one runtime
    pointer. The publisher writes the exact
    immutable manifest object create-only and verifies retry bytes before updating the active
    no-cache pointer. The pointer update uses the R2 ETag observed immediately before the write with
    `If-Match` (`If-None-Match: *` for bootstrap); `412` reloads Catalog and R2 instead of
    overwriting. A stale event never moves the pointer, even when two publisher workers interleave.
    그렇지 않으면 build를 `SUPERSEDED`로 표시한다.
12. **Verify active-map replacement.** Fetch the Catalog v2 runtime manifest, frozen v1 anchor
    manifest, and representative tiles through the production client route. Confirm the v1 parcel
    artifact and old v2 parcel source are absent, the new complete v2 parcel source is loaded, and
    both parcel-anchor plus listing sources are unchanged.

serving rollback은 같은 `data_revision`의 보존된 변경 불가능 release를 staging하고 검증한 뒤
then select it with the same expected-active-release CAS. Rollback creates a new immutable manifest;
it never edits a historical manifest or reconciles feature overlays. A business-data revert creates
a new Iceberg revision and follows the normal dynamic publication flow. Old canonical snapshots and
serving releases remain subject to explicit retention policy.

Martin 문서는 Cloudflare R2를 지원되는 S3 호환 PMTiles 저장소로 설명하며,
`pmtiles.paths`를 통한 remote-prefix polling과 named source의 시작 snapshot 동작을
[Martin file sources](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md).

## Health and observability exception

이 증명에서 사용하는 수정하지 않은 Martin 이미지는 `/health`와 `/_/metrics`를 노출한다.
이는 third-party native endpoint이므로 모노레포 규칙의 증명 전용 예외다.

운영 전 Martin 앞에 다음 endpoint를 노출하는 adapter/proxy를 둔다.

- `/healthz`: process liveness
- `/readyz`: 설정한 PostGIS 또는 PMTiles source를 읽을 수 있을 때 readiness
- `/metrics`: Martin `/_/metrics`를 뒤에 둔 protected scrape path

metrics endpoint를 인증 없는 인터넷 트래픽에 공개하지 않는다. CDN health check는 Martin의
비공개 endpoint 이름에 직접 의존하지 말고 어댑터 계약을 사용한다.

## Troubleshooting and stop conditions

- **스크립트가 local fallback을 보고함:** R2 변수가 보이지 않은 것이다. 오프라인에서는
  예상된 결과이며 real-R2 증거가 아니다.
- **부분 인증 정보를 거부함:** 전체 테스트 집합을 제공하거나 모든 R2 증명 변수를 unset한다.
  검사를 약화하지 않는다.
- **업로드가 precondition failure를 반환함:** 키가 이미 존재한다. 새 고유 증명 키를 사용하고
  덮어쓰지 않는다.
- **Range 읽기가 `206`이 아니라 `200`임:** 중단한다. 선택한 URL/CDN 경로가 Martin에 필요한
  random-access 계약을 증명하지 못했다.
- **전체 공개 readback 크기 또는 SHA-256이 다름:** 중단한다. 공개 URL이 오래됐거나 잘못
  연결됐거나 방금 업로드한 객체를 가리키지 않는다. 대표 타일 일치만으로는 부족하다.
- **직접 공개 PMTiles 실험이 zone의 cacheable-object 한도를 초과함:** 선택적 origin 경로는
  검증되지 않았다. 기본 private-R2/Martin 경로는 전체 Cloudflare edge PMTiles object가
  아니라 MVT 응답을 cache한다.
- **Static feature가 다름:** promote하지 않는다. frozen snapshot, source zoom, `count`, pnu
  문자열, `official_complex_code`, identity content encoding과 archive 변환을 확인한다.
- **Manifest `flat_*` compatibility values differ from the rendered MBTiles:** do not replace the
  check with a sentinel or skip it. Record the deterministic logical tile count/payload bytes in
  the checked proof manifest, then rebuild and run the complete proof twice.
- **Martin does not discover the new archive:** verify `pmtiles.paths`, S3 endpoint/bucket
  credentials, object prefix, immutable filename-derived source ID, and `reload_interval`. A named
  `pmtiles.sources` entry does not poll.
- **Browser CORS fails:** test the Martin/CDN MVT hostname with the real `Origin` header. R2 CORS is
  not involved in Martin's authenticated server-side S3 read.
- **운영 또는 Bronze/lakehouse/recovery bucket이 선택됨:** 쓰기 전에 중단한다.
