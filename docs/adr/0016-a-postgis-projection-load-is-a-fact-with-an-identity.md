---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-31
---

# ADR 0016: PostGIS 적재는 신원을 가진 하나의 사실이다

- Status: Accepted
- Date: 2026-07-31
- 관련: [ADR-0015 키를 가진 Catalog mutation은 원장 하나를 쓴다](./0015-one-idempotency-ledger-for-keyed-catalog-mutations.md), [ADR-0014 serving generation은 한 단위의 소스 선택만 추적한다](./0014-serving-generation-tracks-one-unit-source-selection.md), [ADR-0013 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md), [FP-ADR-0004 정적 벡터 타일 런타임 계약](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)
- 마이그레이션: `20260731000001_spatial_projection_load_ledger.sql`

## Context

`catalog.vector_tile_release.postgis_projection_revision`은 `20260724000001` 이래 존재했고
**아무도 값을 만들지 않았다.** 로컬 시드는 UUID를 지어냈고, 운영자 승격 명령은
`VALUES (..., $7, $3)`으로 `data_revision`을 두 번 바인딩했다. 둘 다 날조였고, ADR-0015가 그 값을
멱등성 요청 지문에 넣으면서 날조가 불변식이 되기 직전이었다.

더 큰 구멍이 그 아래 있었다. 게이트는 `serving_postgis`를 **한 번도 참조하지 않았고** 운영자
명령의 입력 검증도 마찬가지였다. 즉 **projection이 한 번도 적재되지 않은 리비전으로도 승격이
통과했다.** 포인터는 그 단위가 dynamic으로 살아 있다고 말하고, Martin의 뷰는 0피처를 냈으며,
어디에서도 아무 말이 없었다.

### 왜 이 값이 `data_revision`과 같을 수 없나

한 리비전을 다시 materialise하는 일이 이미 두 갈래로 일어나고, 둘 다 기록되지 않는다.

| 위치 | 형태 | 결과 |
| --- | --- | --- |
| v2 시드 | `ON CONFLICT (data_revision, pnu) DO UPDATE` | 이미 승격된 리비전 아래에서 geometry를 교체하고, release와 manifest는 그대로 |
| `publish-administrative-boundary-postgis` | `ON CONFLICT DO NOTHING` + 완전성 검사가 방금 건너뛴 그 표를 읽음 | 소스 geometry가 바뀐 재발행이 아무것도 쓰지 않고 `-ok`를 출력 |

**두 번의 적재는 두 개의 사실이다.** 값이 리비전인 컬럼은 둘을 구분할 수 없으므로 어느 쪽도
증언하지 못한다. 업계 근거는 실제로 갈린다 — PostgreSQL은 `REFRESH` 너머로 matview의 신원을
의도적으로 보존하고, Martin의 `agg_tiles_hash`는 바이트 동일 재빌드에서 같은 값을 내는 콘텐츠
해시다. 그러므로 이 결정의 근거는 다른 곳의 선례가 아니라 **이 저장소**다. 그리고 이 저장소는
이미 한 번 답했다: v1 미러에는 `serving_postgis.parcel_boundary_mirror_rebuild_run`이 있고,
재빌드마다 상태와 카운트를 남긴다. **v2 표들이 그것을 빠뜨린 쪽이다.**

> **정정.** 이 문서의 초안과 마이그레이션 헤더는 세 번째 근거로 "`tiles-slice-proof.sh`가 로더를
> 일부러 두 번 돌린다"를 들었다. 사실이지만 근거가 되지 않는다 — 두 번 도는 루프는
> `scripts/tiles/fixture.sql`이고 그것이 적재하는 것은 **v1** `parcel_boundary_mirror`이며, v2 시드는
> 정확히 한 번 돈다. 그것은 기록되지 않은 재materialise가 아니라 **정반대**이고, 그래서 오히려
> 논지다: 두 번 적재되는 그 표가 바로 `parcel_boundary_mirror_rebuild_run`을 가진 표다.

## Decision

### 1. `serving_postgis.spatial_projection_load` 하나를 둔다

컬럼 어휘는 `parcel_boundary_mirror_rebuild_run`을 그대로 베낀다 — 같은 이름, 같은 CHECK 형태
(`status <> 'succeeded' OR (finished_at IS NOT NULL AND loaded_row_count > 0)`). 두 번째 방언을
만들지 않는다. `publication_unit_key`의 정규식은 `vector_tile_publication_unit_key_check`와 같다.

### 2. publication 두 표를 적재 단위로 재키잉한다

`(data_revision, ...)` → `(projection_load_id, ...)`. 이것이 한 리비전의 두 번째 적재를 제자리
덮어쓰기(parcels)나 조용한 no-op(admin)에서 **따로 서빙·비교·폐기할 수 있는 두 번째 행 집합**으로
바꾼다.

`administrative_boundary_publication_code_key UNIQUE (data_revision, scope_kind, canonical_code)`도
**함께 옮긴다.** 그대로 두면 새 기본키가 두 번째 행 집합을 허용한 한 문장 뒤에서 이 제약이 그것을
거부한다 — parcels에는 능력이 생기고 admin에는 조용히 생기지 않는 상태가 된다. 좁혀진 의미는
정직하다: **한 materialise 안에서** 한 canonical code는 한 단위를 가리킨다. 한 리비전의 두 적재가
서로 일치해야 한다는 것은 서빙 projection의 불변식이 아니며, code 신원은
`catalog.administrative_unit_identifier`가 소유한다.

`parcel_boundary_publication_data_revision_idx`는 **다시 만들지 않고 삭제한다.** 그것은 기본키가
이미 만드는 유니크 인덱스와 컬럼도 순서도 동일한 중복이었다. 이 마이그레이션이 그 인덱스를
삭제하고 다시 만들 자리이므로, 선택의 소유자도 이 마이그레이션이다. 잃는 조회는 없다.

### 3. 두 `*_current` 뷰를 release의 적재 경유로 재배선한다

`release.postgis_projection_revision = publication.projection_load_id`. 타일이 잘려 나오는 행이
선택된 release가 **지명한 바로 그 행**이 된다 — "그 리비전 아래 지금 저장된 무엇이든"이 아니라.

### 4. 게이트가 이 원장의 리더다

`promote_vector_tile_runtime_manifest`가 dynamic 단위에 대해 네 가지를 거부한다: 적재 행 없음,
`succeeded` 아님, 다른 unit key, 다른 data revision, 그리고 **다른 canonical snapshot**.

다섯 번째(snapshot)는 나중에 추가됐고 이유가 둘이다. 하나는 실질 — release가 스냅샷 X를 말하면서
스냅샷 Y로 만든 적재를 가리킬 수 있었다. 다른 하나는 규율 — 그것이 원장의
`canonical_iceberg_snapshot_id`가 갖는 **유일한 리더**다. 세 writer가 채우고 아무도 읽지 않는
컬럼은 `20260730004`가 "`vector_tile_build_job`이 아무도 안 쓰는 표가 된 방식"이라고 부른 그것이고,
이 마이그레이션은 바로 그 교훈을 자기 정당화로 인용한다.

**원장은 자기 리더와 함께 출하한다.** 그것이 이 증분의 형태이며, 신원 부여는 메커니즘이고
뒷받침 없는 projection의 승격을 거부하는 것이 요점이다.

### 5. 적재 id는 승격 명령이 **지명**한다, 해석하지 않는다

`FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID`. 한 리비전이 여러
succeeded 적재를 가질 수 있고 — 그것이 이 원장이 표현하려는 상태다 — 게이트는 그 중 **아무거나**
받아들인다. 그러므로 명령이 해석하면 그것은 사실들 사이의 조용한 선택이고, 하류의 어떤 것도
잘못된 선택을 잡아낼 수 없다. 명령의 기존 형태(모든 사실을 이름 짓고 검증한다)와도 같다.

### 6. 릴리스 행은 삽입 후 통째로 다시 읽는다

`insert_release`는 `ON CONFLICT (id) DO NOTHING`이다. 이미 존재하는 release id로 다시 승격하면 그
행이 말하던 내용이 그대로 남는데, 검증된 것은 **환경변수**였지 그 행이 아니었다. 게이트도 잡지
못한다 — 같은 단위·같은 리비전의 더 오래된 succeeded 적재는 네 조건을 모두 통과하므로, 포인터는
움직이고 뷰는 옛 적재의 행을 계속 서빙한다. 삽입 뒤 저장된 튜플 전체를 비교하고 다르면 거부한다.

### 7. `postgis_projection_revision`을 읽기 계약에서 제거한다 (파괴적)

브라우저는 `serving_generation`만 보고 이 값을 무시한다. 사용자가 전환 비용을 무시하라고 승인했다.
제거 표면은 **다섯 곳이 한 커밋**이다. 셋만 건드리면 라이브 경로가 깨진다.

| 표면 | 조치 |
| --- | --- |
| `foundation-contracts` `VectorTileDynamicPostgisResponse` | 제거 |
| `catalog-domain` `DynamicPostgisSource` | **제거** — `validate()`가 DTO를 이 타입으로 왕복시키고 `deny_unknown_fields`에 기본값도 없다 |
| `sqlx_repository` 매니페스트 리더 | SELECT와 생성자에서 제거 |
| `services/foundation-api` routes 매핑 | 제거 |
| OpenAPI 2부 + SHA 핀 + gongzzang zod | 재생성·갱신 |

유지: `MarkTileLayerDynamicCommand`와 그 요청 지문, `unit_of_work`의 release INSERT. **쓰기 쪽은
값의 의미가 바뀌었을 뿐 사라지지 않았다** — 이제 그것은 적재의 id다.

## 기각한 대안

### 컬럼을 그대로 두고 `data_revision`을 계속 바인딩한다

비용이 0이다. 그러나 ADR-0015의 지문이 이미 그 값을 요청 신원에 넣었으므로, 두는 순간 날조가
불변식이 된다. 그리고 게이트 구멍이 남는다 — 적재된 적 없는 projection의 승격은 이 결정과
무관하게 막아야 하고, 막으려면 참조할 행이 있어야 한다.

### 게이트만 고치고 원장은 만들지 않는다

"publication 표에 이 리비전의 행이 있는가"를 게이트가 직접 세게 한다. 표 하나를 아낀다. 그러나
행이 **몇 개**여야 하는지, **누가 언제** 넣었는지, 그것이 **성공한 적재**였는지를 말할 수 없다.
행의 존재는 완료의 증거가 아니다 — 중단된 적재도 행을 남긴다. `parcel_boundary_mirror_rebuild_run`이
존재하는 이유가 정확히 그것이다.

### 적재 id를 콘텐츠 해시로 한다 (Martin `agg_tiles_hash` 방식)

바이트 동일 재빌드가 같은 신원을 얻는다. 그러나 우리가 구분하려는 것이 바로 **두 번의 적재**이고,
콘텐츠 해시는 정의상 그 둘을 합친다. 재빌드가 같은 결과를 냈다는 사실은 유용하지만 그것은
`geometry_checksum_sha256`이 이미 행마다 말하고 있다.

### `postgis_projection_revision`을 도메인 매니페스트에 남기고 DTO에서만 뺀다

가장 작은 변경으로 보인다. 실제로는 **가장 깨지는** 변경이다. `VectorTileRuntimeManifestResponse::validate`가
DTO를 직렬화해 `catalog_domain::VectorTileRuntimeManifest`로 역직렬화하고, 그 타입은
`deny_unknown_fields`에 기본값 없는 필수 필드를 갖는다. `VectorTileManifestDocument`의 `Deserialize`가
`validate()`를 호출하므로 실패는 픽스처가 아니라 **요청 경로**에서 난다.

### 실패한 적재도 기록한다 (두 번째 커넥션)

`publish-administrative-boundary-postgis`는 트랜잭션 하나다. 실패를 남기려면 두 번째 커넥션과 두
번째 커밋이 필요하고, 그것은 "적재 행과 publication 행이 한 트랜잭션"의 포기다 — ADR-0015 §6이 같은
이유로 같은 결론을 냈다. 실패한 적재는 자기 행과 함께 롤백되고 재시도가 다시 실행한다.

## Consequences

- **뒷받침 없는 승격이 불가능해졌다.** 네 조건 각각을 단일 컬럼 편집으로 만들어
  `a_dynamic_release_without_a_matching_succeeded_projection_load_cannot_be_promoted`가 거부를
  단정하고, 거부가 **포인터가 움직이기 전에** 일어났음을 함께 단정한다.
- **재발행이 조용한 no-op이 아니게 됐다.** 완전성 검사와 행 단위 유효성 검사가 모두 이번 적재를
  키로 읽으므로, `st_isvalid` 필터가 떨어뜨린 삽입이 이전 실행의 행으로 통과할 수 없다.
- **로컬 v2 시드가 실물 적재 행을 만든다.** 지어낸 UUID였던 `019d2b87-…-3604`가 이제 그 값을 id로
  갖는 실제 원장 행이다. `local_vector_tile_seed_contract`가 시드가 계속 그것을 만들고 닫는지 검사한다.
- **`serde` 계약이 5개 표면에서 한 번에 좁아졌다.** OpenAPI 2부와 gongzzang 핀 SHA가 함께 움직인다.
- `CREATE OR REPLACE FUNCTION`이 **네 번째로** 게이트 본문을 교체했다
  (`20260724000001` → `20260730000001` → `20260730000003` → `20260731000001`). 유효 집합 전체를 읽어야
  하는 구조이며, `the_promotion_gate_and_the_domain_agree_on_the_release_object_root`가 파일이 아니라
  `pg_proc.prosrc`를 읽으므로 설치된 본문이 대조 대상이다.

## 남은 부채

1. **publication 두 표가 적재마다 한 벌씩 커지고, 아무것도 지우지 않는다.** 이전에는 리비전당 한
   벌이었다. `administrative_unit_boundary_publication`은 `administrative_boundary_publication_append_only`가
   DELETE까지 덮으므로 publisher capability 없이는 지울 수조차 없다. `20260730000004`가 보존 기간을
   미룬 근거의 절반("행이 작다")은 여기에 넘어오지 않는다 — 이것은 geometry다. 이번에 스위퍼는
   넣지 않는다: 증가는 요청당이 아니라 **운영자가 발행할 때마다**이므로 발행 빈도에 묶인다.
   재검토 방아쇠는 **운영자 명령이 아니라 스케줄로 재적재하는 첫 단위**다.
2. **`status`의 세 값 중 `failed`를 쓰는 운영 writer가 없다.** 이 마이그레이션 자신은 쓴다 —
   publication 행이 없는 dynamic release의 날조된 UUID에 `failed` 행을 만들어, FK가 성립하면서도
   게이트가 그것을 거부하게 한다. 그러나 `publish-administrative-boundary-postgis`는 트랜잭션
   하나이므로 실패를 커밋할 수 없다(§기각한 대안).

   > **정정 (2026-07-31):** 이 항목은 처음에 "계획된 parcels 적재기는
   > `postgis_parcel_boundary_mirror_national_rebuild`를 따를 것"이라고 썼다. **그 명령을 템플릿으로
   > 삼으면 안 된다.** 그것은 자기 활성 대상을 `TRUNCATE TABLE serving_postgis.parcel_boundary_mirror`
   > 한다. 구현 안내서가 명시적으로 금지하는 형태이고("전국 rebuild는 staging과 atomic replacement를
   > 사용해야 하며 active table을 먼저 `TRUNCATE`하지 않는다"), **이 마이그레이션이 그 금지를 더
   > 강하게 만들었다** — 이제 모든 적재가 한 표를 공유하므로 truncate는 서빙 중인 적재와 보관된 적재를
   > 함께 지운다. 재키잉 자체가 staging·swap이 노리던 것(적재마다 서로소인 행 집합, release가
   > 지명하기 전까지 비가시)을 이미 제공하므로, 새 적재기에 staging 표는 필요 없다.
   >
   > 트랜잭션을 열지 않는 형태가 `failed`를 쓸 수 있다는 것만은 맞다. 다만 그 형태의 선례로 쓸 것은
   > `parcel_marker_anchor_rebuild`의 **사전검증 후 커밋** 경로다 — geometry를 한 행도 쓰기 전에
   > 실패를 확정할 수 있는 검증은 한 트랜잭션 안에서 `failed`를 커밋하고 끝내므로, 고아 `running`
   > 행을 남기지 않는다. 두 커넥션은 geometry 적재 중 실패에만 필요하다.
3. **parcels 적재 경로가 아직 없고, 그 앞에 모델 결함이 하나 있다.**
   `serving_postgis.parcel_boundary_publication`에 쓰는 것은 시드와 마이그레이션 백필뿐이다.

   막고 있는 것은 적재기 자체가 아니라 리비전의 신원이다.
   `vector_tile_release.data_revision`는 `catalog.administrative_boundary_revision`에 복합 FK를 건다
   (`20260727000001` `vector_tile_release_data_revision_fkey`). 그래서 **모든** 발행 단위의 리비전이
   행정경계 리비전 원장에 등록돼야 하고, 한 도메인의 원장이 전역 원장 노릇을 한다. 이 거짓말은 이미
   커밋돼 있다 — v2 시드는 **parcels** 리비전을 그 원장에 넣고, `spatial_tile_publication.rs`의
   `seed_data_revision`도 `parcels`·`complex`에 대해 같은 일을 한다. parcels 적재기는 자기 리비전을
   행정경계 리비전이라고 기록하지 않고는 발급할 수 없으므로, 이 위에 세운 적재기는 결함을 물려받는다.
   **적재기보다 이 원장이 먼저다.**
4. **`publish-administrative-boundary-postgis`에는 단위 테스트가 없다.** 원장 삽입·`projection_load_id`
   바인딩·재키잉된 `ON CONFLICT`을 실제로 통과시키는 것은
   `scripts/tiles/administrative-boundary-slice-proof.sh` 하나뿐이고, 그것은 Docker가 필요해서 **CI에
   없다.** 앞선 회귀를 아무 검사도 잡지 못한 것이 이 때문이다.
5. **지문 v1이 그대로인데 값의 의미가 바뀌었다.** `postgis_projection_revision`은 `data_revision`의
   별칭에서 `spatial_projection_load.id`가 됐고 `CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION`은
   상수다. 전환 전에 발급된 키를 전환 후에 재생하면 `MutationFingerprintVersionChanged`가 아니라
   `MutationIdempotencyKeyReused`가 난다. ADR-0015 남은 부채 2("키를 발급하는 클라이언트가 아직
   없다")가 이 범위를 묶어 주지만 없애지는 않는다.
6. **시드는 고정 리터럴 적재 id를 쓴다.** publisher처럼 새 적재를 만들 수 없으므로, 미러 geometry가
   바뀌면 `DO NOTHING`이 조용히 넘어간다. 완화로 시드 끝에서 기록된 카운트와 미러 카운트가
   어긋나면 실패시킨다 — 완전한 답은 아니고, 고정 픽스처와 운영 명령의 차이는 **편집이 눈에
   보인다**는 것뿐이다.
