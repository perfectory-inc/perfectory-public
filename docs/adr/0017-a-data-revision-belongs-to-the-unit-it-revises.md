---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-31
---

# ADR 0017: 데이터 리비전은 그것이 개정하는 단위에 속한다

- Status: Accepted
- Date: 2026-07-31
- 관련: [ADR-0016 PostGIS 적재는 신원을 가진 하나의 사실이다](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md), [ADR-0015 키를 가진 Catalog mutation은 원장 하나를 쓴다](./0015-one-idempotency-ledger-for-keyed-catalog-mutations.md), [ADR-0014 serving generation은 한 단위의 소스 선택만 추적한다](./0014-serving-generation-tracks-one-unit-source-selection.md)
- 마이그레이션: `20260731000002_publication_revision_ledger.sql`

## Context

`catalog.vector_tile_release.data_revision`는 `20260727000001` 이래
`catalog.administrative_boundary_revision`에 복합 FK를 걸었다.

```sql
ALTER TABLE catalog.vector_tile_release
    ADD CONSTRAINT vector_tile_release_data_revision_fkey
    FOREIGN KEY (data_revision, canonical_iceberg_snapshot_id)
    REFERENCES catalog.administrative_boundary_revision(id, canonical_iceberg_snapshot_id)
```

그래서 **모든** 발행 단위의 리비전이 — parcels든 complex든 앞으로 무엇이든 — **행정경계** 리비전
원장에 등록돼야 했다. 한 도메인의 원장이 전역 원장 노릇을 했고, 그 거짓말은 이미 커밋돼 있었다.
v2 시드는 parcels 리비전을 그 원장에 넣었고(`iceberg:tile-runtime-v2`라는 지어낸 source snapshot과
함께), `spatial_tile_publication.rs`의 `seed_data_revision`은 `parcels`·`complex`에 같은 일을 했다.

이것이 parcels 적재기를 막고 있었다. parcels 리비전을 발급하려면 그것이 행정경계 리비전이라고
기록해야 한다 — 소스도, 상태 기계도, 검증도 다른데. 그 위에 세운 적재기는 결함을 물려받는다.

### 그 표는 애초에 행정경계가 아니었다

컬럼 일곱 개에 행정 개념이 하나도 없다: `id`, `canonical_iceberg_snapshot_id`,
`source_snapshot_id`, `source_record_id`, `status`, `created_at`, `validated_at`.

그리고 release FK가 필요로 하는 복합 UNIQUE 키는 **표와 함께 선언되지 않았다.** 226줄 뒤, 그 FK
바로 앞에서 `ALTER TABLE`로 붙었다. 행정 사실 테이블 다섯 개는 전부 `(id)` 하나만 참조한다. 즉 그
키는 **제네릭 소비자를 위해** 존재한다. 이 마이그레이션은 `20260727000001`이 우연히 시작해 둔
분리를 끝내는 것이다.

### 그 원장에는 프로덕션 writer가 없다

저장소 전체에서 `catalog.administrative_boundary_revision`에 INSERT하는 곳은 마이그레이션 백필,
시드, 픽스처, 테스트뿐이다. `status`의 네 값 중 실제 전이는
`candidate` → `validated` 하나뿐이고, `published`와 `superseded`는 **어디에도 writer가 없다.**

## Decision

### 1. `catalog.publication_revision`을 둔다

단위에 스코프된 발행 리비전 원장. `catalog.administrative_boundary_revision`은 행정경계 **사실**
원장으로 남고, 발행 리비전은 `derived_from_administrative_revision`으로 그 사실을 가리킨다(있을 때).

핵심은 참조되는 키다.

```sql
CONSTRAINT publication_revision_scoped_key UNIQUE (id, publication_unit_id, canonical_iceberg_snapshot_id)
```

`vector_tile_release`와 `spatial_projection_load`가 `(data_revision, publication_unit_id,
canonical_iceberg_snapshot_id)`로 이것을 참조한다. **다른 단위의 리비전을 지명하는 release는 이제
23503으로 거부된다** — 게이트가 나중에 알아채는 것이 아니라, 쓰이지 않는다.

`UNIQUE (publication_unit_id, canonical_iceberg_snapshot_id)`도 함께 둔다. 스냅샷이 곧 데이터
버전이므로, 한 단위의 한 스냅샷에 두 리비전은 한 사실의 두 이름이다. 같은 리비전을 다시
materialise하는 것은 적재 원장의 일이다([ADR-0016](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)).

### 2. 이 원장에 **없는** 것들

- **`status` 없음.** 행정 원장의 네 값에 실제 writer는 하나뿐이었다. 발행 리비전의 유효성은 이미
  두 번 진술된다 — succeeded 적재가 있는가, 그리고 런타임 포인터가 그것을 지명한 release를
  고르는가. `20260727000002`는 런타임 매니페스트가 **유일한 가시성 스위치**라고 명시한다. 세 번째
  진술은 `20260730000004`가 지목한 `vector_tile_build_job` 실패다.
- **`source_snapshot_id` 없음.** `20260727000001`은 자기 CHECK를 만족시키려 값을 지어내야 했다 —
  `'iceberg:vector-tile-release:' || uuid`, 자기가 붙어 있는 리비전을 이름으로 삼은 문자열.
  출처는 실제 Iceberg 스냅샷 번호와 release가 이미 들고 있던 `source_record_id`다. 날조는 **쓸 자리가
  없어졌다.**
- **ordinal 키 없음.** §기각한 대안 참조.

### 3. INSERT까지 덮는 capability 트리거 — 발급 구멍

`administrative_boundary_revision_append_only`는 UPDATE와 DELETE만 덮고,
`infra/compose/grant-foundation-runtime.sql`은 `catalog` 스키마의 **모든 테이블에 INSERT를 준다.**
REVOKE 목록에 리비전 원장은 없었다. 즉 **API 롤이 아무 스냅샷이나 주장하는 리비전을, 아무 증거
없이 발급할 수 있었다.**

`publication_revision_publisher_only`는 `BEFORE INSERT OR UPDATE OR DELETE`다. 그리고 두 원장 모두
REVOKE 목록에 넣었다. 둘 다 하는 이유: grant는 **롤 경계**라 누군가 롤을 하나 추가하는 순간 보호를
멈추고, 트리거는 **능력 경계**라 그 실수를 넘긴다.

대가는 정직하게 적는다 — 리비전을 쓰는 모든 곳이 이제 명시적으로 publisher capability를 잡아야
한다. 시드도, 픽스처도. 그것이 요점이다: 리비전을 쓰는 것은 발행하는 것이다.

### 4. 적재는 자기 단위를 FK로 지명한다

`spatial_projection_load.publication_unit_key text`를 `publication_unit_id uuid` FK로 바꾼다.
[ADR-0016](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)의 마이그레이션이 그
컬럼에 남긴 주석은 "두 철자가 어긋나면 안 된다"였다 — **외래 키가 할 일을 산문으로 적어 둔 것**이다.

그 결과 게이트의 다섯 조건 중 셋(다른 단위·다른 리비전·다른 스냅샷)이 **행이 가질 수 없는 상태**가
됐다. 승격 시점의 검사에서 쓰기 시점의 제약으로 옮겨 갔고,
`a_load_cannot_be_written_for_another_units_revision`이 그것을 23503으로 단정한다.

### 5. 도메인이 문서대로 하게 만든다

`validate_serving_transition`의 문서는 처음부터 이렇게 말했다 — "dynamic으로 돌아오는 것은 같은
데이터 리비전(안전한 fallback)이거나 **더 새로운** 리비전일 때 허용된다." 함수에는 dynamic 갈래가
**아예 없었다.** 있을 수도 없었다: 비교할 수 있는 값이 `data_revision`뿐인데 uuid에는 순서가 없다.

`ServingSelection`이 `canonical_iceberg_snapshot_id`를 함께 들고, 그 값으로 비교한다. 게이트도 같은
비교를 하므로 SQL로 직접 부르는 호출자가 우회할 수 없다. 비교는 숫자순이다 — 두 값 모두
`^[1-9][0-9]*$`라서 선행 0이 없고, 따라서 **긴 문자열이 큰 수**이고 길이가 같으면 사전순이다.
정수로 파싱하지 않는 이유는 실제 Iceberg 스냅샷 id가 이미 18자리이기 때문이다.

## 기각한 대안

### `(publication_unit_id, revision_ordinal)` 복합 기본키 (surrogate id 제거)

가장 우아하다. 교차단위 리비전이 **거부되는** 것이 아니라 **표현 불가**가 된다 — ordinal은 자기가
속한 단위 아래에서만 의미를 갖는다. 그리고 순서를 DB가 강제한다.

기각한 이유는 그 순서 보장이 **다른 순서**이기 때문이다. 할당자는 `max(ordinal) + 1`이고 백필은
release의 `created_at` 순이다. 둘 다 **발급 순서**이지 **내용 순서**가 아니다. 오래된 Iceberg
스냅샷으로 나중에 연 리비전이 더 큰 ordinal을 받고 게이트를 통과한다. 그러면 강제되는 명제는 "우리가
이것을 더 최근에 열었다"이고, 그것은 §5가 말하는 "더 새로운"이 아니다. **내용 순서를 담은 값은 이미
있고 숫자다** — `canonical_iceberg_snapshot_id`. 그것으로 비교하면 되고, 그러면 ordinal이 사는 이유가
사라진다.

값도 잃는다. `data_revision = 7`은 형제 컬럼 없이는 영원히 무의미하고, 그 비용은 전환이 아니라
**정상 상태**로 남는다 — 운영자 env var, 공개 매니페스트 필드, 멱등성 지문, 이벤트 DTO, 그리고
앞으로 누가 쓸 모든 로그 줄과 메트릭 라벨. uuid는 그 자리에서 혼자 완결된다.

부수적으로, ordinal은 새 SSOT 위반을 만들었다. 행정 publisher는 `DATA_REVISION` 하나를 받아 두
원장에 같은 값을 넣는데, ordinal은 그것을 **서로 무관한 두 식별자**로 쪼개면서 둘을 묶는 제약을
남기지 않는다. 채택안은 발행 리비전 id를 행정 리비전 id와 **같게** 두고
`derived_from_administrative_revision`으로 묶는다.

### 표 이름만 바꿔 제자리에서 일반화한다

컬럼이 이미 제네릭이니 이름만 바꾸고 단위 스코프 컬럼을 더하자는 안. 표 하나를 아끼지만 두 가지가
걸린다. 첫째, 지시 대상이 다르다 — `administrative_unit_identifier`의 리비전(append-only 사실 행,
`source_snapshot_id` 동등성 트리거로 보호됨)과 parcels 타일 데이터셋의 리비전은 라벨 두 개짜리 한
개념이 아니다. 하나의 id 공간, 하나의 append-only 트리거, 하나의 상태 기계에 넣어 두고 사실
테이블에는 `TG_ARGV` 스코프 문자열을 넘겨 구분하게 하는 것은, 애초에 일어날 이유가 없던 병합을
단속하려고 런타임 검사를 다시 들여오는 것이다. 둘째, 그 안은 스코프 컬럼을 어떤 레지스트리에도
FK로 걸지 않으므로 `'parcelz'`로 발급된 리비전을 아무것도 거부하지 못한다.

### 리비전 유일성 검사만 Rust에 둔다

가장 작다. 그리고 이 저장소가 이미 지불한 비용을 다시 지불한다 — 게이트가 도메인 상태 기계를
반복하는 이유는 SQL로 직접 쓰는 호출자가 있기 때문이고, 마이그레이션 백필과 시드가 바로 그 호출자다.

## Consequences

- **교차단위 리비전이 표현 불가가 됐다.** release도 적재도 `(revision, unit, snapshot)`으로 참조하므로
  다른 단위의 리비전을 지명하면 23503이다. 채워진 데이터베이스 위에서 실증했다.
- **parcels 적재기가 막히지 않는다.** parcels 리비전은 이제 parcels 단위의 원장에서 발급된다.
  다만 적재기 자체는 여전히 없다 — [ADR-0016](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)
  부채 3 참조.
- **발급 구멍이 닫혔다.** 능력 없이 리비전을 만들면 42501이다.
- **행정 원장이 행정 사실만 담는다.** 발행 리비전으로 이관된 행 중 어떤 행정 사실도 참조하지 않는
  것은 삭제된다. 삭제는 문자열이 아니라 **지시 대상**으로 판단한다 — 지어낸 문자열이 두 종류
  (`iceberg:vector-tile-release:`와 시드의 `iceberg:tile-runtime-v2`)라서 문자열 매칭은 하나를 놓친다.
- **`published` 상태가 사라졌다.** writer가 없었고, 런타임 포인터가 소유한 사실을 두 번째로
  진술하고 있었다. `validated`로 접었다.
- `CREATE OR REPLACE FUNCTION`이 **네 번째로** 게이트 본문을 교체했다.

## 남은 부채

1. **`superseded`는 아직 writer가 없다.** `published`는 지웠지만 이것은 남겼다 — 행정 리비전이
   실제로 대체되는 경로(다음 스냅샷 수집)가 생길 때 쓰일 값이고, `published`와 달리 다른 곳이 소유한
   사실의 재진술이 아니다. 그 경로가 오지 않으면 지워야 한다.
2. **`derived_from_administrative_revision`은 단위별로 강제되지 않는다.** admin 단위의 발행
   리비전이 이 값을 반드시 가져야 한다는 것을 CHECK로 표현할 수 없다 — 단위 종류를 스키마가 모른다.
   admin publisher가 항상 채우지만, 그것은 코드의 규율이지 제약이 아니다.
3. **`vector_tile_build_job.input_data_revision`에는 여전히 FK가 없다.** 아무도 쓰지 않는
   테이블이라 지금은 비용이 없고, 쓰기 시작하는 변경이 이 스코프 키를 함께 채택해야 한다.
4. **`catalog.parcel_identifier`는 여전히 행정 리비전 원장에 묶여 있다.** 지적 PNU 사실이지
   행정경계 사실이 아니다. 이 증분은 **발행** 리비전만 분리했다. 사실 원장 자체의 분리는 별개이며,
   `20260727000001`이 지적 PNU 행을 합법화하려고 만든 `legacy:administrative-boundary-revision`
   단일 행이 그 작업의 시작점이다.
