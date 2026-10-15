# ADR 0250: 산업단지의 id 는 하나다 — 공식 단지코드에서 유도한 UUIDv5

- Status: Proposed
- Date: 2026-10-15
- Area: `platforms/foundation-platform`, `products/gongzzang`, `consoles/dawneer`
- Supersedes: [ADR-0048](./0048-a-published-feature-id-needs-a-read-keyed-on-it.md) §2 (`by-lakehouse-id` 조회구) — 3단계에서
- Related: [ADR-0043](./0043-a-canonical-id-is-read-not-recomputed.md), [ADR-0060](./0060-gold-artifact-identity-is-resolved-at-the-catalog-write-boundary.md), [ADR-0226](./0226-gongzzang-complex-target-id-is-the-lakehouse-complex-id.md)

## Context

산업단지 하나가 서로 계산되지 않는 신원을 둘 가졌다.

| 이름 | 만드는 곳 | 형식 | 쓰는 곳 |
|---|---|---|---|
| `catalog.industrial_complex.id` | `RegisterIndustrialComplex`, 업서트 INSERT 의 `Uuid::now_v7()` | UUIDv7 | Postgres 주 키·FK 10개, `GET /catalog/v1/complexes/{id}` 와 하위 경로 |
| `complex_id` (`lakehouse_complex_id` 열) | `industrial_complex_bronze_to_silver.py` `stable_uuid_v5` | UUIDv5 | 타일 feature id, Gold·R2 프로필 키, `GET .../by-lakehouse-id/{id}` |

둘을 섞어 부르면 404 가 났고, 404 는 "없는 단지"로 읽혔다(ADR-0048, Gold 포인터 발행 0행 사고).
ADR-0048 은 두 번째 조회구를 붙였고 ADR-0226 은 공짱이 v5 만 저장하게 막았다. 둘 다 증상을 문마다 막았을 뿐 신원이 둘인 상태는 그대로였다.

그런데 v5 는 무작위가 아니다: `uuid5(NAMESPACE_URL, "foundation-platform:catalog:industrial_complex:" + official_complex_code)`.
**공식 단지코드만 있으면 누구나 계산할 수 있다.** 2026-10-15 운영 실측: `catalog.industrial_complex` 1,442행 전부가
`lakehouse_complex_id` 를 가지며 1,442행 전부 위 유도식과 같다. 이 표를 참조하는 FK 10개 표는 모두 0행이다
(`allowed_industry`, `blueprint`, `complex_attachment`, `complex_notice`, `digital_twin_asset`,
`industrial_complex_gold_pointer`, `industry_group`, `spatial_layer`, `parcel_complex_membership`,
`serving_postgis.parcel_boundary_mirror`). 과거 기록으로 `catalog.outbox_event` 에 산업단지 이벤트 4,326건(전부 발행 완료)이 v7 을 담고 있다.

## Decision

1. **산업단지의 id 는 공식 단지코드에서 유도한 UUIDv5 하나다.** 발급하지 않고 계산한다.
   - Rust: `ComplexId::for_official_complex_code` (`foundation-shared-kernel`). 등록·업서트 두 발급 자리가 모두 이것을 쓴다.
   - SQL: `catalog.industrial_complex_id_for(text)` (pgcrypto `digest`).
   - Spark: 기존 `stable_uuid_v5(complex_id_seed(code))`.
   - 세 언어가 같은 벡터(`000000 → b796bcb0-…`, `111010 → 7df3859c-…`)를 시험에서 고정한다.
2. **DB 가 불변식을 강제한다** (`20261015090000_an_industrial_complex_has_one_id.sql`):
   - `industrial_complex_id_is_derived_from_official_code`: `id = catalog.industrial_complex_id_for(official_complex_code)`
   - `industrial_complex_lakehouse_complex_id_is_the_id`: `lakehouse_complex_id = id`, `NOT NULL`
   - 업서트 유스케이스는 생산자가 준 `lakehouse_complex_id` 가 유도값과 다르면 거부한다. 파이썬과 Rust 유도가 갈라지면 적재 시점에 멈춘다.
3. **이주는 기존 열을 통해 다시 키를 건다. 잃는 행이 없다.**
   - 유도값과 저장된 lakehouse id 가 다른 행이 하나라도 있으면 거부한다(추측 금지).
   - 편집 데이터 FK 8개를 잠시 `DEFERRABLE` 로 두고 부모와 자식을 한 트랜잭션에서 옮긴 뒤 되돌린다.
   - 추가 전용·봉인 표(`parcel_complex_membership`, `parcel_boundary_mirror`)는 다시 쓰지 않는다. 옮길 키를 참조하면 거부한다(운영 0행).
   - 은퇴한 v7 은 추가 전용 장부 `catalog.industrial_complex_retired_id(retired_id → complex_id)` 에 남는다.
     v7 을 담은 과거 기록(`outbox_event` 페이로드, `normalization_application.target_id`)은 다시 쓰지 않고 이 장부로 풀린다.
   - 마이그레이션 안에서 이주 전후 행 수를 비교해 다르면 거부한다.
4. **경로는 하나다**: `/catalog/v1/complexes/{id}` 와 그 하위 경로가 이 id 를 받는다. `by-lakehouse-id` 는 3단계에서 제거한다.
5. 승인 플래그는 두지 않는다. 행을 지우지 않고, 모든 v7 이 장부에 남으며, 기존 호출자가 깨지지 않는다(목록이 주는 `id` 가 곧 새 id 이고 `by-lakehouse-id` 는 같은 행을 찾는다).

### 단계 (각각 독립 PR)

| 단계 | 내용 | 깨지는 호출자 |
|---|---|---|
| 1 | 유도 함수·발급 자리 교체·마이그레이션·장부·CHECK (이 ADR) | 없음 |
| 2 | 공짱 `complex_reader`·웹, 더니어가 `/complexes/{id}` 만 사용 | 없음 |
| 3 | `by-lakehouse-id` 경로·`find_complex_by_lakehouse_id`·`lakehouse_complex_id` 열·DTO 필드·`LakehouseComplexId` 타입 제거, OpenAPI·공짱 pin 재생성 | 1·2 배포 후에만 |

## Rejected

- **v7 을 정본으로 두고 타일·Gold 를 v7 로 다시 굽기**: v7 은 Postgres 밖에서 계산할 수 없다. 레이크하우스가 Postgres 에 신원을 물어야 하는데, 이는 "레이크하우스가 정본, Postgres 는 재구성 가능한 사본"에 반한다.
- **`lakehouse_complex_id` 를 주 키로 승격하고 `id` 를 버리기**: 결과는 같지만 FK 10개와 모든 리더의 열 이름을 한 번에 바꿔야 한다. 같은 열(`id`)의 값만 옮기면 FK·리더가 그대로다.
- **FK 를 끊고 다시 걸기**: 62GB 인 `parcel_boundary_mirror` 를 전부 다시 검증한다. `DEFERRABLE` 전환은 카탈로그 메타데이터만 바꾼다.
- **v7 을 별칭으로 계속 받기**(옛 URL 호환): 신원 공간이 다시 둘이 된다. 장부는 감사용이지 조회구가 아니다.
- **SQL 유도 함수 없이 `lakehouse_complex_id = id` 만 강제**: 쓰기 API 로 등록된 행(레이크하우스 값이 없음)이 다시 아무 id 나 받을 수 있다.

## Consequences

- 쓰기 API 로 등록한 단지도 첫 쓰기부터 레이크하우스가 나중에 붙일 id 를 가진다. ADR-0048 이 남긴 "그런 단지는 패널이 없다"가 사라진다.
- 코드 하나는 영원히 단지 하나다. 적재(업서트)는 활성 코드가 아니라 유도 id 로 기존 행을 찾는다. 원천이 보관된 단지를 계속 싣고 있으면
  그 행의 원천 칸을 갱신하고 보관 상태는 그대로 둔다. 예전에는 옆에 새 행을 만들었다. 한 행 충돌로 1,442행 묶음 전체가 실패하는 일도 없다.
  쓰기 API 로 보관된 코드를 다시 등록하면 `industrial_complex_pkey` 충돌 → `ComplexOfficialCodeConflict` 다(같은 단지이므로 보관 해제가 맞다).
- 공식 단지코드는 사실상 신원이다. 코드를 바꾸는 쓰기는 CHECK 가 거부한다(현재 그런 경로 없음).
- 운영 리허설(2026-10-15, `BEGIN … ROLLBACK`): 0.57초, 1,442 → 1,442행, 장부 1,442행.
