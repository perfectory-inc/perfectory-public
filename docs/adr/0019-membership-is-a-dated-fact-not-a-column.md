---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-07
---

# ADR 0019: 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다

- Status: Accepted
- Date: 2026-08-07
- 관련: [ADR-0017 데이터 리비전은 그것이 개정하는 단위에 속한다](./0017-a-data-revision-belongs-to-the-unit-it-revises.md), [ADR-0018 두 언어가 같은 어휘를 적으면 대조한다](./0018-a-vocabulary-written-in-two-languages-is-compared.md), [기반 목표 G1](../roadmap/foundation-goals.md)
- 마이그레이션: `20260807000001_parcel_complex_membership.sql` (1단계)

## Context

### 저장소가 같은 질문에 두 답을 갖고 있다

"필지는 반드시 산업단지에 속하는가"에 대해 이 저장소는 서로 다른 두 곳에서 반대로 답한다.

```sql
-- 20260719000001_foundation_platform_schema.sql:792
CREATE TABLE catalog.parcel (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,          -- 속한다
    pnu character(19) NOT NULL,
```

```sql
-- 20260719000001_foundation_platform_schema.sql:1095
CREATE UNLOGGED TABLE serving_postgis.parcel_boundary_mirror (
    pnu character(19) NOT NULL,        -- PRIMARY KEY
    complex_id uuid,                   -- 속하지 않을 수 있다
    parcel_id uuid,
```

두 표의 차이는 취향이 아니다. **실제 전국 필지 경계를 담는 쪽이 nullable이다.** mirror는
`silver.parcel_boundaries`에서 전국을 적재하고 PNU를 기본키로 쓴다. 산업단지 안에 있는 필지는
그중 일부다. 즉 데이터가 실제로 들어오는 경로는 이미 "필지는 산단에 속하지 않을 수 있다"를
전제로 만들어져 있고, `catalog.parcel`만 그 사실을 표현할 수 없다.

레이크하우스는 한 걸음 더 나가 있다. `silver.complex_parcel_memberships`는 소속을 **자기 행을
가진 사실**로 다룬다 — `membership_id`, `membership_kind`, `source_method`, `overlap_ratio`,
`valid_from_utc`, `valid_to_utc`. Postgres만 그 사실을 필지 행의 컬럼 하나로 눌러 놓았다.

### 현실이 그렇지 않다

산업단지는 필지의 상위 개념이 아니다. 별도의 실체이며, 여러 필지에 **걸친다**. 그래서
현실에는 이 모델로 표현할 수 없는 상태가 넷 있다.

| 현실 | `complex_id NOT NULL`이 강요하는 것 |
|---|---|
| 산단 밖 필지 (전국 대부분) | 존재할 수 없다 |
| 산단 경계에 걸친 필지 | 한쪽만 골라야 한다 |
| 산단 지정·해제 | 이력 없이 값이 덮인다 |
| 산단이 확장·축소된 시점 | 어디에도 없다 |

OpenAPI는 이 오류를 문장으로 적어 두었다 — `ParcelResponse.complex_id`의 설명은
`"Industrial complex that owns this parcel."`다. 소유는 잘못된 관계다.

### 같은 형태의 결함이 네 곳이다

**신원과 개수를 가진 사실을 한쪽 행의 스칼라 컬럼으로 눌렀다.** 같은 형태다.

| # | 자리 | 실제 | 지금 |
|---|---|---|---|
| 1 | 필지 ↔ 산업단지 | 기간을 가진 N:M | `parcel.complex_id NOT NULL` |
| 2 | 건물 ↔ 필지 | N:M (부속지번) | `building.parcel_id NOT NULL` |
| 3 | 호실 → 건물 | 호실은 건물에 속한다 | `building_unit.parcel_id`만 있고 `building_id`가 **없다** |
| 4 | 필지의 분할·합병 | 전이 그래프 | 표가 없다 |

2번은 저장소가 스스로 알고 있었다. `building-register-consistency-rules.v1.draft.md:43`에
**"건물↔필지는 N:M (부속지번 경유)"** 라고 적혀 있고, `canonical-property-data-platform-northstar.md:44`는
"여러 필지=한 건물 = 부속지번(정부 제공)"이라고 적는다. 그리고 부속지번은 이미 수집 대상이다 —
`hubgokr__building_register_sub_parcel`, `hubgokr__building_register_closed_sub_parcel`. 산문과
수집 계획은 N:M인데 DDL만 1:N이다.

3번은 정부 원장의 계층을 한 단계 접었다. 표제부(건물)와 전유부(호실)는 
`mgm_bldrgst_pk`로 이어지는데, 스키마에서는 둘 다 필지에만 붙어 있어 **어느 호실이 어느 건물에
있는지 물을 수 없다.**

### 형판은 이미 이 저장소 안에 있다

`20260727000001_administrative_boundary_identity.sql`이 행정 단위에 대해 정확히 이 문제를
풀어 두었다. 소속은 `catalog.parcel_administrative_unit`(118행), 전이는
`catalog.administrative_unit_transition`(61행)이다. 둘 다 `effective_period daterange`,
GiST `EXCLUDE`, append-only 트리거, `data_revision`·`source_snapshot_id`·`source_record_id`
계보를 갖는다. 순환 방지 트리거까지 있다.

새 설계를 발명할 필요가 없다. **같은 저장소의 같은 파일에 있는 형판을 두 번째로 적용하는 것이다.**

## Decision

### 1. 소속은 `catalog.parcel_complex_membership`이 소유한다

`parcel_administrative_unit`을 형판으로 삼는다.

```sql
CREATE TABLE catalog.parcel_complex_membership (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    complex_id uuid NOT NULL,
    membership_kind text NOT NULL,
    source_method text NOT NULL,
    overlap_ratio numeric(9,6),
    effective_period daterange NOT NULL,
    data_revision uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT parcel_complex_membership_pkey PRIMARY KEY (id),
    CONSTRAINT parcel_complex_membership_kind_check
        CHECK (membership_kind IN ('inside', 'intersects', 'candidate', 'excluded')),
    CONSTRAINT parcel_complex_membership_method_check
        CHECK (source_method IN ('official_list', 'geometry_overlay', 'manual_review')),
    CONSTRAINT parcel_complex_membership_overlap_check
        CHECK (overlap_ratio IS NULL OR (overlap_ratio >= 0 AND overlap_ratio <= 1)),
    CONSTRAINT parcel_complex_membership_period_check
        CHECK (NOT isempty(effective_period) AND lower_inc(effective_period)),
    CONSTRAINT parcel_complex_membership_pair_excl EXCLUDE USING gist
        (parcel_id WITH =, complex_id WITH =, membership_kind WITH =, effective_period WITH &&),
    CONSTRAINT parcel_complex_membership_one_container_excl EXCLUDE USING gist
        (parcel_id WITH =, effective_period WITH &&) WHERE (membership_kind = 'inside')
);
```

두 `EXCLUDE`가 서로 다른 명제를 강제한다.

- `_pair_excl` — 한 필지·한 산단·한 종류의 소속은 한 시점에 하나다. 레이크하우스 품질 게이트
  `"one active inside or intersects membership per complex_id and pnu"`와 같은 명제다.
- `_one_container_excl` — **필지를 담는 산단은 한 시점에 최대 하나다.** 걸치는 관계
  (`intersects`)는 여러 개일 수 있지만 담기는 관계는 아니다. 이것이 `complex_id NOT NULL`이
  표현하려던 것 중 유일하게 참인 부분이며, 여기서는 `NOT NULL`이 아니라 `EXCLUDE`로 적힌다 —
  "최대 하나"이지 "정확히 하나"가 아니기 때문이다.

어휘 네 값·세 값은 **발명하지 않았다.** `industrial-complex-lakehouse-poc.md:217-218`이 이미
소유하고 있고, 여기서는 같은 값을 Postgres 쪽에 적는 것뿐이다.

### 2. 새 어휘는 ADR-0018 대조 테스트에 등록한다

`membership_kind`와 `source_method`는 이제 **세 곳**에 적힌다 — 레이크하우스 계약, Postgres
CHECK, Rust 도메인. [ADR-0018](./0018-a-vocabulary-written-in-two-languages-is-compared.md)이
이 상황을 위해 존재한다. 두 열거형은 `ALL` 상수를 갖고,
`a_database_vocabulary_is_spelled_the_same_way_in_both_languages`가 `pg_get_constraintdef`를
읽어 대조한다. 등록하지 않은 채 표만 추가하는 것은 이 ADR이 금지한다.

이 검사가 막는 실제 사고: 어휘가 갈라지면 디코더가 런타임에 알 수 없는 값을 만나 서빙 시점에
실패한다 — ADR-0018이 `ServingSourceKind`에서 실제로 겪은 일이다.

### 3. `catalog.parcel.complex_id`는 제거 대상이다

파생 컬럼으로 남기지 않는다. 남기면 두 곳이 같은 사실을 적고, 그것이 이 ADR이 고치는 결함이다.
제거는 세 단계로 나눈다(§이행 순서). 컬럼이 살아 있는 동안에는
`catalog.protect_parcel_pnu_projection`과 같은 형태의 트리거로 발행 경로 밖의 쓰기를 거부한다.

API 비용은 실측했다. `ParcelResponse.complex_id`는 `required` 목록에 있지만, 저장소 안의 유일한
소비자인 gongzzang의 `CatalogParcelResponse`는 **`pnu`와 `kind`만 역직렬화한다.** 나머지 필드는
읽지 않는다. `products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json`은
플랫폼 사본과 바이트 단위로 동일하므로 두 사본을 함께 고친다.

### 4. 건물↔필지와 호실→건물의 모양을 확정한다

지금 만들지 않는다(§이행 순서 3단계). 모양만 확정한다.

- `catalog.building_parcel` — `(building_id, parcel_id, relation_kind)`. `relation_kind`는
  `main`(대표지번)과 `attached`(부속지번). `main`에 대해
  `EXCLUDE (building_id WITH =, effective_period WITH &&) WHERE (relation_kind = 'main')`.
- `catalog.building_unit.building_id` — FK 추가. `parcel_id`는 그다음 증분에서 제거한다.

두 변경 모두 **부속지번 silver 계약이 실물이 된 뒤**에 착수한다. 지금
`silver.building_register_sub_parcel`은 `"status": "planned"`이며, 채울 소스가 없는 표를 먼저
만드는 것은 G1이 세는 바로 그 결함이다.

### 5. `catalog.parcel_transition`은 생산자가 생길 때 만든다

모양은 `administrative_unit_transition`을 그대로 따른다 —
`transition_kind IN ('merged_into', 'split_from', 'replaced_by')`, 기간, 순환 방지 트리거.

지금 만들지 않는 이유는 **채울 데이터가 없기 때문이다.** 분할·합병 이력을 담은 소스는 현재
어느 채널에서도 수집되지 않는다. 표만 먼저 만들면 G1의 "생산자 없는 canonical 표" 수가 늘고,
그것은 이 ADR이 줄이려는 수다. 소스가 확인되는 시점에 별도 ADR로 연다.

## 기각한 대안

### `complex_id`를 nullable로만 바꾼다

가장 작다. 산단 밖 필지는 표현할 수 있게 된다. 그리고 나머지 셋을 전부 놓친다 — 걸친 필지도,
지정·해제 이력도, 확장 시점도. 그리고 `parcel_boundary_mirror`가 이미 nullable인데도 부족하다는
것이 이 ADR의 출발점이다. mirror는 서빙 사본이라 이력을 가질 이유가 없지만 `catalog.parcel`은
카탈로그다.

무엇보다, 이 안은 **소속을 필지의 속성으로 두는 전제를 유지한다.** 소속은 두 실체 사이의 사실이며
어느 쪽의 속성도 아니다. 산단이 확장되어 필지를 새로 포함하게 되는 사건을 필지 행의 UPDATE로
적는 한, 그 사건의 시점·근거·방법은 계속 사라진다.

### 소속을 산업단지 쪽 배열로 둔다 (`industrial_complex.parcel_ids uuid[]`)

방향만 뒤집었을 뿐 같은 결함이다. 배열 원소는 자기 기간도, 자기 계보도, 자기 `membership_kind`도
가질 수 없다. 그리고 `overlap_ratio` 같은 관계 자체의 속성을 둘 자리가 없다 — 레이크하우스는
이미 그 값을 계산해서 갖고 있다.

### `silver.complex_parcel_memberships`를 그대로 읽고 Postgres에는 두지 않는다

레이크하우스가 이미 옳게 모델링했으니 Postgres에 복제하지 말자는 안. SSOT 관점에서 가장 매력적이고,
실제로 이 ADR은 그 표의 어휘와 게이트를 그대로 채택한다.

기각한 이유는 실측이다. **Rust가 Iceberg 행을 읽을 수 없다.** 워크스페이스에 Iceberg 클라이언트도
`object_store`도 없고, Parquet 리더는 `#[cfg(test)]` 안에만 있다. 서빙 경로가 실제로 읽는 것은
Postgres이며, 레이크하우스에서 Postgres로 넘어오는 다리는 지금 `parcel_boundary_mirror` 적재기
하나다. 이 안은 그 다리가 생긴 뒤에야 성립하고, 그때가 되면 이 표는 mirror와 같은 성격의
서빙 투영이 된다 — 그 전환은 이 ADR이 만드는 표를 버리는 것이 아니라 채우는 쪽을 바꾸는 것이다.

## Consequences

- **산단 밖 필지가 표현 가능해진다.** 전국 필지 적재기가 `catalog.parcel`에 쓸 수 있게 되는
  전제 조건이며, 지금은 `complex_id NOT NULL` 때문에 쓸 수 없다.
- **소속의 시점과 근거가 남는다.** `source_method`가 `official_list`인지 `geometry_overlay`인지
  구분되므로, 공식 목록과 기하 중첩이 어긋나는 필지를 질의로 찾을 수 있다.
- **G1 지표는 나빠지지 않는다 — 그러나 그 이유를 정직하게 적는다.** 마이그레이션의
  `INSERT INTO`도 생산자로 세므로(`render-foundation-baseline.py:60-71`), 백필이 있는 새 표는
  즉시 생산자를 갖는다. 생산자 없는 표 21개는 그대로이고 분모만 58 → 59가 된다. 하지만 백필
  생산자는 **1회성 다리**이지 운영 생산자가 아니다. 진짜 생산자는
  `silver.complex_parcel_memberships`를 읽는 코드이며 그것은 아직 없다. 이 표를 "생산자 있음"으로
  세는 것은 측정의 한계이고, G1 재측정이 그 한계를 드러내야 한다.
- **API 계약이 변한다.** `ParcelResponse.complex_id`는 3단계에서 사라지고,
  `GET /catalog/v1/complexes/{id}/parcels`는 소속 표를 경유해 읽는다. 저장소 안 소비자는 이 필드를
  읽지 않으므로 이 변경으로 깨지는 코드는 없다.
- **`catalog.parcel`을 참조하는 다른 표는 건드리지 않는다.** `parcel_industry_assignment`,
  `spatial_layer`, `digital_twin_asset`, `manufacturer.primary_parcel_id`는 필지를 지명할 뿐
  소속을 주장하지 않으므로 그대로다.

## 이행 순서

1. **표를 추가한다** (`20260807000001`). 기존 `parcel.complex_id`에서 백필하되, 기간의 하한은
   `parcel.created_at::date`, `membership_kind`는 `inside`, `source_method`는 `official_list`로
   둔다. 계보는 `20260727000001`이 만든 것과 같은 형태의 `legacy:` source record를 마이그레이션
   안에서 발급한다 — **레이크하우스에 없는 출처를 지어내지 않기 위해서다.** 이 단계는 기존 컬럼을
   건드리지 않으므로 단독으로 배포 가능하다.
2. **읽기를 옮긴다.** `/complexes/{id}/parcels`와 산단 스코프 질의가 소속 표를 읽게 하고,
   `parcel.complex_id`에 쓰기 금지 트리거를 건다. 두 경로가 같은 답을 내는지 대조하는 테스트를
   먼저 둔다.
3. **컬럼과 API 필드를 제거한다.** OpenAPI 사본 두 개를 함께 고친다.
4. 부속지번 silver가 실물이 된 뒤 §4를 착수한다.

각 단계는 `cargo xtask verify` 녹색에서만 커밋한다([ADR-0004](./0004-verification-ssot.md)).

## 남은 부채

1. **`parcel_transition`이 없다.** §5. 필지 분할·합병 이력을 담은 소스가 확인되지 않았다.
   PNU로 과거를 조회하는 기능은 이 표가 없는 한 불가능하다.
2. **`building.parcel_id`와 `building_unit.building_id`는 여전히 틀려 있다.** §4. 모양은
   확정했으나 소스가 `planned` 상태라 착수하지 않았다.
3. **소속 표의 운영 생산자가 없다.** 백필만 있다. `silver.complex_parcel_memberships`를 읽는
   경로는 Rust가 Iceberg를 읽을 수 있게 된 뒤에야 생긴다.
4. **`industrial_complex.primary_bjdong_code`는 여전히 스칼라다.** 여러 시군구에 걸친 산단이
   실재하며, 같은 형태의 결함이다. 소속 표가 자리 잡은 뒤 같은 형판으로 처리한다.
