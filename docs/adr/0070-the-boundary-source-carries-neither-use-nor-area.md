# ADR 0070: 경계 원천은 필지의 용도도 면적도 나르지 않는다

- Status: Accepted
- Date: 2026-09-01
- 관련: [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md) (같은 판정, 산업단지 쪽), [ADR-0019 소속은 컬럼이 아니라 날짜가 붙은 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md) (같은 표의 한 칸 옆), [ADR-0068 명령은 자기가 연 객체를 이름한다](./0068-the-command-names-the-object-it-read.md), [ADR-0069 한 칸에 다섯 가지가 들어 있다](./0069-one-column-holds-five-kinds-of-thing.md)

## Context

`docs/roadmap/foundation-goals.md` 의 G1 은 생산자 없는 canonical 표를 가장 큰 구조적 격차로
지목하고, 2026-09-01 재측정에서 **61개 중 20개**다. 그중 `catalog.parcel` 하나가 나머지를
함께 막는다 — 이 표를 가리키는 외래키가 여덟 개이고 그중 넷이 `NOT NULL` 이다.

```
catalog.building.parcel_id                  NOT NULL   ← 생산자 없는 20표에 포함
catalog.building_unit.parcel_id             NOT NULL   ← 포함
catalog.manufacturer.primary_parcel_id      NOT NULL   ← 포함
catalog.parcel_industry_assignment.parcel_id NOT NULL  ← 포함
catalog.digital_twin_asset.parcel_id        nullable   ← 포함
catalog.spatial_layer.parcel_id             nullable   ← 포함
catalog.parcel_marker_anchor.parcel_id      nullable
serving_postgis.parcel_boundary_mirror.parcel_id nullable
```

### 로드맵이 적은 막힘 이유는 더 이상 사실이 아니다

> "필지(`parcel`)는 다르다 — 실버에 아직 없으므로 소유자 결정이 여전히 선행한다."
> (`foundation-goals.md`, 2026-08-06 실측)

2026-09-01 실측: `silver.parcel_boundaries` 에 **39,861,511행**이 있고, 재실행 안전장치가
그 표에서 돈다(root ADR-0069). 정본 실버가 없어서 막힌 것이 아니다.

### 실제로 막는 것은 두 칸이다

```sql
catalog.parcel.kind     text   NOT NULL
    CHECK (kind = ANY (ARRAY['factory','support','public','river','other']))
catalog.parcel.area_m2  bigint NOT NULL
    CHECK (area_m2 >= 0)
```

**`kind` 는 사람이 정하는 값이다.** 이 어휘를 쓰는 유일한 경로는
`catalog-application/src/update_parcel_kind.rs` 이고, 그 입력은 `applied_by: StaffId` 를
필수로 받아 편집 원장에 귀속을 남긴다(ADR-0023). 즉 이 칸은 수집이 아니라 **판단**으로
채워진다. 그리고 어휘 자체가 산업단지 내부의 토지 이용 구분이다 — 공장·지원·공공·하천 은
단지 안에서만 뜻이 있고, 단지 밖 필지에 붙일 값이 이 다섯 중에 없다. `other` 로 3,986만
행을 채우는 것은 모른다는 사실을 아는 것처럼 적는 일이다.

`complex_id NOT NULL` 을 지운 [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)
가 같은 결함을 한 칸 옆에서 이미 고쳤다. 그때의 근거 — "산업단지에 속하지 않는 필지가 표현
불가이고 전국 필지 대부분이 그렇다" — 는 `kind` 에도 그대로 적용된다.

**`area_m2` 는 원천에 없다.** 필지 변환기가 shapefile 에서 읽는 속성은 `PNU` 와 `JIBUN`
둘뿐이다(`vworld_cadastral_shapefile_silver_export.rs`). `silver.parcel_boundaries` 계약의
20개 칸 어디에도 면적이 없다. 폴리곤에서 계산할 수는 있지만 그것은 다른 숫자다 —
`catalog_domain::Parcel` 은 이 칸을 "Official parcel area" 라고 적고 있고, 공부면적과 도형
면적은 같지 않다. 도형으로 계산한 값을 공부면적 자리에 넣으면
[ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md) 이 금지한 것을 한다.

### 이것이 왜 지금 보이지 않았나

두 칸 다 `NOT NULL` 이라 **생산자를 쓰기 시작하면 첫 줄에서 막힌다.** 그래서 생산자가 없고,
생산자가 없으니 표가 비어 있고, 표가 비어 있으니 이 칸들이 문제라는 사실이 드러나지 않는다.
ADR-0040 이 산업단지에서 같은 고리를 끊었다: 1,442곳 대신 6곳이 서빙되던 이유가
`primary_bjdong_code NOT NULL` 하나였다.

> **컬럼을 필수로 두는 것은 생산자에 대한 주장이다.** "이 값은 항상 있다"가 아니라 "이 값을
> 항상 만드는 무언가가 있다"는 주장이고, 그 주장이 거짓이면 결과는 둘 중 하나다. 없는 값을
> 지어내거나, 표가 비어 있거나. — ADR-0040

## Decision

1. **`catalog.parcel.kind` 를 nullable 로 내린다.** `parcel_kind_check` CHECK 은 **그대로
   둔다** — Postgres 는 `NULL` 로 평가되는 CHECK 행을 받아들이므로, 값이 있으면 여전히 다섯
   어휘 중 하나여야 하고 없으면 통과한다. "아직 판단하지 않았다"와 "틀린 값"은 다른 사실이고
   이 구분은 유지된다.

2. **`catalog.parcel.area_m2` 를 nullable 로 내린다.** `parcel_area_m2_check` 도 그대로 둔다.
   면적을 아는 원천이 생기면 그때 채운다. 도형에서 계산해 채우는 것은 이 결정이 허용하는
   범위가 아니다 — 그것은 다른 사실이고, 다른 칸을 쓸 일이다.

3. **도메인·계약·이벤트가 같은 말을 한다.** DB 만 nullable 로 바꾸고 Rust 타입을 그대로 두면
   첫 `NULL` 행에서 읽기가 실패한다. `catalog_domain::Parcel` 의 `kind` 와 `area_m2` 가
   `Option` 을 가지고, `foundation_contracts` 의 필지 DTO 와 OpenAPI 의 `required` 목록이
   따라간다.

4. **판단을 요구하는 경로는 약해지지 않는다.** `update_parcel_kind` 는 여전히
   `ParcelKind`(Option 아님)를 받는다. 사람이 용도를 정할 때 "모름"으로 정할 수는 없다.
   nullable 이 뜻하는 것은 **적재 시점에 아직 아무도 정하지 않았다**이지, 용도가 선택
   사항이라는 것이 아니다.

5. **이 ADR 은 생산자를 만들지 않는다.** 막고 있던 것을 치울 뿐이다. 3,986만 행을 어떤
   단위로, 어떤 순서로 넣을지는 다음 결정이고, 그 결정은 이 두 칸이 열린 뒤에야 쓸 수 있다.

## Consequences

`catalog.parcel` 에 생산자를 쓸 수 있게 된다. 그것이 생기면 `catalog.building`,
`catalog.building_unit`, `catalog.manufacturer`, `catalog.parcel_industry_assignment` 의
필수 외래키도 채울 대상이 생긴다 — 생산자 없는 20표 중 다섯이 한 결정에 걸려 있었다.

**읽는 쪽이 `NULL` 을 보게 된다.** `GET /catalog/v1/parcels` 응답의 두 필드가 선택이 되고,
그것을 소비하는 화면은 "미지정"과 "값 없음"을 구별해 보여야 한다. 지금은 표가 비어 있어
아무도 이 응답을 받지 못하므로, 호환성이 아니라 **처음부터 그렇게 만드는 문제**다.

**면적이 필요한 기능은 여전히 막혀 있다.** 이 결정은 면적을 만들어 주지 않는다. 공부면적을
나르는 원천(예: 토지대장)을 수집하기 전까지 `area_m2` 는 비어 있고, 그 사실이 이제 표에
정직하게 나타난다 — 예전에는 표 전체가 비어 있어서 같은 사실이 보이지 않았다.

**로드맵의 G1 서술을 고쳐야 한다.** "필지는 실버에 없다"는 2026-08-28 이후 사실이 아니고,
그 문장이 남아 있는 동안 이 항목은 데이터 문제로 보였다. 실제로는 스키마 문제였다.
