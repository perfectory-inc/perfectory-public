---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-11
---

# ADR 0024: 서빙 투영은 타일 계약이 지명한 것만 싣는다

- Status: Accepted
- Date: 2026-08-10
- 관련: [ADR-0019 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md) (같은 결함의 서빙 쪽), [ADR-0021 아무도 읽지 않는 표면은 옮기지 않고 지운다](./0021-an-unread-surface-is-deleted-not-migrated.md), [ADR-0022 "현재"는 오늘이다](./0022-current-means-today-and-one-view-says-so.md)
- 마이그레이션: `20260810000004_parcel_publication_drops_the_complex_code.sql`

## Context

[ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)는 `catalog.parcel.complex_id NOT NULL`을
제거했다. 근거는 **산업단지에 속하지 않는 필지가 표현 불가**라는 것이었고, 전국 필지 대부분이 그렇다.

그 결함은 카탈로그에서만 고쳐졌다. **지도 타일이 실제로 잘려 나가는 표에 같은 것이 남아 있다.**

```sql
-- 20260724000001_spatial_tile_publication.sql:332
CREATE TABLE serving_postgis.parcel_boundary_publication (
    pnu character(19) NOT NULL,
    ...
    complex_id uuid,                    -- nullable
    official_complex_code text NOT NULL, -- 필수
```

같은 사실이 한 행에 두 번 적혀 있고, 그중 **하나만 필수**다. 산단 밖 필지를 이 표에 넣을 방법이
구조적으로 없다. 유일한 기존 생산자가 그 강제를 증명한다 — 시드는
`JOIN catalog.industrial_complex` 내부 조인과 `WHERE mirror.complex_id = '<한 산단>'`으로만 쓸 수
있다(`infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql:107,117`).

### 그 값은 실제로 타일에 실려 나간다 — 그것이 문제다

이 ADR의 초안은 "아무도 읽지 않으니 지운다"고 적었다. **틀렸다.**
`scripts/tiles/martin-dynamic.yaml`의 `parcels` 소스는 두 속성을 선언한다.

```yaml
properties:
  pnu: Parcel number
  official_complex_code: Industrial-complex code
```

그리고 `scripts/tiles/mvt_assert.rs`는 **모든 레이어의 모든 피처**에 그 속성이 있다고 전제한다 —
피처의 신원 자체가 `(layer, pnu, complex_code)`로 정의돼 있다. `tiles_slice_contract.rs`도
Martin YAML이 그 속성을 선언하는지 검사한다.

초안이 근거로 삼은 `vector_tile_feature_filter_properties`는 다른 질문에 답하는 함수다 — 그 doc가
"**consumer-safe property names that may be used for client-side filtering**"이라고 적는다. 타일에
무엇이 실리는가가 아니라 클라이언트가 필터에 쓸 수 있는 것이 무엇인가이며, 그 함수를 부르는 곳은
자기 테스트뿐이다.

### 그래서 진짜 근거는 이것이다

**타일 한 장이 소속을 주장하면 안 된다.**

[ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md)은 소속이 자료가 말하는 사실이고 도형으로
계산해내는 것이 아니라고 정했다. 필지 피처에 산단 코드를 실으면, 그 타일은 **발행 시점에 얼어붙은
소속 주장**을 들고 다닌다. 산단이 확장되거나 해제되면 타일과
`catalog.parcel_current_complex`가 어긋나고, 어긋남을 막는 것이 없다. 소속을 묻는 질문에 답이 둘이
된다 — 하나는 데이터, 하나는 캐시된 그림.

지금 그 값이 채워지는 경로가 그 성격을 그대로 보여준다. `parcel_boundary_mirror.complex_id`에
프로덕션 생산자가 없고, 시드가 픽스처 산단 하나를 하드코딩해 내부 조인으로 붙인다. 즉 실려 나가는
소속은 **자료에서 온 것이 아니라 픽스처 상수**다.

필지 타일이 답해야 하는 것은 "이 다각형이 어느 필지인가"이고, 그 답은 PNU다.

## Decision

### 1. 필지 피처는 소속을 싣지 않는다 — 표에서 뷰, Martin, 검증기까지

`parcel_boundary_publication.official_complex_code`를 제거하고, 그 값이 흐르던 경로를 끝까지 따라
지운다.

| 자리 | 조치 |
|---|---|
| `serving_postgis.parcel_boundary_publication` | 컬럼 제거 |
| `serving_postgis.parcel_boundary_current` 뷰 | `pnu`, `geom`만 남김 |
| `martin-dynamic.yaml`의 `parcels`·`parcel_anchor` | 속성 선언 제거 |
| `mvt_assert.rs` | 피처 신원을 `(layer, pnu)`로 좁힘 |
| `tiles_slice_contract.rs` | 두 레이어의 기대 속성에서 제거 |
| 시드·픽스처 | 산단 내부 조인 제거 |

nullable로 완화하지 않는다. 남기면 적재기가 "그럼 뭘 넣지"를 다시 묻게 되고, 자료에서 오는 답이
없으므로 픽스처 상수를 또 넣게 된다.

**`parcel_anchor_aggregate`는 예외이며 그대로 둔다.** 그 레이어는 필지 하나가 아니라 **산단 하나**를
가리키는 마커이고, 거기서 `official_complex_code`는 소속 주장이 아니라 **그 피처 자신의 신원**이다.
자기 이름을 싣는 것은 남의 소속을 싣는 것과 다르다.

### 2. `complex_id`는 남긴다 — 지우지도, 채우지도 않는다

nullable이고 소비자가 없다는 점은 같지만, 성격이 다르다. 이것은 **소속을 서빙 투영이 실을지에 대한
열린 질문**([ADR-0021](./0021-an-unread-surface-is-deleted-not-migrated.md) 남은 부채)의 자리이고,
`parcel_boundary_mirror`가 같은 컬럼을 같은 상태로 갖고 있다. 하나만 지우면 두 서빙 표가 서로 다른
모양이 되어 그 질문을 답하기 더 어려워진다.

지금 지우지 않는 이유를 정확히 적는다: **필수가 아니라서 적재기를 막지 않는다.** 막는 것은
`official_complex_code`뿐이고, 이 증분은 막는 것만 치운다.

### 3. 서빙 투영은 계약이 지명한 속성만 싣는다

일반 규칙으로 적는다. `vector_tile_feature_filter_properties`가 레이어별 공개 속성의 정본이며,
서빙 뷰가 그보다 많이 실으면 그것은 **계약 밖 데이터 노출**이다.

이 규칙을 검사로 만들지는 않는다(§남은 부채 2). 지금 레이어가 셋이고 뷰가 둘이라 기계 검사의 값이
그것을 유지하는 비용보다 크지 않다. 레이어가 늘면 다시 본다.

## 기각한 대안

### `NOT NULL`만 풀고 컬럼은 남긴다

가장 작다. 적재기가 막히는 문제는 즉시 해소된다.

기각한 이유는 **적재기가 그 칸에 무엇을 넣을지 여전히 답이 없기 때문이다.** 산단 밖 필지는 NULL,
안쪽은 코드 — 그러면 같은 사실이 `complex_id`와 두 벌이 되고 둘을 묶는 제약이 없다. 그리고 그 값을
읽는 소비자가 애초에 없으므로, 채우든 비우든 아무 질문에도 답하지 않는 컬럼이 남는다.

### 뷰에서만 빼고 표에는 남긴다

계약 위반(계약에 없는 속성 노출)은 즉시 해소되고, 표 변경이 없어 되돌리기 쉽다.

기각한 이유는 `NOT NULL`이 표에 있기 때문이다. 뷰에서 빼도 **적재기는 여전히 값을 만들어야 한다.**
이 증분의 목적이 그 강제를 없애는 것이므로 표를 건드리지 않으면 목적을 달성하지 못한다.

### 소속을 발행 시점에 `parcel_current_complex`로 해석해 싣는다

서빙 투영이 산단을 계속 갖되, 얼린 문자열이 아니라 소속 표에서 파생한다.

기각한 이유는 **그 값을 요구하는 소비자가 없기 때문이다.** 계약이 필지 레이어에 `pnu`만 지명하므로,
파생해서 실어도 타일에 나가지 않거나 나가면 계약 위반이다. 소속을 서빙에 싣는 것이 필요해지는 날
[ADR-0021](./0021-an-unread-surface-is-deleted-not-migrated.md)의 열린 질문과 함께 판단한다 — 그때는
`complex_id`(§Decision 2)가 그 자리다.

## Consequences

- **산단 밖 필지를 서빙 투영에 넣을 수 있게 된다.** [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)
  3단계가 카탈로그에서 연 것과 같은 문을 서빙에서 연다. 전국 필지 적재기의 선행조건 하나가 해소된다.
- **타일이 계약대로 나간다.** 뷰가 내보내는 속성이 `vector_tile_feature_filter_properties`의
  `parcels` 항목과 일치한다.
- **시드와 증명 픽스처가 바뀐다.** 유일한 기존 생산자가 그 컬럼을 채우고 있었으므로 함께 고친다.
  시드가 `industrial_complex`를 내부 조인할 이유도 사라진다.
- **`parcel_boundary_mirror`는 건드리지 않는다.** 그 표의 `complex_id`·`parcel_id`는 처음부터
  nullable이고 프로덕션 생산자가 채우지 않는다. 이 증분의 범위 밖이다.
- **G1 지표는 변하지 않는다.** 표가 늘거나 줄지 않는다.

## 남은 부채

1. **`parcel_boundary_publication`의 production 경계는 생겼지만 publishable upstream 생산자가
   없다.** `publish-parcel-boundary-postgis`가 봉인된 source evidence 하나에서
   출처를 읽어 이 표를 append하고, load id로 행 집합을 분리하며, source/target의 EPSG:5179 EWKB
   content digest와 target lineage를 대조한 뒤에만 load를 `succeeded`로 닫는다. 따라서 이 표에
   쓰는 production 경계 자체는 생겼다. 다만 현재 mirror writer는 bounded QA only이고 그 upstream
   실행 증거가 production cutover와 national rollout을 부정하므로 봉인 가능한 기존 run은 0개다.
   production-capable Iceberg→run→evidence 생산자가 오기 전에는 end-to-end production producer가
   완성됐다고 보지 않는다.
2. **서빙 뷰가 계약보다 많이 싣는 것을 막는 검사가 없다.** §Decision 3. 지금은 사람이 대조해야
   한다.
3. **`complex_id` 두 서빙 표에 남아 있다.** §Decision 2. 소속을 서빙이 실을지의 판단과 함께.
