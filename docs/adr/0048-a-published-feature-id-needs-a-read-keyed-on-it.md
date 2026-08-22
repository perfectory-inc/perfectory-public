# ADR 0048: 발행된 feature id 에는 그 id 로 여는 조회구가 필요하다

- Status: Accepted
- Date: 2026-08-23
- 관련: [ADR-0043 정본 id 는 다시 계산하지 않고 읽는다](./0043-a-canonical-id-is-read-not-recomputed.md), [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md), [ADR-0024 서빙 투영은 타일 계약이 이름한 것만 싣는다](./0024-the-serving-projection-carries-only-what-the-tile-contract-names.md)

## Context

산업단지 하나에는 서로 계산되지 않는 식별자가 **둘** 있다.

| | 만드는 곳 | 형태 |
|---|---|---|
| `catalog.industrial_complex.id` | Catalog 가 행을 만들 때 `Uuid::now_v7()` | UUIDv7 |
| lakehouse `complex_id` | Bronze→Silver 작업이 원천 코드에서 유도 | UUIDv5 |

migration `20260818082317_industrial_complex_carries_its_sourced_description.sql` 주석이
`111010` 실측으로 적어 둔 그대로다 — 정본 행은 `01a0136d-…-7e61-…`, lakehouse 행은
`7df3859c-…-51fa-…`. 두 값은 서로에게서 계산되지 않는다.

바깥으로 **발행되는** 쪽은 후자다.

- `serving_postgis.industrial_complex_boundary_publication.complex_id` 가 lakehouse id 이고,
  DDL 주석이 `Not catalog.industrial_complex.id` 라고 명시한다.
- `promote-industrial-complex-boundary-runtime` 이 `feature_id_property: "complex_id"` 로
  올리므로, `complex` 벡터 타일의 feature id 가 그 값이다.
- Gold profile 객체 키도 그 값에서 파생된다.

그런데 2026-08-23 기준 Catalog v1 의 산업단지 단건 조회는 `GET /catalog/v1/complexes/{id}`
하나뿐이고, 그 `{id}` 는 **정본 id** 다. 즉 **지도에서 도형을 눌러 손에 쥔 값으로는 그 단지를
열 수 없었다.** 두 id 를 섞어 호출하면 404 가 나는데, 404 는 "그런 단지가 없다"로 읽힌다 —
실제 원인은 "식별자 공간이 다르다"인데도.

`catalog.industrial_complex (lakehouse_complex_id) WHERE lakehouse_complex_id IS NOT NULL` 부분
유니크 인덱스는 같은 migration 에 이미 있다. 조회구만 없었다.

## Decision

1. **발행한 식별자에는 그 식별자로 여는 조회구를 함께 발행한다.** 소비자가 손에 쥘 수 있는
   값이 하나뿐인데 그 값으로 여는 API 가 없으면, 그 발행은 미완이다.
2. Catalog v1 에 `GET /catalog/v1/complexes/by-lakehouse-id/{lakehouse_complex_id}` 를 추가한다.
   응답 본문은 `GET /catalog/v1/complexes/{id}` 와 **같은** `IndustrialComplexResponse` 다 —
   같은 자원을 다른 열쇠로 여는 것이지 다른 표현을 만드는 것이 아니다.
   `/catalog/v1/parcels/by-pnu/{pnu}` 가 이미 쓰는 모양을 따른다.
3. 노출 등급도 `GET /catalog/v1/complexes/{id}` 와 **같게**(비보호) 둔다. `listComplexes` 가
   이미 익명 호출자에게 `lakehouse_complex_id` 를 포함한 전체 행을 준다. 한 열쇠만 잠그는 것은
   이 표면이 갖고 있지 않은 기밀성을 주장하는 것이고, 잠금이 아니라 장식이다.
4. **소비자는 식별자 공간을 입구에서 검증한다.** `UUIDv5` 형식(버전 니블 `5`)을 강제하면
   정본 `now_v7()` 값을 이 경로로 보내는 실수가 404 가 아니라 400 으로 끝난다. gongzzang 은
   `shared_kernel::lakehouse_complex_id::LakehouseComplexId` 와 웹의
   `LAKEHOUSE_COMPLEX_ID_PATTERN` 이 이를 강제한다. DB 쪽 같은 불변식은
   `industrial_complex_lakehouse_complex_id_is_uuid_v5` 와
   `complex_boundary_publication_complex_id_is_uuid_v5` 다.
5. **응답이 요청한 식별자를 담는지 확인한다.** `complex_reader.rs` 는 응답의
   `lakehouse_complex_id` 가 요청값과 다르거나 `null` 이면 거부한다. 이 검사가 막는 실제 사고:
   쓰기 API 로 등록되어 lakehouse id 가 없는 행이 lakehouse id 조회의 답으로 흘러가면,
   **사용자가 누른 것과 다른 단지**를 보여 준다.

## Consequences

- Catalog v1 표면이 경로 하나 늘어난다. `docs/openapi/catalog.v1.json` 은
  `export-catalog-openapi` 재생성으로 갱신되고, gongzzang 의 소비자 pin
  (`foundation-platform-catalog-api-contract.v1.pin.json`) 이 새 sha256 과
  `getComplexByLakehouseId` 항목을 함께 싣는다.
- `official_complex_code` 로 여는 조회구는 **만들지 않는다.** 사람이 인용하는 코드일 뿐
  타일이 발행하는 식별자가 아니고, 두 번째 열쇠는 세 번째 id 공간을 부를 뿐이다.
- 정본 행에 `lakehouse_complex_id` 가 없는 단지(쓰기 API 로 등록된 것)는 이 경로로 열리지
  않는다. 열려야 할 이유도 없다 — 그런 단지는 타일에도 없다.
