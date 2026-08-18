# ADR 0040: 아무도 채우지 않는 컬럼은 필수일 수 없다

- Status: Accepted
- Date: 2026-08-18
- 관련: [ADR-0033 주소 출처가 없는 산업단지는 표현할 수 없다](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md), [ADR-0034 행정구역 코드는 자기 정밀도를 싣고 다닌다](./0034-an-administrative-code-carries-its-own-granularity.md), [ADR-0035 쓰지 않는 지역은 필수가 아니다](./0035-a-region-the-pipeline-does-not-use-is-not-required.md), [ADR-0019 소속은 컬럼이 아니라 날짜가 붙은 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md)
- 완결: [ADR-0035](./0035-a-region-the-pipeline-does-not-use-is-not-required.md) Consequences 의 남은 일 (3)

## Context

`docs/roadmap/production-readiness.md` 가 지목한 남은 구간은 실버에서 Postgres canonical 로
넘어오는 길이고, 산업단지가 그 사례다. 2026-08-18 실측:

```
r2.silver.industrial_complexes   1,442행
r2.gold.complex_catalog          1,442행
catalog.industrial_complex           6행   ← 시드 1 + 테스트 픽스처 5. 생산자 없음
```

`GET /catalog/v1/complexes` 는 `catalog.industrial_complex` 를 그대로 읽는다
(`catalog-infrastructure/src/sqlx_repository.rs` 의 `list_complexes`). 즉 읽을 데이터가 없어서가
아니라 **그 표에 쓰는 코드가 없어서** 화면에 6곳만 나온다.

그 코드를 쓰지 못하게 막고 있던 것은 하나다.

```sql
catalog.industrial_complex.primary_bjdong_code  character(10) NOT NULL
                                                CHECK (~ '^[0-9]{10}$')
```

[ADR-0034](./0034-an-administrative-code-carries-its-own-granularity.md) 가 만든 주소 해소기가
실물에 대해 답한 것은 **시군구 정밀도까지**다. 1,442건 중 읍면동 코드를 가진 것은 **0건**이고,
1건(`446400`)은 시군구 코드조차 없다. `gold.complex_catalog` 계약에는
`primary_bjdong_code` 컬럼이 **아예 없다** — `sido_code`·`sigungu_code`·`address_text` 만 있다.

[ADR-0035](./0035-a-region-the-pipeline-does-not-use-is-not-required.md) 는 레이크하우스 계약에서
같은 요구를 이미 내렸고, Consequences 에 남은 일 (3) 으로
"`catalog.industrial_complex.primary_bjdong_code` 의 `NOT NULL` 은 아직 열려 있다"고 적어 두었다.
이것이 그 결정이다.

**컬럼을 필수로 두는 것은 생산자에 대한 주장이다.** "이 값은 항상 있다"가 아니라 "이 값을 항상
만드는 무언가가 있다"는 주장이고, 그 주장이 거짓이면 결과는 둘 중 하나다. 없는 값을 지어내거나,
표가 비어 있거나. 지금은 후자이고, 그것이 1,442곳 대신 6곳이 서빙되는 이유다.

## Decision

1. **`catalog.industrial_complex.primary_bjdong_code` 를 nullable 로 내린다.**
   마이그레이션 `20260818050655_allow_industrial_complex_without_a_bjdong_code.sql`.
   `industrial_complex_primary_bjdong_code_shape` CHECK 은 **그대로 둔다** — Postgres 는 CHECK 이
   `NULL` 로 평가되는 행을 받아들이므로, 코드가 있으면 여전히 10자리 숫자여야 하고 없으면 통과한다.
   "모른다"와 "형식이 틀렸다"는 다른 사실이고 이 구분은 유지된다.

2. **도메인·계약·이벤트가 같은 말을 한다.** DB만 nullable 로 바꾸고 Rust 타입을 `String` 으로
   두면 첫 `NULL` 행에서 읽기가 실패한다. `catalog_domain::IndustrialComplex`,
   `UpsertIndustrialComplexCommand`, `RegisterIndustrialComplexInput`,
   `IndustrialComplexCatalogRow`, `foundation_contracts::catalog::IndustrialComplexResponse`,
   `RegisterComplexRequest` 가 모두 `Option<String>` 을 가진다. OpenAPI 의
   `IndustrialComplexResponse.primary_bjdong_code` 는 `required` 에서 빠진다.
   `validate_primary_bjdong_code` 는 값이 있을 때만 돈다 — 검사는 약해지지 않고 적용 범위만 좁아진다.

3. **이벤트는 새 버전을 만든다.** `catalog.industrial_complex.created.v2` 의
   `primary_bjdong_code` 를 `Option` 으로 바꾸는 것은 기존 페이로드의 의미를 바꾸는 일이고,
   `foundation-shared-kernel/src/events/catalog_v1.rs` 자신이 "구조 변경은 새 버전"이라고 적어 두었다.
   `catalog.industrial_complex.created.v3` 를 더하고 v1·v2 는 손대지 않는다. 새 생성은 v3 를 쓴다.
   *이 규칙이 막는 실제 사고:* `payload.primary_bjdong_code` 가 항상 문자열이라고 가정한 소비자가,
   같은 이벤트 타입에서 어느 날 `null` 을 받는 일.

4. **`load-industrial-complex-canonical` 이 이 표의 생산자다.**
   `r2.gold.complex_catalog` 의 현재 Iceberg 스냅샷을 읽어 `catalog.industrial_complex` 에
   `official_complex_code` 자연키로 upsert 한다. 지우지 않는다(append-only).
   같은 자연키가 이미 있으면 **그 행의 `id` 를 유지한 채 갱신한다** — `id` 는
   `catalog.parcel`·`catalog.blueprint` 등 9개 외래 키가 가리키는 값이므로, 새 `id` 로 다시
   넣는 것은 그 참조를 끊는 일이다. Gold 스냅샷 안에 같은 코드가 두 번 나오면 거부한다.

5. **적재기는 Gold 행이 싣고 있는 것만 쓴다.** `gold.complex_catalog` 에 `primary_bjdong_code`
   컬럼이 없으므로 적재된 모든 행의 그 값은 `NULL` 이다 — 삽입이든 갱신이든. canonical 표의 이
   컬럼들은 Gold 의 투영이고, Gold 에서 오지 않은 값이 재적재를 살아남으면 그 행은 자기 출처를
   잘못 말하게 된다. 값을 지어내지 않는다는 규칙의 반대 방향이 이것이다.

6. **`area_m2` 를 만들 수 없는 행은 건너뛰고 세어서 보고한다.** `official_area_sqm` 은 Gold 계약에서
   선택이고 `decimal(18,2)` 이며, `catalog.industrial_complex.area_m2` 는 `bigint NOT NULL
   CHECK (>= 0)` 이다. 값이 없거나 · 정수가 아니거나 · 음수인 행은 **건너뛴다.** 반올림은 없는
   값을 지어내는 것과 같은 부류다. 실행 요약 JSON 이 읽은 행수 · 삽입 · 갱신 · 무변경 · 건너뜀을
   사유별로 싣고, 건너뛴 행이 하나라도 있으면 경고를 남긴다.
   실측(2026-08-18 스냅샷): 1,442행 전부가 정수이고 `null` 이 없으므로 건너뛴 행은 0 이다.

7. **`kind` 가 계약 밖 값이면 전체가 실패한다.** 건너뛰지 않는다. 없는 면적은 출처의 공백이지만,
   `INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES` 밖의 분류값은 계약이 깨진 것이고, 한 행을 건너뛰어
   가려서는 안 된다.

8. **Iceberg 스냅샷 스캔은 한 곳에만 있다.** `industrial_complex_gold_profile_export` 안에 있던
   `iceberg_scan` 모듈과 `LakehouseObjectReader` 를 크레이트 수준 `lakehouse_snapshot_scan` 으로
   올리고 두 커맨드가 같이 쓴다. Iceberg 매니페스트를 읽는 두 번째 구현은 만들지 않는다.

### 위협 모델 ([ADR-0027](./0027-every-guard-declares-its-threat-model.md) 결정 1항)

- 부류: 결정 1·2·5 는 **표현 불가능(prevention)**, 결정 6·7 은 **탐지(detection)** 다.
- Prevents: `primary_bjdong_code` 자리에 시군구 코드나 `0000000000` 을 채워 넣는 일. 값이 없어도
  행을 쓸 수 있으므로 채워 넣을 이유가 사라진다. 값이 있으면 10자리 검사가 그대로 돈다.
- Prevents: 재적재가 canonical 행의 `id` 를 바꾸어 9개 외래 키를 끊는 일(결정 4).
- Prevents: 면적을 반올림해서 `area_m2` 를 지어내는 일(결정 6).
- Does not prevent: `446400` 을 포함한 1,442건의 읍면동 코드를 **알아내는** 일. 이 결정은 모른다고
  적을 수 있게 만들 뿐이고, 그 값은 여전히 출처의 문제다(ADR-0035 남은 일 (1)).
- Does not prevent: Gold 행의 `name`·`kind` 가 **사실인지**. 글자는 출처 그대로다.

## Consequences

- **`catalog.industrial_complex` 에 생산자가 생긴다.** `docs/roadmap/foundation-baseline.md` 의
  G1 수치는 **변하지 않는다** — 그 지표는 `INSERT INTO <표>` 문장이 코드에 있는지를 세고, 이 표는
  `unit_of_work.rs` 의 upsert 때문에 이미 "생산자 있음"으로 세어지고 있었다. 즉 이 표는 G1 이
  놓친 사례다: 쓰는 **문장**은 있었고 그 문장을 실물 출처로 부르는 **커맨드**가 없었다.
  이 결정이 만드는 것은 그 커맨드다.
- **`GET /catalog/v1/complexes` 가 1,442곳을 돌려준다.** 다만 그 응답에는 아직 **주소·상태·지정일·
  완공일·관리기관·시행자**가 없다. 이 컬럼들은 Silver 와 Gold 에는 있지만
  `catalog.industrial_complex` 에는 없고, 표를 넓히는 것은 API 계약이 함께 따라와야 하는 별개
  결정이다. **화면에 산업단지 주소가 나오지 않는다는 뜻이고**, 그 결정을 하기 전까지는 그렇다.
- **응답의 `primary_bjdong_code` 는 1,442곳 전부에서 `null` 이다.** 기존 6행만 값을 가진다.
  이 필드에 의존하는 소비자는 오늘 없다 — gongzzang 의 계약 핀
  (`foundation-platform-catalog-api-contract.v1.pin.json`)이 고정한 필수 응답 필드에 이 이름은 없다.
- **되돌리는 비용.** 스키마를 되돌리는 문장은 한 줄이지만 그 문장은 `NULL` 행이 하나라도 있으면
  실패한다. 즉 되돌리는 비용은 1,442건의 읍면동 코드 출처를 구하는 비용과 같다(ADR-0035 가 같은
  자리에서 말한 것과 같은 형태의 비용).
- **ADR-0035 와의 관계: 유지, 완결.** 결정 1~9 는 그대로다. Consequences 의 남은 일 (3) 이 이
  결정으로 닫히고, (1) `446400` 의 주소 출처와 (2) 지역을 dated fact 로 둘지 여부는 **여전히
  열려 있다.**
- 남은 일: (1) `catalog.industrial_complex` 를 넓혀 주소·상태·지정일을 서빙할지, 아니면 무거운
  상세를 Gold 프로필 포인터(ADR-0036·0037)로 보낼지 결정. (2) 이 적재기를 정기 실행에 넣는 일 —
  오늘은 운영자가 부르는 커맨드다.
