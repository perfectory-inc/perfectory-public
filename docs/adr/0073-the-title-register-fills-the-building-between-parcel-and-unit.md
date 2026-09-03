# ADR 0073: 표제부가 필지와 호 사이의 건물을 채운다

- Status: Accepted
- Date: 2026-09-03

## Context

호는 붙었고 건물이 비어 있다. ADR-0072 로 `catalog.building_unit` 이 19,536,363행을 얻었지만,
그 호들이 매달릴 `catalog.building` 은 생산자가 없는 그릇이다. 지금 호는 건물을 건너뛰고
필지에 직접 매달려 있어서, "이 호가 몇 층짜리 어떤 용도의 건물에 있는가" 를 답할 수 없다.

원천은 실측했다 (2026-09-03, 서버에서 실물 zip 을 열어서):

| | |
|---|---|
| Bronze | `bronze/source=hubgokr__building_register_main/` — 월별 전국 스냅숏 4개(4~7월), 각 ~678MB |
| 7월분 실물 | `mart_djy_03.txt` 3.51GB, 파이프 구분 77칸, 헤더 없음, **8,051,204행** |
| 구성 | 주건축물 7,375,417 · 부속건축물 675,235 · 구분 무표기 552 |
| Silver | **없음** — 층수 두 칸만 뽑아 쓰는 witness 독자(`building_register_title.rs`)가 전부 |

칸 배치는 표본이 아니라 값의 성질로 판정했다. 특히 면적 두 후보(`[26]`/`[28]`)는 11층
건물에서 `[28]=12,877 ≈ 층수 × [26]=1,261(바닥면적)` 로 **`[28]`이 연면적**임을 확정했다.
실측 배치:

```
[0] 관리대장 PK        [8..12] PNU 부품(시군구·법정동·대지구분·본번·부번)
[22] 동명칭            [24] 주부속구분명(주건축물/부속건축물)
[26] 건축면적          [28] 연면적 ←
[31] 구조코드 [32] 구조명   [34] 주용도코드 [35] 주용도명
[40] 호수(참고용)      [43] 지상층수 [44] 지하층수
[60] 사용승인일(yyyymmdd)
```

`catalog.building` 에는 자연키 칸이 없다 — `building_unit` 이 ADR-0072 직전에 가졌던 것과
같은 구멍이고, 같은 이유로 재적재가 매번 전체를 새 행으로 만든다.

## Decision

1. **표제부는 Silver 를 거친다.** `silver.building_register_titles` 를 신설하고
   Bronze zip → Silver 는 기존 관(층·호가 쓴 Rust 정규화 → scalar handoff → Iceberg)을
   그대로 탄다. 층·호·면적 Silver 와 나란한 네 번째 표이며, 건너뛰면 레이크하우스가 정본이라는
   원칙(root ADR)과 층·호가 이미 낸 전례를 둘 다 어긴다.
2. **주·부속 모두 싣는다.** 부속건축물 675,235동도 각자 PK 를 가진 실물 건물이다. 구분값이
   비어 있는 552행은 정규화 상태로 기록하고 싣는다 — 지어내지 않는다.
3. **`catalog.building` 투영은 ADR-0072 의 관을 재사용한다.** 매니페스트 기반 핸드오프 내보내기
   + `register_pk` upsert 적재기. `parcel_id` 는 PNU 부품 `[8..12]` 를 조립해
   `parcel_id_for_pnu` 로 유도하고, 필지에 없는 건물은 건너뛰고 센다.
4. **자연키 마이그레이션을 이 결정과 함께 낸다.** `catalog.building` 에
   `register_pk text NOT NULL UNIQUE`. 행 `id` 는 `perfectory.catalog.building.v1\0` 네임스페이스의
   UUIDv8 유도 — 호와 같은 construction 이라 재적재가 같은 id 를 낸다.
5. **칸 대응** (원천에 없는 것은 기본값 — 이웃에서 베끼지 않는다):
   `purpose_code`←`[34]`, `structure_code`←`[31]`, `floor_area_m2`←`[28]`(연면적),
   `stories`←`[43]`, `below_ground_floors`←`[44]`, `built_year`←`[60]`의 연도.
   옥탑 세 칸(`has_rooftop`/`rooftop_area_m2`/`rooftop_usage`)은 층별개요 Silver 의 소관이므로
   이번 적재는 스키마 기본값으로 두고, 채움은 별도 후속으로 남긴다.
6. **적재 대상은 최신 스냅숏 하나다(7월).** 월별 스냅숏 4개를 겹쳐 실으면 같은 건물이 네 번
   생긴다. 이력 적재는 Silver 의 `source_snapshot_id` 가 감당하고, canonical 은 현재 상태다.

## Consequences

- 계보가 완성된다: 필지 → 건물(805만) → 호(1,953만). `building_unit` 이 건물과 이어지려면
  후속으로 `building_id` 연결(호의 `building_mgm_bldrgst_pk` 경유)이 필요하며, 이는 이 ADR 의
  범위 밖에 두되 다음 결정으로 명시해 둔다.
- Silver 표 하나·계약·정규화 계획·투영 명령이 늘어난다. 전부 기존 네 벌의 전례를 따른다.
- 부속건축물의 `purpose_code` 등이 빈 값일 수 있다 — 필수 칸의 실측 채움율은 구현 단계에서
  재고 요약에 남긴다(빈 값 강제는 하지 않는다).
