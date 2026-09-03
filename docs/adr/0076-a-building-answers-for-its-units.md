# ADR 0076: 건물은 자기 호를 답한다

- Status: Accepted
- Date: 2026-09-03

## Context

3층 계보는 데이터로는 완성됐지만(ADR-0074: 19,402,750호 연결), API 에는 건물이라는 자원이
없다. 건물은 필지의 부속 목록(`/parcels/by-pnu/{pnu}/buildings`)으로만 나오고, "이 건물의
호 목록"은 어떤 경로도 답하지 못한다 — 오늘의 증명(11층 건물의 호 5,372개)은 SQL 로 했지,
서비스가 한 것이 아니다.

착수 조사에서 어긋남 하나가 드러났다. **마이그레이션 20260903000003 이 건물 다섯 칸의
NOT NULL 을 내렸는데, 읽기 모델(`catalog_domain::Building`·`BuildingResponse`)은 여전히
값을 약속한다** (`purpose_code: String`, `floor_area_m2: f64`, `built_year: i32`). NULL 을
실은 건물 수백만 동이 적재된 지금, 그 행이 낀 필지의 기존 건물 목록 경로는 읽는 순간
디코드 오류를 낸다. "결정은 코드가 읽기 전까지 구현이 아니다"의 반대편 — **쓰기만 결정을
따라가고 읽기가 낡은 채 남았다.**

설계 기준은 실제 생산 관례에서 가져온다 (우리 수준의 주장이 아니라 벤치마크다):

- **자원 지향 설계** (Google AIP-122/131): 계층의 각 실체는 자기 URI 를 가진 자원이고,
  하위 목록은 `{parent}/{id}/{children}` 이다. 자연키 별칭은 이 저장소가 이미
  `parcels/by-pnu/{pnu}` 로 낸 전례를 따른다.
- **목록은 태어날 때부터 페이지를 나눈다** (Google AIP-158, Stripe cursor pagination):
  무페이지 목록은 자원이 자라면 사고가 된다. 실측이 이미 말한다 — 한 건물이 호 5,372개,
  한 필지가 호 12,654개를 가진다.

## Decision

1. **읽기 모델이 스키마를 따라간다 (선행 수정).** `Building`·`BuildingResponse` 의
   `purpose_code`·`structure_code`·`floor_area_m2`·`stories`·`built_year` 는 `Option` 이
   된다 — 20260903000003 이 이미 내린 결정의 독자 쪽 이행이며, NULL 은 답이다(ADR-0074 §2).
   `below_ground_floors`(스키마 기본 0)와 옥탑 칸은 그대로다.
2. **건물이 자원이 된다.** `GET /catalog/v1/buildings/{id}` 와, 필지의 by-pnu 전례를 따라
   `GET /catalog/v1/buildings/by-register-pk/{register_pk}`. 응답에 `register_pk` 가 실린다 —
   대장 PK 는 사람이 건물을 부르는 이름이고, 재적재가 upsert 하는 자연키다(ADR-0073 §4).
3. **`GET /catalog/v1/buildings/{id}/units` 는 태어날 때부터 keyset 페이지네이션이다.**
   정렬은 `(dong_name, ho_name, id)` — 사람이 호를 세는 순서이고, `id` 가 동률을 끊어
   커서가 안정된다. 응답은 `{ items, next_cursor }` 봉투이고, 커서는 그 3튜플의 불투명
   base64 인코딩이다. `limit` 는 기본 200·최대 1000 이며, 경계 밖은 400 으로 거부한다
   (complex 검색의 페이지 상한 전례).
4. **호 응답에 `building_id`(nullable)가 실린다.** ADR-0074 가 채운 연결의 노출이며,
   NULL 인 호도 실린다 — 숨기면 "연결 안 된 호는 없다"는 거짓 주장이 된다.
5. **기존 무페이지 목록 둘은 이 결정이 고치지 않는다.** `/parcels/by-pnu/{pnu}/units` 는
   실측 최대 12,654행을 봉투 없이 낸다. 응답 모양 변경은 호환성 파괴이므로 별도 결정으로
   남기고, 새 경로가 그 모양을 **복사하지 않는 것**이 이 ADR 의 몫이다.

## Consequences

- 처음으로 "이 건물에 무슨 호가 있는가"를 서비스가 답한다. 증명은 배포 후 실물 건물
  (호 5,372개)에 대한 curl 로 한다 — 페이지를 끝까지 걸어 합이 5,372 인지.
- 읽기 모델 Option 화는 API 응답의 JSON 에서 해당 칸을 nullable 로 만든다. 이 칸들을
  이미 읽는 소비자는 없다(경로 자체가 NULL 행에서 500 이었으므로).
- OpenAPI 는 코드의 utoipa 주석에서 생성되는 기존 SSOT 를 그대로 탄다. 새 계약 파일은
  없다 — 응답 모양의 정본은 `foundation-contracts` 한 곳이다.
