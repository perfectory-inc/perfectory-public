# ADR 0550: 중개사 가입 확인은 바뀐 사무소만 싣는 Postgres 명부에 묻고, 없으면 VWorld 에 실시간으로 묻는다

- Status: Accepted
- Date: 2026-11-12

## Context

공짱 회원 가입에서 중개사는 "이 사람이 등록된 중개사인가"를 확인받아야 한다(사장 2026-11-11, 루트
ADR-0511 §4: 중개사가 우선 사용자다). 사람은 **이름 + 자격증번호** 또는 **이름 + 중개사무소 등록번호**로
가린다. 그 명부는 이미 Silver 에 있다.

- `silver.real_estate_broker_office`(#1323): VWorld AL_D171 전국 파일, 사무소마다 한 행, 키 (시군구, 등록번호).
- `silver.real_estate_broker_member`(#1325): AL_D172, 사무소에 등록된 사람마다 한 행(이름·종별·자격증번호·직위),
  키는 사무소 — 사람은 제공자 키가 없어 사무소의 명단 전체가 한 판이다. 두 표 다 매일 오는 전국 파일에서
  **바뀐 사무소만** 덧붙이고, 사라진 사무소는 `row_state = 'removed'` 표지를 덧붙인다(키마다 최신이 현재,
  루트 ADR-0286 `LatestPerKey`).

실측(W67c, 2026-11-11, R2 읽기 키 + VWorld 실호출):

| 측정 | 값 |
|---|---|
| D171 사무소 2026-11-09 | 106,429 행 |
| D172 사람 2026-11-09 | 179,124 행 (공인중개사 122,683 · 보조원 54,180 · 중개인 1,546 · 법인 704) |
| 파일 주기 | 매일 새 전국 파일 (Bronze 장부 10-28..11-09 하루 하나) |
| 실시간 API `getEBBrokerInfo` 대 11-09 파일 | 강남 8,138 대 8,136 (사람 13/12 차이 = 0.16%) |

즉 파일은 낡지 않았고, 하루 이틀의 지연(그날 등록한 사람)만 실시간 API 가 메운다. `getEBBrokerInfo` 는
`ldCode`(시군구)·`brkrNm`(중개업자명)·`jurirno`(등록번호)·`bsnmCmpnm`(상호)로 거르고(전부 선택),
자격증번호(`crqfcNo`)는 응답에만 있다(VWorld API 문서, 2026-11-12 확인). 한 쪽 최대 1,000 행.

가입 확인은 한 사람에 대한 점 조회다. 두 가지는 **할 수 없다**.

- 서빙 정본인 R2 미리구운 객체(루트 ADR-0096·0108)는 공개 엣지다. 사람 이름·자격증번호를 키로 한 공개
  객체는 명부를 통째로 내놓는 것과 같다.
- 가입마다 Trino 로 Silver 를 읽는 것은 사용자 요청 경로에 분 단위 분석 엔진을 놓는 일이다.

ADR-0108 은 Postgres 를 "소형 운영 데이터"로 남겼다. 이 명부(두 표 합계 약 28만 행, 수십 MB)가 거기에
맞는다. 대형 투영을 되살리는 것이 아니라, 비공개 점 조회의 색인이다.

### 실사례

- **Stripe Identity·Plaid Identity Verification**: 확인 API 는 "일치/불일치"와 최소한의 속성만 답하고
  대상 목록을 내주지 않는다. 호출자는 서비스 자격으로 부른다.
- **Kafka Connect JDBC sink·Debezium 의 upsert 모드, Netflix 의 Iceberg → 온라인 저장소 증분 적재**:
  레이크하우스는 정본, 온라인 저장소는 바뀐 키만 받는 파생 색인. 소비자는 자기 워터마크를 들고 다닌다.
- **Kimball SCD Type 2**: 온라인 표도 덮어쓰지 않고, 옛 행은 "언제 물러났는지"를 받는다.

## Decision

1. **명부는 Postgres 두 표와 적재 원장 하나다**(foundation DB, `catalog` 스키마).
   - `catalog.real_estate_broker_office` — 사무소 행(시군구·등록번호·상호·상태 코드/이름·데이터기준일자).
   - `catalog.real_estate_broker_member` — 사람 행(시군구·등록번호·이름·종별·자격증번호·직위·데이터기준일자).
   - `catalog.real_estate_broker_lookup_load` — 적재 한 번에 한 행: 읽은 두 Silver 표의 Iceberg 스냅숏,
     그 적재가 본 `ingested_at_utc` 최대값(다음 적재의 워터마크), 바뀐 사무소 수, 넣은 행·물린 행 수.

   두 명부 표는 **덧붙이기만 한다**(사장 원칙: 모든 데이터는 쌓는다). 행은 그것을 넣은 적재
   (`loaded_by_load_id`)를 들고, 그 사무소가 다음에 바뀐 적재가 한 번 `retired_by_load_id` 를 채운다.
   현재 행 = `retired_by_load_id IS NULL`. 이 둘 말고 행을 고치는 쓰기는 없다. Postgres 는 정본이 아니다:
   Silver 에서 언제든 다시 만들 수 있다(ADR-0108).

2. **적재는 바뀐 사무소만 읽는다**(사장 원칙: 모든 처리는 부분). `load-real-estate-broker-lookup`
   (foundation-outbox-publisher)이 Trino CLI 로 읽는다(`unit_official_price_projection_load` 와 같은 길,
   Trino CLI 실행부는 두 적재가 공유한다).
   - 두 Silver 표의 현재 스냅숏을 고정한다(`FOR VERSION AS OF`).
   - 바뀐 사무소 = 고정한 스냅숏에서 `ingested_at_utc >=` 원장의 마지막 워터마크인 행의 키. Silver 는
     덧붙이기만 하고 적재마다 새 `ingested_at_utc` 를 찍으므로, 이것이 지난 적재 뒤 덧붙은 행이다.
     같은 실행이 여러 커밋으로 나눠 쓸 수 있어 `>` 가 아니라 `>=` 로 읽는다(마지막 묶음을 한 번 더 읽어도
     같은 결과다). Iceberg 파일 통계가 옛 파일을 건너뛴다.
   - 그 사무소들의 현재 행 = 계약의 `LatestPerKey` 규칙(키·`newest_first`·제거 열·`keep_ties`)을 **Rust 계약
     상수에서 SQL 로 렌더해** 읽는다. 규칙을 손으로 다시 적지 않는다(루트 ADR-0267: 현재 행은 한 곳에서).
   - 한 트랜잭션에서: 그 사무소들의 현재 Postgres 행에 `retired_by_load_id` 를 채우고, 새 현재 행을 넣고,
     원장 행을 쓴다. 실패하면 아무것도 남지 않고 다음 실행이 같은 워터마크에서 다시 읽는다.
   - **첫 적재는 전부다**(선언된 예외): 원장이 비어 있으면 워터마크가 없어 전 사무소가 바뀐 것이다.
     계산 비용: 사무소 106,429 + 사람 179,124 행, Trino 로 수십 MB, 1분 안팎(같은 크기의 Silver 레인
     적재 실측 161–251 초보다 작다).
   - 바뀐 것이 없으면 쓰지 않고 `unchanged` 를 낸다.

3. **작업은 `real_estate_broker_lookup_load` 하나다**(`definitions/real-estate-broker-lookup.definition.json`).
   두 Silver 레인이 바꾸면 시작하고(`started_by: inputs`), 매일 한 번 대체 실행한다(루트 ADR-0171).
   그래프 간선은 `silver-real-estate-broker-office`·`silver-real-estate-broker-member` →
   `serving-real-estate-broker-lookup`.

4. **조회는 foundation-api 의 `POST /internal/real-estate-brokers/verify` 하나다.**
   - 서비스 주체만 부른다: 정책 `foundation.real_estate_broker:verify`(서비스), Identity 작업 정책은 이것을
     `gongzzang-api` 에만 준다(루트 ADR-0214 계약 파일).
   - 본문은 `{name, license_no}` 또는 `{name, registration_no}` — 정확히 하나. 이름은 앞뒤 공백을 떼고 안쪽
     공백을 하나로, 번호는 공백·하이픈 차이를 지우고 비교한다.
   - 답은 `result`(`match`·`no_match`)와 맞은 한 사람의 종별·직위, 그 사무소의 등록번호·상호·상태,
     데이터기준일자, `checked`(`registry`, 실시간까지 갔으면 `vworld_live`)뿐이다. **사람 목록을 내주지
     않고**, 이름만으로는 묻지 못한다. 경로에 개인정보를 싣지 않으려고 GET 이 아니라 POST 이고, 로그에는
     결과만 남는다.
   - 같은 이름·번호가 여러 사무소에 있으면(소속 이동 중 하루 겹침) 하나라도 맞으면 `match` 이고 가장 나중에
     적재된 사무소가 답한다. 사무소 상태(영업·휴업 …)는 답에 실어 보내고, 가입을 받을지는 제품이 정한다.

5. **명부에 없으면 VWorld 에 실시간으로 묻는다.** `getEBBrokerInfo` 를 `brkrNm`(+ 등록번호로 물으면
   `jurirno`)로 부르고 응답에서 자격증번호·등록번호를 맞춘다. 최대 3 쪽(3,000 행), 요청 전체 3 초. VWorld 가
   답하지 않거나 키가 없으면 `checked` 에 `vworld_live` 가 없는 `no_match` 다 — 확인을 못 한 것과 아니라고
   확인한 것을 호출자가 가른다. 실시간 결과는 Postgres 에 쓰지 않는다: 다음 날 파일이 Silver 를 거쳐
   명부를 고친다(정본은 하나).

## Consequences

- 공짱 가입 화면(제품 쪽)이 이 경로를 부른다. foundation-api 에는 `FOUNDATION_PLATFORM_VWORLD_API_KEY`·
  `_DOMAIN` 이 필요하다(없으면 5 의 실시간 확인만 꺼진다).
- 두 Silver 표의 `declaredStatus` 가 이 조회로 서빙된다. 대외 by-PNU·화면 개수에는 더하지 않는다(내부 전용).
- 적재 비용(계산): 첫 적재 1분 안팎, 이후 하루 바뀐 사무소 수백 개, Postgres 증가 하루 수백 행. 돈은 들지
  않는다(기존 Trino·Postgres).
- 위험: 같은 이름·자격증번호가 서로 다른 사람일 가능성은 자격증번호가 사람마다 달라 무시한다. 등록번호로
  물으면 그 사무소의 같은 이름이 둘일 수 있으나 둘 다 그 사무소의 등록 인원이라 `match` 가 맞다.
