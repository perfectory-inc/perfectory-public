# ADR 0280: 공매·법원경매·등록공장은 Foundation 이 PNU 로 묶는 공공 원천이다

- Status: Accepted
- Date: 2026-10-19
- Supersedes: [FP-ADR-0009](../../platforms/foundation-platform/docs/adr/0009-cross-service-lakehouse-registry-control-plane.md) 의 "Data owner" 중 `Onbid sale data, court auction data` 두 항목,
  [GZ-ADR-0039](../../products/gongzzang/docs/adr/0039-service-owned-lakehouse-registry-integration.md) 표의 `Onbid Bronze/Silver/Gold`·`Court auction Bronze/Silver/Gold` 두 행.
  두 ADR 의 나머지(레지스트리 제어면, 매물·매물 사진·시세의 Gongzzang 소유)는 그대로다.

## Context

사장 지시(2026-10-19): 공짱 필지 화면에 공매·경매·공장 탭을 두기 위해 세 원천을 Foundation 파이프라인(Bronze → Silver, PNU 키)으로 들인다.

그런데 FP-ADR-0009 와 GZ-ADR-0039 는 "Onbid sale data, court auction data" 를 Gongzzang 소유로 적었다. 그 뒤로 굳은 다른 결정과 맞지 않는다.

| 사실 | 근거 |
|---|---|
| 공공데이터 수집은 Foundation 소유, Gongzzang 은 계약 소비만 | 루트 `AGENTS.md` "영역 간 경계" |
| Gongzzang 은 `DATA_GO_KR_*` 를 가질 수 없다 | `products/gongzzang/docs/architecture/foundation-platform-boundary.v1.json` |
| 온비드는 data.go.kr OpenAPI 로만 제공된다(`apis.data.go.kr/B010003/*`) | data.go.kr 15157207·15157216·15157222·15157254 |
| 데이터 쪽은 값만 내고 화면 모양은 소비자가 정한다 | 루트 ADR-0193 계열(Gold 표 하나 = 데이터 종류 하나) |
| 두 ADR 이 Gongzzang 소유로 적은 뒤 Gongzzang 쪽 수집기·표 계약은 한 번도 생기지 않았다. 7월 일회성 적재 `gongzzang_silver.court_auction_property` 는 계약·작업 없이 남아 있다 | `platforms/foundation-platform/docs/runbooks/data-catalog.md` "lakehouse 에만 있고 계획에 없는 표" |

즉 소유 문장은 있는데 그 소유를 실행할 길(키·수집기·계약)은 Foundation 에만 있다. 같은 사실(누가 공공 원천을 모으나)이 두 곳에서 다르게 적혀 있는 상태다.

## Decision

1. **공매(온비드)·법원경매·등록공장현황은 Foundation 소유 공공 원천이다.** Bronze 는 Foundation R2 의 `bronze/source=<ADR-0014 slug>/` 에, Silver 는 Foundation 레이크하우스 `silver.*` 에 둔다. Gongzzang 은 Foundation 이 낸 계약(ODCS·by-PNU 레인)으로만 읽는다.
2. **원천 이름(ADR-0014).** `datagokr__onbid_real_estate_item`, `datagokr__onbid_announcement`, `datagokr__onbid_announcement_bid_result`, `datagokr__onbid_item_bid_result`, `datagokr__registered_factory`, `courtauctiongokr__court_auction`. 같은 공장 자료를 가리키던 자리표시 `factoryongokr__factory_registration`(수집 경로 없음)은 `datagokr__registered_factory` 로 대체한다: 실제로 받는 곳이 data.go.kr 이기 때문이다(FP-ADR-0032, 제공자는 받는 곳의 도메인).
3. **PNU 는 원천이 준 값을 먼저 쓴다.** 온비드 물건은 `ltnoPnu`(19자리), 법원경매 물건은 `pnuNoCtt` 를 그대로 키로 쓴다. 다시 만들거나 고치지 않는다. 형식이 틀린 값은 버리지 않고 Silver 에 `pnu = NULL` 과 원문을 함께 둔다.
4. **원천이 PNU 를 주지 않으면(등록공장) 주소로 찾되, 어느 단계가 찾았는지 남긴다.** 단계는 (가) 지번 주소 → 법정동 코드(`reference.legal_dong_code_snapshot`) + 산 + 본번·부번 → PNU, 그 PNU 가 `silver.parcel_boundaries` 에 있을 때만, (나) 도로명 주소 → `silver.building_register_titles.road_address_raw` 와 정규화 일치 → 그 행의 `pnu`, 후보가 PNU 하나로 모일 때만. 못 찾으면 `pnu = NULL` 과 이유. 도형 포함관계(좌표가 필지 안)로 정하지 않는다. 적재마다 단계별 건수와 일치율을 남긴다.
5. **Bronze 는 덧붙이기만 한다.** 응답 원문을 그대로, 요청 지문과 함께 쓴다(`If-None-Match: *`). Silver 는 원천 키 + 내용 해시가 바뀐 행만 새 판으로 쌓는다(덮어쓰지 않음).
6. 이미 있는 `gongzzang_silver.court_auction_property`(7월 일회성)는 이 결정으로 새로 만들지 않는다. 새 `silver.court_auction_*` 가 같은 값을 정기적으로 내면 그것을 읽던 dbt 모델(`stg_gongzzang__court_auction_observation`)을 새 표로 옮기고, 옛 표는 그 PR 에서 은퇴시킨다.

## Consequences

- 세 원천의 계약(레이크하우스 표·ODCS)·작업(`orchestration/jobs.v1.json`)·파이프라인 그래프 노드는 `platforms/foundation-platform` 에 생긴다.
- data.go.kr 개발계정 한도(하루 1,000 호출)가 공매 주기를 정한다. 전수 1회 약 62 호출이므로 3시간마다 전수 + 공고·결과는 매시간 증분으로 시작한다. 운영계정 상향(활용사례 등록, 무료)이 되면 주기를 올린다.
- 법원경매 수집 방식(정직한 수집 규칙)은 별도 결정이다: [ADR-0281](./0281-court-auction-is-collected-openly-politely-and-stops-when-refused.md).
- 등록공장의 일치율이 낮으면(도로명 주소가 건축물대장에 없는 공장) 주소 DB(juso.go.kr) 원천을 더하는 결정을 따로 한다. 이 ADR 은 이미 있는 표만 쓴다.

---

개정 각주(2026-10-19): Consequences 의 "개발계정 한도(하루 1,000 호출)" 는 틀렸다. 키는 운영계정이고, 네 API 모두 게이트웨이 응답 헤더가 `X-RateLimit-Limit: 100000`/일이다. 수집기 상한은 그 1/5 이다(`data-go-kr-onbid.contract.json`). 물건 전수가 3시간마다인 이유는 호출 한도가 아니라 Bronze 저장 비용이다.
