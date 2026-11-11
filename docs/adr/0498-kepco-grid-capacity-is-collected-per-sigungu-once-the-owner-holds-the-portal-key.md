# ADR 0498: 한전 분산전원연계정보는 시군구마다 받고, 사장이 전력데이터 개방포털 인증키를 받으면 첫 실호출로 칸을 정한다

- Status: Accepted
- Date: 2026-11-11
- Builds on: ADR-0400(수집 완전성 판정표 — `waiting_on_owner_application`), ADR-0497 §7(지역 단위 신호는 필지 문서에 넣지 않는다),
  ADR-0329 §3(`views.changed_codes`), ADR-0312(부분 처리), ADR-0406(한전 업종별 전력사용량은 받지 않는다 — 다른 API)

## Context

W63 조사(`platforms/foundation-platform/docs/catalog/parcel-utility-connectivity-sources.md` 15번)는 공장 입지에서 가장 자주 막히는
"전력 여유" 를 필지에 붙일 수 있는 유일한 공개 원천으로 한전 분산전원연계정보 API 를 골랐다. 사장은 미수집 원천 전부의 수집을
승인했다(2026-10-30).

**확인한 것(2026-11-11, 1차 출처).**

- 공공데이터포털 [15147381](https://www.data.go.kr/data/15147381/openapi.do): 이용허락범위 "제한 없음", 그러나 "전력데이터개방포털 API 로
  회원가입 및 인증키 발급 완료 후 사용 가능". 요청 주소·칸은 포털에 로그인해야 보이는 사용 매뉴얼에만 있다.
- 전력데이터 개방포털 [분산전원연계 정보](https://bigdata.kepco.co.kr/cmsmain.do?scode=S01&pcode=000493&pstate=L&redirect=Y):
  "시도코드, 시군구코드, 도로명을 기준으로 분산형전원 누적 연계용량 및 여유용량 정보" 를 준다. **조회 단위는 시군구(와 도로명)다.
  법정동이 아니다** — W63 의 "법정동·리 약 2만 번 조회" 추정은 틀렸다.
- 우리에게 그 포털의 인증키가 없다. 노트북 `.env.local` 에는 공공데이터포털 키(`DATA_GO_KR_SERVICE_KEY`)뿐이고, 그 키로는 이 API 를
  부를 수 없다. `config/runtime-secrets.contract.json` 에도 한전 키가 없다.

## Decision

1. **판정은 `waiting_on_owner_application` 이다**(ADR-0400). 엔드포인트 카탈로그에 `kepco-bigdata-dispersed-generation-connection`
   한 줄을 두고, 수집기는 키가 놓인 뒤의 PR 이 짓는다. 키 없이 칸 이름을 추측해 수집기를 짓지 않는다.
2. **사장이 할 일(한 번).** 전력데이터 개방포털(bigdata.kepco.co.kr) 회원가입 → 마이페이지에서 Open-API 인증키 발급(분산전원연계
   정보 활용 신청). 받은 키는 ai-server 의 수집 env 와 노트북 `.env.local` 에 `KEPCO_BIGDATA_API_KEY` 로 놓는다. 비용 0원.
3. **첫 실호출이 칸을 정한다.** 키가 놓이면 수집 PR 이 시군구 하나를 불러 요청 인자 이름(다른 한전 개방 API 의 `apiKey`·`metroCd`·
   `cityCd`·`returnType` 과 같은지)과 응답 칸(변전소·주변압기·배전선로 이름, 누적 연계 용량, 여유 용량)을 실측해 계약에 적는다.
4. **부분 처리 — 선언한 예외.** 원천은 바뀐 지역 목록을 주지 않는 실시간 값이라 "바뀐 것만 묻기" 가 불가능하다. 그래서 정한 주기마다
   시군구 전부(약 250, 2026-11 행정표준코드 기준 추정 — 수집 PR 이 센다)를 한 바퀴 묻고, **응답 해시가 바뀐 시군구만 Bronze·Silver 에
   덧붙인다**(append-only). 한 바퀴는 수백 번 호출이라 하루 한도 안에 든다(계산).
5. **법정동 코드 변경을 따라간다.** 조회 인자는 시도·시군구 코드다. 그 코드는 `reference.legal_dong_code_change` 의 지금 서 있는 짝
   (`views.changed_codes`, ADR-0329 §3)에서 매 바퀴 다시 뽑는다. 개편으로 사라진 시군구 코드는 그 바퀴에서 빠지고 새 코드가 들어온다.
   적재 때 한 번 풀고 멈추지 않는다.
6. **화면.** 값은 시군구·변전소 단위라 필지 문서(ADR-0497 `parcel-utility`)에 넣지 않는다(ADR-0497 §7). 지역 단위 서빙은 수집이 선 뒤
   따로 정한다.

## Consequences

- 지금은 아무것도 수집되지 않는다. 사장 한 단계(포털 인증키) 뒤에 수집 PR 하나, 지역 단위 서빙 결정 하나가 남는다.
- 실패가 막는 사고: 칸 이름을 추측해 지으면 첫 응답에서 칸이 비어 "여유 용량 없음" 을 거짓으로 쌓는다. 그래서 첫 실호출 전에는 짓지 않는다.
