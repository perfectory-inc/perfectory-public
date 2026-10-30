# ADR 0355: 건축HUB·주소 전자지도·기타 일괄 파일 82개 원천을 하나씩 판정하고, 연결 순서를 정한다

- Status: Accepted
- Date: 2026-10-30
- Builds on: ADR-0077(매일 보고 반영), ADR-0168·ADR-0172(일일 원천 수집), ADR-0169(Silver 레인은 장부에서 판을 고른다),
  ADR-0280(필지 원천의 수집·적재 틀), ADR-0288(place-identity), ADR-0311·ADR-0318(바뀐 키만 쌓는 Silver),
  ADR-0312·ADR-0344(Gold 는 바뀐 PNU 만), ADR-0314(부분 처리가 기본)

## Context

사장이 2026-10-30 "모든 데이터 수집을 승인" 했다. 데이터 지도(`docs/data-pipeline-map.md`)의 세 원천 가족이
이 ADR 의 범위다. 건축HUB 파일 59개, 주소정보 전자지도 11개, 기타 공공 일괄 파일 12개다.
지도는 이들을 "일부 연결"·"승인 후 수집 가능" 이라고만 말한다. 원장을 직접 셌다.

**측정 (2026-10-30, 운영 `catalog.bronze_object`·`catalog.source_catalog` 읽기 전용).**

- **건축HUB 57개는 이미 매달 수집된다.** 일일 원천 수집(`source_sweep`, ADR-0077)이 hub.go.kr 목록을 다시 보고
  없는 파일만 받는다. 57개 원천 모두 2026-06 ~ 2026-10 다섯 판을 갖는다. 합계 **78.16 GB**, 최신 판 **16.04 GB**.
  "나머지는 수집 단계에 남는다" 는 수집이 멈췄다는 뜻이 아니다. 수집은 돌고, Silver 가 없다.
- **건축HUB 연도별 에너지 2개**(`task_code` 0501·0502)는 제공자 목록에 파일이 없다(`provider_inventory_missing`).
  `source_catalog` 행도 Bronze 객체도 0 이다. 월별 전기·가스 파일(0801·0802)은 받고 있다. 연도 값은 월 값의 합이다.
- **주소정보 전자지도 11개는 0 객체이고 수집기도 없다.** `source_catalog` 행이 없다. 저장소 전체에 `jusogokr__`
  를 읽는 코드가 없다. "승인 후 수집 가능" 은 사실이 아니었다. 제공처(행정안전부 주소기반산업지원서비스,
  business.juso.go.kr)는 전자지도를 **이용 신청과 승인** 뒤에만 내려 준다. 웹 신청이나 방문 접수로 신청하고,
  처리 기간은 10일이다. 우리에게는 승인된 계정이 없다. `.env.local` 에 juso 키 이름이 없다.
- **기타 공공 일괄 파일 12개도 0 객체이고 수집기도 없다.** 카탈로그의 제공자가 `mixed_public_source` 다.
  어느 기관의 어느 파일인지 처음부터 적힌 적이 없다. 세대수·동 인구만 `mois.go.kr` 이다.
- **PNU 연결은 실물로 확인했다.** 최신 Bronze ZIP 의 앞 몇 줄을 R2 범위 읽기로 열었다(읽기 전용 키). 표제부와 같은 칸이
  있다: 시군구코드·법정동코드·대지구분·번·지. 확인한 파일은 건축인허가 기본개요(`mart_kcy_01`, 41칸), 철거멸실(`mart_kcy_07`, 34칸),
  총괄표제부(`mart_djy_02`, 64칸), 부속지번(`mart_djy_05`, 33칸), 월별 전기(`eais_elcty_YYYYMM`, 17칸)다.
  부속지번은 행마다 두 번째 지번(칸 23–27)도 싣는다. 이것이 건물↔필지 다리다.
  월별 에너지에는 관리번호가 없다. 열쇠는 지번과 사용 월이다.

### 실사례

- **건축 허가가 공사 신호다.** 미국의 Shovels·BuildZoom 은 지자체 건축허가 원장을 모아 "이 필지에 지금 무엇이
  지어지는가" 를 판다. CoStar·Reonomy 의 건물 이력도 허가 → 착공 → 사용승인의 사건열이다. 우리 건축인허가 기본개요가
  그 사건열의 원천이다(허가일·착공예정일·실착공일·사용승인일).
- **원천마다 판정표를 남긴다.** Airbnb Dataportal·LinkedIn DataHub 의 "dataset status" 처럼, 받는 것·안 받는 것과
  그 이유를 원천마다 적는다. 그러면 "왜 이것은 화면에 없나" 에 묻지 않고도 답할 수 있다.

## Decision

1. **판정은 다섯 가지다.** 원천마다 하나를 고른다. 가치는 산업용 부동산(공장·창고·지식산업센터) 매수인·임차인에게 주는 값이다.
   - **이미 연결**: Silver 레인이 이미 읽는다.
   - **지금 연결**: Silver 계약과 바뀐 키 적재를 만든다. 패널에 쓸 값이면 Gold/by-PNU 까지 잇는다. 순위대로 만든다.
   - **나중 연결**: 값은 있으나 다른 레인이 먼저 서야 한다(앞 순위, 또는 거리 파생 같은 새 틀).
   - **Bronze 보관만**: 수집은 계속하되(원본 보존, append-only) Silver 를 만들지 않는다. 주거 전용·중복·명부류다.
   - **받을 수 없음**: 제공자가 파일을 내지 않는다.
2. **지금 연결할 원천은 이 순서로 만든다.** 한 레인씩 만든다. 모두 같은 HUB 스트리밍 엔진(`hub_register_silver_export`)과
   Silver 갱신(ADR-0169), 키 묶음 비교(ADR-0318)를 쓴다. 새 엔진은 만들지 않는다.
   1. 건축인허가 기본개요 → `silver.building_permit_basis_outline` (ADR-0356, 이 묶음의 첫 레인).
   2. 총괄표제부 → `silver.building_register_master`.
   3. 부속지번 → `silver.building_register_sub_parcel`. place-identity(ADR-0288)의 건물↔필지 근거다.
   4. 월별 전기·가스 → 하나의 에너지 레인. 열쇠가 관리번호가 아니라 (PNU, 사용 월)이므로 엔진의 열쇠 칸을 넓힌다.
   5. 철거멸실 → 건물 수명 사건열의 끝.
   주소정보 전자지도의 **건물 도형**도 지금 연결이다. 다만 아래 3의 신청이 끝나야 받을 수 있다.
3. **주소정보 전자지도는 사장의 이용 신청이 먼저다.** 우리가 신청 절차를 대신할 수 없다. 승인이 오면 첫 판을
   수동으로 받아 크기·형식·칸을 실측하고, 그 PR 이 수집기를 만든다. 받는 법을 실측 전에 추측해 짓지 않는다.
   그 전까지 그래프 상태는 `planned` 다. 건물 도형의 열쇠는 건물관리번호(25자리)다. 앞 19자리가 법정동코드 10, 산 1, 본번 4, 부번 4 이므로
   PNU 로 바꿀 수 있다. 산 칸은 주소 체계(0/1)와 PNU 체계(1/2)가 다르다. 이 변환은 HUB 대지구분과 같은 규칙(FP-ADR-0023)을 따른다.
4. **기타 공공 일괄 파일은 원천마다 실제 제공자를 먼저 적는다.** `mixed_public_source` 로는 수집기를 만들 수 없다.
   각 원천을 연결하는 PR 이 그 카탈로그 행의 제공자·파일 번호·수집 경로를 실물로 바꾼다. 수집기는 새로 쓰지 않는다.
   data.go.kr 파일 데이터 수집기(ADR-0280 §4, `collect-data-go-kr-registered-factory`)를 계약에 적힌 여러 파일로 넓힌다.
   필지와는 두 가지로 연결한다. 행정동 코드(인구·연계표)이거나, 점 위치에서 필지까지의 거리(IC·항만·공항·역)다.
   거리는 도형에서 파생한 값일 뿐이다. 신원(PNU)을 도형으로 정하지 않는다는 규칙(ADR-0280 §4)과 부딪히지 않는다.
   거리 파생 Gold 는 새 틀이므로 따로 ADR 을 쓴다.
5. **새로 켤 수집은 없다.** 건축HUB 57개는 이미 켜져 있다(`source_sweep`, enabled 2026-10-01). 연도별 에너지 2개는
   제공자가 낼 때까지 `provider_inventory_missing` 으로 둔다. 목록에 새로 생기면 일일 수집이 받는다.
6. **이 판정을 그래프의 원천 가족 설명에 반영한다.** 지도는 그래프에서 다시 생성한다(`render-pipeline-map.py`).
   손으로 고치지 않는다. 주소정보 전자지도와 기타 일괄 파일의 상태는 `collection_available` 에서 `planned` 로 바꾼다.
   수집기가 없는데 "승인 후 수집 가능" 이라고 말하던 것이 이번에 찾은 거짓 신호다.

## 원천별 판정

가치는 H 높음 · M 중간 · L 낮음이다. 보유는 운영 장부(2026-10-30)의 바이트이고, GB 는 10^9 바이트다.
에너지 가스·정기점검 이력·건축인허가 이외 계열의 PNU 칸은 같은 계열 파일에서 추정했다. 연결하는 PR 이 실측한다.

#### building_hub_bulk

| source_slug | 이름 | 보유(5판, GB) | 최신판 | PNU | 가치 | 판정 | 이유 |
|---|---|---:|---:|---|---|---|---|
| `hubgokr__building_register_basis_outline` | 건축물대장 기본 정보 파일 | 3.59 | 0.72 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 호 레인(building-register-units)의 basis 입력으로 읽힌다 |
| `hubgokr__building_register_main` | 건축물대장 표제부 파일 | 3.34 | 0.65 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 Silver 레인이 있다 |
| `hubgokr__building_register_master` | 건축물대장 총괄표제부 파일 | 0.19 | 0.04 | 예(시군구·법정동·대지구분·번·지 칸) | H | 지금 연결(2) | 한 대지의 여러 동 합계(대지·연면적·주차) — 공장 단지형 대지의 규모 |
| `hubgokr__building_register_apartment_price` | 건축물대장 공동주택 가격 파일 | 27.81 | 5.89 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 Silver 레인이 있다 |
| `hubgokr__building_register_exclusive_unit` | 건축물대장 전유부 파일 | 4.81 | 0.98 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 Silver 레인이 있다 |
| `hubgokr__building_register_floor_overview` | 건축물대장 층별개요 파일 | 3.35 | 0.61 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 Silver 레인이 있다 |
| `hubgokr__building_register_sewage_facility` | 건축물대장 오수정화시설 파일 | 1.38 | 0.34 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 오수정화시설 |
| `hubgokr__building_register_sub_parcel` | 건축물대장 부속지번 파일 | 0.59 | 0.12 | 예(시군구·법정동·대지구분·번·지 칸) | H | 지금 연결(3) | 건물이 걸친 다른 필지 — 건물↔필지 연결(place-identity, ADR-0288) |
| `hubgokr__building_register_district_zone` | 건축물대장 지역지구구역 파일 | 0.98 | 0.20 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 대장 지역지구 — 토지이용계획(VWorld)이 정본 |
| `hubgokr__building_register_exclusive_common_area` | 건축물대장 전유공용면적 파일 | 16.34 | 3.30 | 예(시군구·법정동·대지구분·번·지 칸) | H | 이미 연결 | 이미 Silver 레인이 있다 |
| `hubgokr__building_energy_monthly_gas` | 건물 가스 사용량 파일 | 0.04 | 5.9M | 예(시군구·법정동·대지구분·번·지 칸) | M | 지금 연결(4) | 월별 가스 사용량 — 전기와 같은 레인 |
| `hubgokr__building_energy_monthly_electricity` | 건물 전기 사용량 파일 | 0.13 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | H | 지금 연결(4) | 월별 전기 사용량 — 가동·공실 신호. 관리번호가 없어 PNU+월 키 레인이 필요 |
| `hubgokr__building_permit_basis_outline` | 기본개요 | 2.19 | 0.44 | 예(시군구·법정동·대지구분·번·지 칸) | H | 지금 연결(1순위) | 허가일·착공(예정·실제)·사용승인일·건축구분(신축/증축) — 필지에 지금 짓는 중/허가만 난 건물. ADR-0356 레인 |
| `hubgokr__building_permit_building_outline` | 동별개요 | 1.58 | 0.32 | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 허가 건의 동별 구조·용도 — 기본개요 다음 |
| `hubgokr__building_permit_floor_overview` | 층별개요 | 2.15 | 0.43 | 예(시군구·법정동·대지구분·번·지 칸) | L | 나중 연결 | 허가 건의 층별 용도 |
| `hubgokr__building_permit_unit_outline` | 호별개요 | 0.28 | 0.06 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 허가 건의 호별 — 공장·창고에 드묾 |
| `hubgokr__building_permit_major_repair` | 대수선 | 0.01 | 2.9M | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 대수선 — 건물 수명 사건 |
| `hubgokr__building_permit_structure_ledger` | 공작물관리대장 | 0.07 | 0.01 | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 공작물(옹벽·저장조·굴뚝 등) — 공장 부대시설 |
| `hubgokr__building_permit_demolition_loss` | 철거멸실관리대장 | 0.08 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | M | 지금 연결(5) | 철거·멸실 신고 — 건물 수명의 끝 |
| `hubgokr__building_permit_temporary_building` | 가설건축물 | 0.27 | 0.06 | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 가설건축물 — 공장 부지의 임시 창고·사무실 |
| `hubgokr__building_permit_sewage_facility` | 오수정화시설 | 0.37 | 0.07 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 오수정화시설 |
| `hubgokr__building_permit_parking` | 주차장 | 0.30 | 0.06 | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 주차장 허가 — 물류 차량 수용 |
| `hubgokr__building_permit_annex_parking` | 부설주차장 | 5.4M | 1.1M | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 부설주차장 대수 |
| `hubgokr__building_permit_exclusive_common_area` | 전유공용면적 | 0.59 | 0.12 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 허가 건의 전유·공용면적 — 대장 레인이 이미 있음 |
| `hubgokr__building_permit_unit_exclusive_common_area` | 호별전유공용면적 | 1.50 | 0.30 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 허가 건의 호별 면적 — 같음 |
| `hubgokr__building_permit_district_zone` | 지역지구구역 | 1.12 | 0.23 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 허가 건의 지역지구 — 토지이용계획(VWorld)이 정본 |
| `hubgokr__building_permit_road_ledger` | 도로대장 | 0.04 | 7.4M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 도로대장(건축선 지정 도로) |
| `hubgokr__building_permit_site_location` | 대지위치 | 1.15 | 0.23 | 예(시군구·법정동·대지구분·번·지 칸) | M | 나중 연결 | 허가 건이 걸친 대지 목록 — 부속지번과 같은 연결 근거 |
| `hubgokr__building_permit_housing_type` | 주택유형 | 0.02 | 3.2M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택유형 — 주거 |
| `hubgokr__housing_permit_basis_outline` | 기본개요 | 0.04 | 7.6M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_building_outline` | 동별개요 | 0.07 | 0.01 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_floor_overview` | 층별개요 | 0.21 | 0.04 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_unit_outline` | 호별개요 | 0.37 | 0.07 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_ancillary_facility` | 부대시설 | 0.04 | 8.1M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_sewage_facility` | 오수정화시설 | 3.3M | 0.7M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_parking` | 주차장 | 5.8M | 1.2M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_annex_parking` | 부설주차장 | 0.4M | 0.1M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_exclusive_common_area` | 전유공용면적 | 0.08 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_act_unit_exclusive_common_area` | 행위호전유공용면적 | 0.02 | 4.7M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_act_outline` | 행위개요 | 0.08 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_managed_communal_type_outline` | 관리공동형별개요 | 0.02 | 4.2M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_managed_communal_ancillary_welfare_facility` | 관리공동부대복리시설 | 1.1M | 0.2M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_district_zone` | 지역지구구역 | 3.8M | 0.8M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_site_location` | 대지위치 | 0.05 | 0.01 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__housing_permit_welfare_sale_facility` | 복리분양시설 | 0.02 | 3.2M | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 주택 인허가 — 주거 전용 |
| `hubgokr__building_register_closed_basis_outline` | 기본개요 | 0.43 | 0.09 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_master` | 총괄표제부 | 0.06 | 0.01 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_main` | 표제부 | 0.76 | 0.15 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_floor_overview` | 층별개요 | 0.61 | 0.12 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_sub_parcel` | 부속지번 | 0.11 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_exclusive_common_area` | 전유공용면적 | 0.29 | 0.06 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_sewage_facility` | 오수정화시설 | 0.30 | 0.06 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_exclusive_unit` | 전유부 | 0.09 | 0.02 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_apartment_price` | 공동주택가격 | 0.06 | 0.01 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_register_closed_district_zone` | 지역지구구역 | 0.18 | 0.04 | 예(시군구·법정동·대지구분·번·지 칸) | L | Bronze 보관만 | 폐쇄대장 — 과거 건물 이력. 공장 매수인에겐 드묾; 수명 레인이 서면 다시 본다 |
| `hubgokr__building_energy_yearly_electricity` | 지번별에너지(전기,연도별) | 0 | — | — | L | 받을 수 없음 | 제공자 목록에 없음(task 0501). 월별 파일의 합으로 대신한다 |
| `hubgokr__building_energy_yearly_gas` | 지번별에너지(가스,연도별) | 0 | — | — | L | 받을 수 없음 | 제공자 목록에 없음(task 0502). 월별 파일의 합으로 대신한다 |
| `hubgokr__building_maintenance_inspection_agency` | 점검기관 | 0.4M | 0.1M | 아니오(기관 명부) | L | Bronze 보관만 | 점검기관 명부 — 필지와 무관 |
| `hubgokr__building_maintenance_regular_inspection_history` | 정기점검이력 | 8.0M | 1.6M | 미측정 | M | 나중 연결 | 정기점검 이력 — 건물 상태 신호 |

#### juso_electronic_map_bulk

| source_slug | 이름 | 보유 | PNU/장소 연결 | 가치 | 판정 | 이유 |
|---|---|---:|---|---|---|---|
| `jusogokr__building` | JUSO 건물 도형 | 0 | 예(건물관리번호) | H | 지금 연결 | 건물 도형 — 지도 위 건물 윤곽. 건물관리번호 앞 19자리(법정동10+산1+본번4+부번4)로 PNU 연결 |
| `jusogokr__base_interval` | JUSO 기초구간 | 0 | 코드·도형 | L | Bronze 보관만 | 기초구간 |
| `jusogokr__basic_area` | JUSO 국가기초구역 | 0 | 코드·도형 | L | Bronze 보관만 | 국가기초구역(우편번호 구역) |
| `jusogokr__building_group` | JUSO 건물군 | 0 | 코드·도형 | L | Bronze 보관만 | 건물군 |
| `jusogokr__building_entrance` | JUSO 건물 출입구 | 0 | 코드·도형 | L | Bronze 보관만 | 건물 출입구 점 |
| `jusogokr__legal_emd` | JUSO 법정구역 읍면동 | 0 | 코드·도형 | L | Bronze 보관만 | 같음 |
| `jusogokr__legal_ri` | JUSO 법정구역 리 | 0 | 코드·도형 | L | Bronze 보관만 | 리 경계 — 지금 쓰는 곳 없음 |
| `jusogokr__legal_sido` | JUSO 법정구역 시도 | 0 | 코드·도형 | L | Bronze 보관만 | 법정구역 — VWorld 경계·code.go.kr 법정동이 이미 정본 |
| `jusogokr__legal_sigungu` | JUSO 법정구역 시군구 | 0 | 코드·도형 | L | Bronze 보관만 | 같음 |
| `jusogokr__road_section` | JUSO 도로구간 | 0 | 도형 겹침만 | L | 나중 연결 | 도로구간 선 — 도로명 주소의 뼈대 |
| `jusogokr__road_width` | JUSO 실폭도로 | 0 | 도형 겹침만 | M | 나중 연결 | 실폭도로 면 — 필지가 닿은 도로 폭(대형 차량 진입) |

#### other_bulk

| source_slug | 이름 | 보유 | PNU/장소 연결 | 가치 | 판정 | 이유 |
|---|---|---:|---|---|---|---|
| `public-bulk-highway-access-point` | 고속도로 접근점 | 0 | 점 위치 → 거리 파생 | H | 나중 연결 | 고속도로 IC — 물류 접근성(필지→가장 가까운 IC 거리). 제공기관 후보: 한국도로공사 |
| `public-bulk-school-location` | 학교 위치 | 0 | 점 위치 → 거리 파생 | L | Bronze 보관만 | 초중고 위치 — 주거 지표 |
| `public-bulk-university` | 대학교 | 0 | 점 위치 → 거리 파생 | L | Bronze 보관만 | 대학 — 연구 인력, 산업용 매수인엔 약함 |
| `moisgokr__household_count` | 세대수 | 0 | 코드(행정동) | L | Bronze 보관만 | 행정동 세대수 — 인구와 같은 파일의 다른 칸 |
| `moisgokr__dong_population` | 동 인구 | 0 | 코드(행정동) | M | 나중 연결 | 행정동 인구 — 인력 풀. 행정안전부 주민등록 인구통계 |
| `public-bulk-railway-station` | 철도역 | 0 | 점 위치 → 거리 파생 | M | 나중 연결 | 철도역 — 통근·화물. 후보: 국가철도공단 |
| `public-bulk-railway-station-line` | 철도역 노선 매핑 | 0 | 점 위치 → 거리 파생 | L | Bronze 보관만 | 역↔노선 대응 — 철도역 레인의 보조 |
| `public-bulk-subway-station` | 도시철도역 | 0 | 점 위치 → 거리 파생 | M | 나중 연결 | 도시철도역 — 인력 통근. 후보: 전국도시철도역사정보표준데이터 |
| `public-bulk-port` | 항만 | 0 | 점 위치 → 거리 파생 | M | 나중 연결 | 항만 — 수출입 물류 거리. 후보: 해양수산부 |
| `public-bulk-airport` | 공항 | 0 | 점 위치 → 거리 파생 | M | 나중 연결 | 공항 — 항공 화물 거리. 후보: 한국공항공사·인천국제공항공사 |
| `public-bulk-commercial-district` | 상권 | 0 | 점 위치 → 거리 파생 | L | Bronze 보관만 | 상권 — 상가 지표 |
| `public-bulk-adm-dong-code-linkage` | 행정동 법정동 연계 | 0 | 코드(행정동) | M | 나중 연결 | 행정동↔법정동 연계 — 행정동 단위 통계를 필지에 붙이는 다리 |

## Consequences

- **비용(계산).** 건축HUB Bronze 는 이미 78.16 GB 이고 매달 약 16 GB 씩 는다. R2 저장 단가 $0.015/GB·월에
  1,450원/$ 로 계산하면 지금 약 1,700원/월, 한 해 뒤(약 270 GB) 약 5,900원/월이다. 내려받기와 R2 쓰기는 무료 등급 안이다.
  Silver 레인 하나(예: 기본개요, 판당 원문 1.72 GB)는 Parquet 로 수백 MB 이다. 바뀐 키만 쌓으므로 판마다 더하는 양은
  그보다 훨씬 작다. 주소정보 전자지도 전국 건물 도형은 아직 실측하지 못했다. 첫 판을 받는 PR 이 크기와 비용을 적는다.
  월 5만 원에 가까운 원천은 없다.
- **사장 할 일.** 주소기반산업지원서비스(business.juso.go.kr)에서 "주소 전자지도" 이용을 신청한다.
  신청 항목은 건물 도형·실폭도로·도로구간이다. 나머지 8개는 보관용으로 함께 신청한다. 이용 목적은
  "산업용 부동산 필지 정보 서비스의 건물 윤곽 표시" 다. 승인되면 그 계정으로 첫 판을 받는 단계를 알려 준다.
- 다음 PR 들은 2의 순서를 따른다. 각 PR 은 자기 레인의 실측 칸 배치, 작업(`jobs.v1.json`, `scope`), 그래프 엣지,
  ODCS 계약을 한꺼번에 싣는다.
