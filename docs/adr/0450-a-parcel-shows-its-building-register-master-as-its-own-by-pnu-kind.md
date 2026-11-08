# ADR 0450: 필지 화면에 그 필지의 총괄표제부 — 자기 by-PNU 종류 parcel-register-masters

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0287 §9(자기 생산자 종류), ADR-0292(서빙 필지판의 PNU 만), ADR-0312 §2(바뀐 키만),
  ADR-0329 §3(`views.changed_codes`), ADR-0357(총괄표제부 Silver), ADR-0399·ADR-0449(같은 틀의 멸실·등록공장 종류),
  ADR-0435(첫 발행은 자동 승격)
- Amends: ADR-0357 §3 — 소비자는 건물 by-PNU 패널의 칸이 아니라 필지 레인의 자기 종류다

## Context

사장 목표(2026-11-03·11-08)는 "보여 주는 것이 이긴다" 다. 정의에서 만든 표(`production-readiness` §10)에서 **총괄표제부**
(`silver.building_register_master`)는 매달 적재되는데도 Gold·서빙 길이 없는 "없음" 줄이었다. 여러 동이 선 대지(공장 단지형)의
규모 — 대지·건축·연면적 합계, 주·부속 건축물 수, 총주차수 — 는 표제부(동마다)만으로는 화면에 나오지 않는다.

ADR-0357 §3 은 소비자를 "건물 by-PNU 패널" 로 적었다. 건물 패널(`gold.building_panel`, 595만 PNU)은 표제부·층·호를 한 생산자가
묶는 큰 Gold 이고, 칸을 더하면 그 생산자와 증분 계획 전부를 다시 재야 한다. 멸실·부속지번·에너지·등록공장이 이미 같은 이유로
필지 레인의 자기 종류가 되었다(ADR-0287 §9).

**측정 (2026-11-09, 운영 Trino 읽기 전용).** 판 `202610` 하나: 행 621,193(대지 621,193), PNU 617,386 개. 한 PNU 에 최대 70 대지,
99% 는 1 대지. 주용도 상위는 단독주택 208,599 · 공장 71,926 · 동물및식물관련시설 69,137 · 창고시설 38,495. 대지면적이 `0` 인
행이 표본에 흔하다(제공자가 비워 둔 값).

### 실사례

- **사실을 차원 키 아래 묶어 둔 읽기 모델.** CoStar·Reonomy 의 부동산 화면은 대지(site) 합계를 필지 문서의 한 구획으로 두고,
  동별 정보는 그 아래 따로 둔다. 우리 by-PNU 종류(ADR-0193)가 같은 모양이다.
- **키가 바뀌는 사실은 양쪽에서 비교한다.** Delta·Iceberg 의 change data feed 는 한 키의 이전 행과 새 행을 함께 내보내, 키가 다른
  파티션으로 옮겨 가도 옛 쪽을 지운다.

## Decision

1. **종류 한 줄** `parcel-register-masters`(호스트 `parcel_by_pnu_gateway`, 종류 `register-masters`, 섹션 `register_masters`,
   열 `register_masters_json`·`register_master_total:long`, 생산자 `building_register_master_silver_to_gold`). 경로는
   `/parcels/register-masters/by-pnu/`. 정의 `definitions/parcel-register-masters.definition.json` 하나가 작업·자산·연결·레인·
   메모리·레지스트리를 갖는다(ADR-0418).
2. **문서.** 그 필지의 현재 총괄표제부(관리건축물대장PK 마다 하나)를 사용승인일·착공일·허가일 가운데 늦은 날 순으로 최대 50,
   칸은 이름 붙은 Silver 칸 그대로(`plot_area_m2`·`building_area_m2`·`total_floor_area_m2`·`main_purpose_name`·
   `main_building_count`·`annex_building_count`·`parking_count`·`permit_date`·`start_date`·`use_approval_date`), 빈 값은 null.
   `register_master_total` 이 전부를 센다. 건물명은 싣지 않는다(ADR-0357 §1). 서빙 필지판의 PNU 만 문서를 갖는다(ADR-0292).
3. **법정동 코드 변경을 따라간다.** 모든 필지 섹션과 같은 `renumbered`(ADR-0329 §3)로 서빙 코드 아래에 읽는다. 변경 표가 움직이면
   종류를 통째로 짓는다(`WHOLE_TABLE_INPUTS`, 필지 종류 공통의 선언된 예외). 합성 99999 코드로 옮겨진 행이 새 코드 필지에 붙는
   시험이 있다.
4. **부분 처리.** Silver 의 키는 PNU 가 아니라 관리건축물대장PK 다. 덧붙음만 읽으면 대지가 다른 PNU 로 옮겨 갈 때 옛 PNU 를
   모른다. 그래서 고정한 Silver 와 지금 Silver 의 현재 행을 PNU 로 비교해(`changed_rows`, 양방향) 달라진 PNU 와 필지판에 들고 난
   PNU 만 다시 계산한다. 비교는 62만 행 두 벌이라 덧붙음 읽기와 비용 차이가 없다.
5. **화면.** 공짱 필지 패널의 건물 다음에 "총괄표제부" 구획: "총괄표제부 N건", 대지마다 주용도 / 대지·건축·연면적 /
   주·부속건축물수·총주차수 / 허가·착공·사용승인일. 라벨은 원천 칸 이름이고, 출처 문구는 넣지 않는다. 대장이 `0` 이나 빈칸으로 둔
   수치는 보이지 않는다(값을 지어내지 않고, 없는 값을 0 으로 보이지도 않는다).
6. **켜기.** 두 작업(`gold_parcel_register_masters_rebuild`·`by_pnu_parcel_register_masters_bake`)은 꺼진 채로 들어간다. 첫 전량
   빌드를 잰 뒤 켜고(ADR-0386 §3), 첫 발행은 ADR-0435 자동 승격이 한다. 새 스위치는 없다.

## Consequences

- §10 표에서 총괄표제부 줄이 "없음 → 길 있음·꺼짐" 이 된다(정의에서 계산). 작업이 켜지면 "화면" 이 되어 화면에 닿는 Silver 가
  28 → 29 개다(등록공장이 먼저 켜지면 29 → 30).
- **비용(계산).** 첫 전량 빌드는 62만 행 Silver 와 서빙 필지판 PNU 열 하나를 읽는다 — 멸실 종류(42만 필지)와 같은 규모다. Gold 는
  60만 행 안팎, 문서마다 수백 바이트라 수백 MB 아래, R2 월 10원 아래. 이후 빌드는 새 판이 대지를 바꿀 때(월 1 회)와 필지판이
  움직일 때만 일을 한다.
- 대지면적이 비어 있는 대지는 면적 줄이 짧게 보인다. 건폐율·용적률은 Silver 가 아직 이름 붙이지 않은 칸이라(ADR-0357 §1)
  이 종류에 없다. 이름을 붙이는 일은 그 Silver 의 후속이다.
