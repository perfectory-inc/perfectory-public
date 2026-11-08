# ADR 0451: 필지 화면에 그 필지의 건축인허가 — 자기 by-PNU 종류 parcel-building-permits

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0287 §9(자기 생산자 종류), ADR-0292(서빙 필지판의 PNU 만), ADR-0312 §2(바뀐 키만),
  ADR-0329 §3(`views.changed_codes`), ADR-0356(건축인허가 Silver), ADR-0450(같은 틀의 총괄표제부 종류),
  ADR-0435(첫 발행은 자동 승격)
- Amends: ADR-0356 — 기본개요의 소비자는 건물 by-PNU 패널의 수명 칸이 아니라 필지 레인의 자기 종류다

## Context

사장 목표(2026-11-03·11-08)는 "보여 주는 것이 이긴다" 다. 정의에서 만든 표(`production-readiness` §10)에서 **건축인허가
기본개요**(`silver.building_permit_basis_outline`)는 매달 적재되는데도 Gold·서빙 길이 없는 "없음" 줄이었다. 산업 부동산
사용자에게 "이 필지에 지금 무엇이 지어지고 있나, 허가만 나고 착공하지 않았나" 는 표제부(다 지은 건물)로는 보이지 않는다.

ADR-0356 은 소비자를 "건물 by-PNU 패널의 수명 칸" 으로 적었다. 총괄표제부(ADR-0450)와 같은 이유로 595만 PNU 의 건물 패널
생산자를 다시 재는 대신 필지 레인의 자기 종류로 둔다.

**측정 (2026-11-09, 운영 Trino 읽기 전용).** 판 `202610` 하나: 허가 6,527,521 건(관리허가대장PK 모두 고유), PNU 3,742,220 개.
한 PNU 에 최대 11,076 건(공유 지번), 99% 는 8 건 이하, 99.9% 는 22 건 이하. 건축구분은 신축 2,894,618 · 빈칸 2,138,242 ·
증축 954,682 · 용도변경 442,992 · 대수선 48,252 · 가설건축물축조허가 23,334.

### 실사례

- **허가 이력은 부동산 문서의 한 구획.** BuildZoom·Reonomy 는 필지(부동산) 화면에 그 필지의 건축 허가 목록을 최신순으로 두고, 한
  허가마다 종류·용도·면적·날짜만 보인다. 우리 by-PNU 종류(ADR-0193)가 같은 모양이다.
- **키가 바뀌는 사실은 양쪽에서 비교한다**(ADR-0450 과 같다).

## Decision

1. **종류 한 줄** `parcel-building-permits`(호스트 `parcel_by_pnu_gateway`, 종류 `building-permits`, 섹션 `building_permits`,
   열 `building_permits_json`·`building_permit_total:long`, 생산자 `building_permit_silver_to_gold`). 경로는
   `/parcels/building-permits/by-pnu/`. 정의 `definitions/parcel-building-permits.definition.json` 하나가 작업·자산·연결·레인·
   메모리·레지스트리를 갖는다(ADR-0418).
2. **문서.** 그 필지의 현재 허가(관리허가대장PK 마다 하나)를 사용승인일·실제착공일·착공연기일·착공예정일·건축허가일 가운데 늦은
   날 순으로 최대 50, 칸은 이름 붙은 Silver 칸 그대로(`arch_kind_name`·`main_purpose_name`·`plot_area_m2`·`building_area_m2`·
   `total_floor_area_m2`·`permit_date`·`start_scheduled_date`·`start_delayed_date`·`start_actual_date`·`use_approval_date`),
   빈 값은 null. `building_permit_total` 이 전부를 센다. 건물명·대지위치는 싣지 않는다(ADR-0356). 50 은 99.9% 의 필지를 다
   담고, 공유 지번 1만 건도 문서를 256 KiB 묶음 예산 안에 둔다.
3. **법정동 코드 변경을 따라간다.** 모든 필지 섹션과 같은 `renumbered`(ADR-0329 §3). 변경 표가 움직이면 종류를 통째로 짓는다
   (`WHOLE_TABLE_INPUTS`, 필지 종류 공통의 선언된 예외). 합성 99999 코드로 옮겨진 행이 새 코드 필지에 붙는 시험이 있다.
4. **부분 처리.** ADR-0450 §4 와 같다: 키가 PNU 가 아니므로 고정한 Silver 와 지금 Silver 의 현재 행을 PNU 로 양방향 비교해
   달라진 PNU 와 필지판에 들고 난 PNU 만 다시 계산한다.
5. **화면.** 공짱 필지 패널의 총괄표제부 다음에 "건축인허가" 구획: "건축인허가 N건", 허가마다 건축구분·주용도 / 대지·건축·연면적 /
   건축허가·착공예정·착공연기·실제착공·사용승인일. 라벨은 원천 칸 이름이고, 출처 문구는 넣지 않는다. 대장이 `0` 이나 빈칸으로 둔
   수치는 보이지 않는다.
6. **켜기.** 두 작업(`gold_parcel_building_permits_rebuild`·`by_pnu_parcel_building_permits_bake`)은 꺼진 채로 들어간다. 첫 전량
   빌드를 잰 뒤 켜고(ADR-0386 §3), 첫 발행은 ADR-0435 자동 승격이 한다. 새 스위치는 없다.

## Consequences

- §10 표에서 건축인허가 기본개요 줄이 "없음 → 길 있음·꺼짐" 이 된다(정의에서 계산). 작업이 켜지면 "화면" 이다.
- **비용(계산).** 첫 전량 빌드는 653만 행 Silver 와 서빙 필지판 PNU 열 하나를 읽는다 — 소유 종류(3,940만 필지)보다 한 자릿수
  작다. Gold 는 374만 행 안팎, R2 월 수십 원. 이후 빌드는 새 판이 허가를 바꿀 때(월 1 회)와 필지판이 움직일 때만 일을 한다.
- 건축구분이 빈칸인 허가(33%)는 주용도만 제목에 보인다. 건축인허가의 다른 표(동별개요·층별개요·주차장·대수선 등)는 이 종류에
  넣지 않는다. 각각 Silver 가 먼저 있어야 하고(지금 주차장만 적재됨), 붙일 때는 이 문서의 레코드에 관리허가대장PK 로 잇는다.
