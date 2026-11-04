# ADR 0381: 승격한 판은 두 판이 다른 PNU 만 다시 짓고, 모든 서빙 레인이 함께 옮겨 간다 — 승격은 콘솔에서 요청한다

- Status: Accepted
- Date: 2026-11-04
- Implements: [ADR-0291](./0291-the-served-cadastral-edition-is-operator-chosen-and-readiness-gated.md) §3(동시 재굽기), §1(운영자 선택)
- Builds on: [ADR-0379](./0379-a-cadastral-edition-is-served-only-through-a-readiness-gated-promotion-recorded-in-the-contract.md)(승격 관문·원장), [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분 Gold), [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(커밋이 지은 것을 기록), [ADR-0282](./0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md)(운영자 스위치), [ADR-0303](./0303-each-by-pnu-lane-is-its-own-schedule-pipeline.md)(레인마다 파이프라인), [ADR-0334](./0334-each-user-journey-has-an-slo-and-every-data-release-is-written-audited-and-canaried-before-it-is-served.md)(WAP)

## Context

승격은 `served_edition` 만 옮긴다. 두 판은 이미 `silver.parcel_boundaries` 에 나란히 있으므로 Silver 스냅숏은 하나도 움직이지
않는다. 그래서 오늘의 플래너(`gold_rebuild.plan`)는 승격 뒤에도 "할 일 없음"이라 말하고, 모든 레인이 옛 판을 계속 서빙한다.
반대로 증분 재빌드가 돌더라도 비교 쪽(`read_served_parcels` 의 앞 핀)이 지금 계약의 서빙판을 읽는다. 그러면 두 쪽이 같은 판이라
바뀐 것이 보이지 않는다. 또 레인(필지 패널과 그 by-PNU 종류 8개)은 각자 일정대로 다시 지어 각자 내보낸다. 그래서 그 사이에는
판이 섞여 서빙된다(2026-10-21 공매 632 고아와 같은 종류).

두 판의 차이(서울 시도 11, 202606 → 202609, 11-04 Trino 실측):

| 무엇 | 필지 수 |
|---|---|
| 한쪽 판에만 있는 PNU | 2,750 (새 판에만 480, 옛 판에만 2,270) |
| 두 판 모두에 있고 도형 바이트가 다른 것 | 680,482 |
| 같은 것 | 215,989 |
| 바이트가 다른 것 중 종로구 표본 | 37,862 가운데 37,852 가 중심 이동 1e-13 도 이하(부동소수 잡음), 실제로 바뀐 것은 10 |

필지 패널이 필지에서 읽는 열은 `pnu` 하나다(`BUILD_COLUMNS`). 그래서 패널 차이는 PNU 집합의 차이다(서울 0.31%). 도형을
읽는 레인이 바이트로 비교하면 76% 가 바뀐 것으로 보여 전량이 된다.

## Decision

1. **Gold 커밋은 지은 판을 기록한다.** `gold_rebuild.pins_properties`(커밋 속성의 유일한 작성자)가 필지를 읽는 커밋에
   `foundation.served-parcel-edition` 을 적는다. 기록 전의 커밋은 승격 원장으로 날짜를 맞춘다(`vworld_parcel_editions.served_at`).
2. **판이 옮겨 가면 필지 입력이 바뀐 것이다.** 플래너는 지은 판과 서빙판이 다르면 재빌드 이유를 하나 더하고, 증분이면 생산자에
   `--incremental-from-parcel-edition <앞 판>` 을 넘긴다. `gold_incremental.validate_arguments` 는 그 값을 앞 핀에만
   `silver.parcel_boundaries@served_edition` 으로 싣는다. `read_served_parcels`(→ `served_rows(snapshot=…)`)는 앞 핀에서 그 판을
   읽는다. `changed_inputs` 는 필지를 바뀐 입력으로 센다. 그러면 기존 장치(`changed_rows` → `affected_pnus` → 패리티 표본 →
   MERGE → `__changes` → by-PNU 굽기)가 **두 판이 다른 PNU 만** 다시 짓는다. 서울로 치면 89.9만 중 2,750 이다. 새 장치는 없다.
   생산자마다 손댄 곳도 없다(공매·소유자 종류도 같은 읽기를 쓴다).
3. **모든 서빙 우주 레인이 함께 옮겨 간다(장벽).** `scripts/ops/edition_lockstep_gate.py` 가 레인을 계약에서 끌어낸다: 필지 패널과
   그 패널을 호스트로 하는 종류(`by_pnu_kinds`, 지금 8개). 각 Gold 의 판을 플래너와 같은 방식으로 읽는다(카탈로그 REST,
   `lane_follow_gate.py` 와 같은 읽기). 하나라도 서빙판이 아니면 `by-pnu-serving-bake.sh` 의 그 레인들은 내보내지 않고 `waiting` 으로
   끝난다. 지금 서빙하는 것은 그대로 남는다. 모두 서빙판이 되면 모두 내보낸다. Netflix·Tabular 의 WAP 처럼 "쓰기 → 감사 → 함께
   게시"이며, 게시 단위가 레인 하나가 아니라 서빙 우주 전체다. 11-04 실측: 9개 Gold 모두 202606 으로 읽히고 장벽은 열린다.
4. **승격은 콘솔에서 요청한다(감사됨).** 운영자 스위치에 `promote_edition` 을 더한다. `parcel_edition_readiness` 작업에만 묻고,
   판(`YYYYMM`)을 이름하며, 48시간 뒤 만료된다. `catalog.operator_action` 에 누가·언제·왜가 덧붙이기로 남는다(마이그레이션
   20261104120000). 파일은 `switches/promote.d/<id>` 이다. 판정 작업은 요청마다 한 번 답한다.
   - 그 판이 지금 보고서로 `ready` 면 `promote` 가 승격된 계약(원장에 요청자·이유가 든 한 줄)을
     `/var/lib/foundation-platform/parcel-editions/promotions/<id>.contract.json` 에 둔다.
   - 아니면 이유와 함께 거부한다.
   - 서빙은 그 계약 변경이 검토(PR)를 거쳐 배포될 때 옮겨 간다. Kargo(Akuity)가 단계 검증을 통과한 Freight 를 Git 커밋으로
     승격하는 것과 같은 GitOps 다. 서버가 저장소에 직접 쓰는 길은 만들지 않는다(자격증명이 늘어난다; 사장 결정 사항).
   - 더니어 「지적 판」 탭: 준비된 판에 승격 요청 칸이 있고, 요청과 답을 보인다.
5. **관문은 한 배포 늦게 선다 — 배포 순서.** ① 이 변경을 배포한다(장벽은 승격이 없으면 언제나 열린다). ② 레인 9개가 각자
   한 번씩 기록 있는 커밋을 남길 때까지 기다린다(매일 재빌드, `gold.parcel_*` 의 `foundation.served-parcel-edition`; 원장 날짜
   맞춤은 승격 PR 과 배포 사이에 지어진 커밋을 새 판으로 오인할 수 있으므로 기록이 정본이다). ③ 콘솔에서 승격을 요청하고,
   승인된 계약을 PR 로 낸다. ④ 그 배포 뒤 플래너가 각 Gold 를 증분으로 다시 짓고, 장벽이 모두 지어진 때 함께 내보낸다.

## Consequences

- 승격 한 번의 패널 재계산은 두 판이 다른 PNU 만큼이다(서울 0.31% 실측; 전국으로 늘리면 약 12만 PNU, 계산값).
  `max_changed_key_fraction`(0.3) 아래이므로 전량으로 떨어지지 않는다.
- 타일 레인은 도형을 굽는다. 승격은 바뀐 필지가 있는 시도마다 다시 굽는다(ADR-0316 Part C). 시도마다 한쪽 판에만 있는
  필지가 있으므로 사실상 전국이다(10-15 실측 82분). 이것은 선언된 예외다. 도형 잡음(1e-13 도)을 허용 오차로 거르는 비교는
  도형 레인을 부분 처리로 바꿀 때 쓴다.
- 배포 직후 한 번: 생산자 코드가 바뀌었으므로(ADR-0321) 각 패널 Gold 는 전량을 계산해 바뀐 키만 게시한다. 계산은 전량이다.
  이 변경은 행을 바꾸지 않으므로 게시되는 키는 0 이어야 한다.
- 남은 일: PostGIS 거울·카탈로그 투영·시군구 대응표는 by-PNU 레인이 아니어서 장벽 밖이다. 이들은 이미 서빙판 하나를
  읽는다(`served_edition_view`). 판이 옮겨 가면 다음 실행이 새 판을 지으므로 그 사이 몇 시간은 엇갈린다. 이들을 장벽에 넣는
  것은 후속이다.
