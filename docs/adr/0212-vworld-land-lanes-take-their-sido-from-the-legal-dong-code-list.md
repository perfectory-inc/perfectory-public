# ADR 0212: VWorld 토지 레인의 시도는 법정동코드 목록에서 온다

- Status: Accepted
- Date: 2026-10-14
- Amends: [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)
  ③(토지 레인 계약의 `completeness` 가 시도 개수를 적는다)
- Builds on: [ADR-0143](./0143-legal-dong-code-changes-come-from-code-go-kr.md)(code.go.kr 전체 표,
  `reference.legal_dong_code_snapshot`)

## Context

ADR-0169 ③의 VWorld 토지 레인 여섯 개(토지특성·임야대장·개별공시지가·대지권등록·토지이동이력·토지이용계획)는
계약 `vworld-land-*-source-objects.json` 의 `silver_refresh.completeness` 에 `"count": 17` 을 손으로 적고,
판의 시도 수가 그 수와 같을 때만 판으로 쳤다. 2026-07-01 광주(29)·전남(46)이 전남광주(12)로 합쳐져 시도는
16 이다. 원천이 새 코드로 시도 16 파일을 내면 여섯 레인이 모두 그 판을 `skipped_incomplete` 로 버리고, 옛 판에
머문다. 같은 사실(시도 목록)이 여섯 파일에 손으로 복제돼 있었고, 정본은 이미 따로 있다:
`reference.legal_dong_code_snapshot`(code.go.kr 전체 표, 코드마다 생성일·폐지일).

## Decision

1. 시도 레인의 계약은 시도 수를 적지 않는다. `completeness` 는
   `{"load_granularity": "sido", "regions_from": "reference.legal_dong_code_snapshot"}` 이다. 계약 검사
   (`silver_refresh/lane.rs`)는 시도 레인의 `count` 와 다른 `regions_from` 을 거부한다. 전국 레인
   (용도지역 코드표)은 그대로 `national` 1 이다.
2. 레인은 실행마다 그 표의 현재 Iceberg 스냅숏을 읽어(`lakehouse_snapshot_scan`, 코어 크레이트) 가장 새
   `snapshot_date` 의 시도 행(`NN00000000`)으로 달력을 만든다(`land::SidoCalendar`). 판 `YYYYMMDD` 에서
   생성일 ≤ 판 < 폐지일 인 코드가 그 판이 덮어야 할 시도다. 폐지일 없는 `폐지` 행은 시점을 알 수 없어 뺀다.
3. 판은 그 날짜의 시도 집합과 **같은** 지역 집합을 가질 때만 판이다(개수가 아니라 코드). 옛 코드로 늦게 온 새
   판은 건너뛰고 직전의 완전한 판을 쓴다 — 틀린 코드를 새 날짜로 싣는 것보다 기다리는 쪽이 안전하다.
4. 기계적 차단: `land_tests.rs` 가 (a) 여섯 계약에 시도 수가 없고 `regions_from` 이 코드 목록임을, (b) 손으로
   적은 `count` 를 넣은 계약이 거부됨을, (c) 통합 전후 판이 달력대로 골라짐을 확인한다.

## Consequences

- 시도 통합·분리는 코드 목록 수집(ADR-0143, 매일)만으로 레인에 반영된다. 계약 PR 이 필요 없다.
- 레인 실행은 Iceberg 카탈로그와 레이크하우스 R2 읽기가 판 선택 전에 필요하다(이미 적재 단계에서 쓰던 것).
  코드 목록 표에 스냅숏이 없으면 시도 레인은 거부된다.
- 기각한 대안: 계약의 17 을 16 으로 고치기(다음 개편에 또 틀린다), 시도 목록을 별도 JSON 에 두기(사본이 하나 더
  생긴다), 판에서 가장 많은 시도 수를 기준으로 삼기(한 시도가 빠진 판을 완전하다고 본다).
