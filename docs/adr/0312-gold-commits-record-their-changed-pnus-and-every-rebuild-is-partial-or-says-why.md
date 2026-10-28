# ADR 0312: Gold 커밋은 바꾼 PNU 를 기록하고, 모든 재구축은 부분이거나 전량인 이유를 적는다

- Status: Accepted
- Date: 2026-10-28

## Context

사장 원칙: 모든 처리는 바뀐 것만 다시 하고, 각 단위는 자기 몫만 읽는다. 전량은 이유와 잰 비용을 적은 예외다.
2026-10-28 Gold 재구축과 굽기 변경 탐지에서 이 원칙이 깨진 곳 넷을 쟀다(ai-server, 읽기만).

1. **굽기 delta 가 Gold 스냅숏 둘을 통째로 조인한다.** `by_pnu_panel_delta.py` 는 레인마다 두 스냅숏을 PNU 로
   full outer join 해 `row_digest` 를 비교한다. 증분 Gold(ADR-0180)는 자기가 합친 PNU 를 이미 아는데도 그렇다.
   실측(`/data/foundation-platform/by-pnu-bake/*/runs/*/change-set.json` 의 `wall_seconds`):

   | 레인 | 행 | 바뀐 PNU | delta 초 |
   |---|---|---|---|
   | parcel | 39,861,511 | 0 | 410.7 · 473.6 · 599.2 |
   | parcel | 39,861,511 | 4,265 | 458.7 |
   | parcel | 39,861,511 | 341,651 | 318.1 |
   | parcel-zoning | 39,729,618 | 0 | 791.6 · 897.4 · 1,536.0 |
   | parcel-price | 35,784,007 | 0 | 456.0 · 914.0 |
   | parcel-forest-ledger | 39,719,263 | 4,258 | 701.4 |
   | building | 5,945,767 | 0 · 7 | 21.7 – 45.7 |

   바뀐 것이 0 개여도 40M 행 레인은 7–25 분을 쓴다. 문서 첫머리의 "Gold panels are rebuilt whole" 은 ADR-0180
   뒤로 틀린 말이다.
2. **공매(`gold.parcel_public_auction`)가 늘 전량이다.** 4.6K 행을 내는데 230 → 257 → 510 → 511 초로 느려졌다
   (10-24 · 25 · 26 · 28 `gold-kind` 줄). 10-28 계획: `mode_reasons: ["the contract declares no incremental
   rebuild for this table"]`. 원인 셋:
   - 자기 생산자 종류는 `kind_tables` 의 `incremental` 을 물려받지 않았다(`gold_rebuild.kind_tables`).
     계약이 incremental 도 전량 이유도 적지 않았는데 아무도 막지 않았다(10-24…10-28).
   - `lane_inputs` 가 물건·사진 표를 "통째 입력"으로 적었다(`current_rows` 가 모든 판을 읽는다는 이유).
   - 빌드가 필지 경계 전체(served edition, 14.9GB 표)와 계보 후보 6,504,198 개를 읽고, 계수기 `count()` 마다
     계획 전체를 다시 돌렸다. 10-28 로그: 계보 사슬 걷기 ≈ 85 초, 후보 6.5M 행 broadcast 작업이
     31.3 · 31.8 · 49.4 · 49.9 초, 필지 경계 스캔이 패스마다 다시. 서빙되는 회차는 52,269 개뿐이다.
3. **스키마 이주 백필이 다음 Gold 재구축을 전량으로 만든다.** `lakehouse_schema_migrate.backfill` 은 모든 행을
   다시 쓰는 커밋에 생산자 핀을 남기지 않는다. 계획은 머리가 "생산자 커밋이 아니다"로 보고 전량을 고른다
   (`gold_rebuild.head_build_pins`). 백필 함수는 빌드 자신의 함수(`BACKFILLS`)라 내용은 그대로인데도 그렇다.
4. **계획은 Silver 핀만 보고 생산자 코드는 보지 않는다.** 코드 변경은 패리티 표본(ADR-0180 §5)이 우연히 잡을
   때만 전량이 된다. 표본 20,000 PNU 가 n 개 PNU 에만 닿는 코드 변경을 잡을 확률은 `1 − (1 − 20000/N)^n`:

   | 표 (N) | n = 100 | n = 1,000 | n = 4,000 | n = 10,000 |
   |---|---|---|---|---|
   | parcel 39.86M | 5% | 39% | 87% | 99.3% |
   | building 5.95M | 29% | 97% | ≈100% | ≈100% |

## Decision

1. **증분 Gold 커밋은 바꾼 PNU 를 `gold.<t>__changes` 에 쌓는다.** ADR-0191 §4 ⑥이 측정 뒤로 미뤄 둔 그 표다.
   새 장치를 만들지 않고 ADR-0191 의 이름(`__changes`)·열(`_from_snapshot_id`, `_to_snapshot_id`)·사슬 걷기
   (`changelog.chain`)를 그대로 쓴다.
   - 행: `pnu`, `_from_snapshot_id`(커밋의 부모), `_to_snapshot_id`(커밋), `_to_snapshot_id` 로 나눈다. 쌓기만 한다.
     Silver 변경 기록처럼 모양은 원본 표에서 파생하고 계약에 따로 적지 않는다(ADR-0191 §1).
   - 쓰는 곳은 `gold_incremental.Outcome.commit` 한 곳이다. 합치기는 합친 출처(`plan_merge` 의 source: upsert 와
     delete)의 PNU 를, 빈 합치기와 내용이 같은 전량(ADR-0274 §1 의 빈 append)은 PNU 없이 걸음만 남긴다. 걸음마다
     NULL 표지 한 행을 더해, 바뀐 것이 없는 걸음도 기록된다.
   - Gold 커밋 **뒤에** 쓴다. 기록이 실패하면 그 걸음은 기록 없음이 되고, 다음 delta 가 두 스냅숏 조인으로 돌아갈
     뿐이다. 그래서 실패는 로그(`gold-changes-unrecorded`)로 남기고 실행을 실패시키지 않는다.
   - **delta 는 기록을 먼저 읽는다.** 기준 스냅숏에서 지금 스냅숏까지 `main` 을 걸어 모든 걸음이 기록됐거나
     압축(`replace`)이면, 기록된 PNU 만 두 스냅숏에서 골라 비교한다. 200 개 이하는 Iceberg 가 파일을 거르는 `IN`
     필터로(빈 집합은 파일을 하나도 읽지 않는다), 그 위는 semi-join(500,000 개까지 broadcast)으로. 나머지 행은
     `same` 이고 수는 스냅숏 요약의 `total-records` 에서 뺀다. 전량 덮어쓰기·백필·손 쓰기처럼 기록 없는 걸음이
     하나라도 있으면 지금처럼 두 스냅숏을 조인한다. 두 길의 개수와 목록은 같다(시험이 단언한다). 요약에
     `change_source: recorded|diff` 를 남긴다.
2. **모든 재구축은 부분이거나, 전량인 이유를 적는다.**
   - `gold_rebuild.load_contract` 는 `incremental` 과 `full_reason` 중 정확히 하나를 적지 않은 표를 거부한다
     (`PlanError`, `max_row_loss_reason` 이 없을 때와 같이). 막는 사고: 10-24…10-28 공매가 어느 쪽도 적지 않고
     매 실행 전량으로 돌았다.
   - 자기 생산자 종류도 `kind_tables.incremental` 을 물려받는다. 그럴 수 없는 종류는
     `kind_tables.overrides.<lane>.full_reason` 을 적는다.
   - 공매 생산자는 `gold_incremental.Lane` 으로 바뀐 PNU 만 다시 만든다. 영향 PNU(`affected_pnus`)는 행이 바뀐
     회차(물건·사진 표의 두 판 차이)와, 바뀐 입력이 없어도 지난 빌드 뒤 마감 시각이 지난 회차의, **지금 Gold 가 그
     회차를 싣는 PNU** 와 **지금 놓이는 PNU** 다. 필지 경계와 계보는 모든 회차를 옮길 수 있어 통째 입력으로 남긴다
     (바뀌면 전량, 이유는 계획에). 같은 `placement` 함수가 전량과 증분을 만들고, 패리티 표본과 시험(증분 = 전량)이
     이를 단언한다.
   - 공매가 읽는 필지는 물건 행이 이름 대는 PNU 와 그 계보 후계뿐이다(`parcels_named`, 한 번 읽어 둔다). 계보
     후보도 그 PNU 에서 나온 것만 broadcast 한다(`read_carry_candidates(sources=)`).
3. **스키마 이주 백필은 다시 쓴 생산자 커밋의 핀을 그대로 싣는다**(`lakehouse_schema_migrate.carried_properties`).
   등록된 백필은 빌드 자신의 함수라 행은 여전히 그 핀으로 빌드한 것이다. 계획은 내용 변화가 없다고 보고(핀이 같으면
   `nothing_to_do`, 입력이 움직였으면 `incremental`) 전량으로 가지 않는다. 머리가 생산자 커밋이 아닌 표(손 쓰기)는
   실을 핀이 없어 지금처럼 전량이다. 백필 커밋은 변경 기록이 없으므로 다음 굽기 delta 는 두 스냅숏을 비교한다
   (`row_digest` 가 백필로 바뀌었을 수 있다).
4. **생산자 코드 변경 → 선언된 전량은 후속으로 둔다.** 설계는 ADR-0191 §5 그대로다: 생산자가 자기 모듈 소스의
   해시(`foundation.build-source-sha256`, docstring·주석을 뺀 AST 기준)를 커밋 요약에 남기고, 계획은 머리의 값과
   다르면 `full` 과 그 이유를 적는다. 이번에 넣지 않는 이유: 모든 생산자의 커밋 경로와 계획을 함께 바꿔야 하고,
   배포 직후 값이 없는 머리를 어떻게 볼지(전량이면 필지 패널 40M 전량 재구축 한 번이 배포마다 따른다)를 정해야 한다.
   그때까지 위 표의 확률이 지금 막는 정도다.

## W1 (ADR-0191 Silver 변경 기록) 과의 이음매

Gold 의 `affected` 는 여전히 Silver 두 판의 차이(`changed_rows`)로 바뀐 행을 찾는다. ADR-0191 ③④가 Silver
`__changes` 를 내면 그 자리에서 `changelog.chain` 으로 걸음을 읽어 차이를 대신한다. 이 ADR 은 같은 `chain` 과 열
이름을 Gold 쪽에 써 두었으므로, Silver 쪽이 들어와도 걷는 규칙은 하나다. `changelog.py` 는 고치지 않았다.

## Consequences

| 무엇 | 전 (잰 값) | 후 (계산한 값, 운영 측정 전) |
|---|---|---|
| 굽기 delta, 40M 레인, 바뀐 PNU 0 | 410 – 1,536 초, 두 스냅숏 79.7M 행 full outer join | 메타데이터만(스냅숏·`__changes.partitions`), 데이터 파일 0 개; Spark 기동 몫만 |
| 굽기 delta, 40M 레인, 바뀐 PNU 4,265 | 458.7 초, 79.7M 행 셔플 | 셔플 0, broadcast semi-join 으로 두 스냅숏의 두 열만 훑음; 비교 행 ≤ 8,530 |
| 공매 재구축 | 늘 full, 230 → 511 초, 4.6K 행 덮어쓰기 | 물건이 바뀐 시간: 영향 PNU 만 합침(한 시간 몫의 회차); 후보 broadcast 6.5M → 물건이 이름 대는 PNU 에서 나온 것만; 필지는 한 번, 이름 댄 것만 남김. 계보 사슬 걷기 ≈ 85 초는 남는다 |
| 백필 다음 재구축 | 전량(필지 패널 40M) | 핀이 같으면 `nothing_to_do` |

- 운영 전후 값은 다음 배포 뒤 같은 줄로 잰다: 굽기 `change-set.json` 의 `wall_seconds`·`change_source`, 공매
  `gold-kind ... mode= seconds= changed_keys=`.
- 처음 한 번은 기록이 없다: 배포 뒤 첫 굽기는 기준 스냅숏이 기록 이전이라 지금처럼 조인한다. 그 다음부터 기록을 읽는다.
- `gold.<t>__changes` 는 쌓기만 하고 작다(커밋당 바뀐 PNU 수 + 1 행). 만료·압축 대상이 아니다.
- 공매의 계보 사슬 걷기 85 초(드라이버에서 6.5M 링크 전체)는 계보 판이 바뀔 때만 다시 할 수 있도록 후보를
  계보 스냅숏별로 두는 것이 다음 일이다.
