# ADR 0471: Gold 입력이 아직 적재되지 않은 레인의 컷오버는 풀을 잡기 전에 기다린다

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0435(자동 승격), [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)
  (claim 은 Spark 단계 동안만), ADR-0350 §6·ADR-0371 §5(`history` 준비 단계), ADR-0439(갱신이 첫 발행보다 앞)

## Context

**사고 (W47b, 2026-11-09 잰 값, `by-pnu-bake/cutover/logs/cutover-g*.log`·`gold-rebuild.log`).** 자동 승격이 Silver 가 아직
없는 레인의 컷오버를 시작했고, 그 컷오버는 gold 단계에서 spark 풀을 claim 한 뒤에야 Gold 계획기에게 거절당했다.

| 시각(Z) | 레인 | gold 단계 | 계획기의 거절 |
|---|---|---|---|
| 11:59, 17:30 | parcel-court-auction | 33 s, 56 s 실패 | `silver.court_auction_item_photo` 없음(허용 목록에 없음) |
| 16:58 | parcel-building-permits | 73 s 실패 | 필수 Silver 8 개 없음 |
| 17:42 | parcel-nearby | 46 s 실패 | 필수 Silver 7 개 없음(data.go.kr 적재가 만드는 표) |
| 18:42 → 19:20+ | parcel-nearby | claim 을 쥐고 floor-overview Silver 적재·패널 Gold 뒤에서 38 분 넘게 대기 | (아직 대기 중, 끝나면 같은 거절) |

실패마다 유닛이 OnFailure 로 알렸고, 배포마다 백오프가 초기화되어(ADR-0435) 다시 시도했다. 18:42Z claim 이 대기열 맨 앞에
있는 동안 그 레인의 입력을 만드는 `data_go_kr_file_data` 의 시작(11-09 18:15Z 조정기가 걸어 둔 run)은 그 뒤에 선다. 같은
날 11:40Z 에는 이 적재의 일정 시작이 11:02Z 부터 줄 선 컷오버 claim 에 거절됐다(`start-scheduled-job: ... kept for claims queued
before ...`). 레인이 자기 입력을 만드는 적재 앞에 서는 순서 역전이다.

답은 이미 있었다. 계획기(`gold_rebuild.plan`)는 필수 입력이 없거나, 선택 입력이 없는데 `absent_inputs_accepted` 에 없으면
거절한다. 다만 그 판정은 Spark 안에서, claim 뒤에서만 돌았다. 컷오버에는 같은 성격의 준비 단계가 이미 하나 있다: `history`
(ADR-0350 §6)는 실거래 이력이 다 적재되지 않은 레인을 claim 없이 `waiting`(exit 75, 알림 없음)으로 끝낸다.

큰 곳의 방식: Kubernetes 는 Pod 의 readiness/init 조건이 맞지 않으면 노드 자원을 잡지 않고 Pending 으로 둔다. Airflow 의
sensor(`reschedule` 모드)는 기다리는 동안 worker 슬롯을 돌려준다. Dagster 의 declarative automation 은 상위 자산이 없으면
`missing` 으로 실행을 만들지 않는다. 공통점은 **입력이 준비됐는지는 자원을 잡기 전에, 싸게** 본다는 것이다.

## Decision

1. **`inputs` 단계.** 컷오버 단계는 `history inputs preview gold ...` 이다. `inputs` 는
   `scripts/ops/gold_input_readiness.py <gold 표>` 를 운영 지도 읽기 토큰으로(`history` 와 같은 `operations-map.env`) 돌린다.
   Iceberg REST 카탈로그(iceberg_metadata.py)만 읽고 Spark·claim 은 없다.
2. **계획기와 같은 목록.** 기다릴 입력은 계획기가 거절하는 것과 정확히 같다: 생산자의 필수 입력
   (`gold_rebuild.producer_inputs`, ADR-0130 생산자가 입력 목록의 주인)과, 계약이 없어도 된다고 하지 않은 선택 입력. 두 번째
   목록을 만들지 않는다. 생산자가 모듈 수준에서 Spark 를 가져오는 경우(패널 생산자)는 Spark 없이 목록을 알 수 없으므로
   `ready` 라고 그렇게 말하고, 판정은 지금처럼 gold 단계의 계획기가 한다(지금 기다리는 레인은 모두 자기 생산자 종류라 해당 없음).
3. **종료 코드.** `ready` 0, `waiting` 75(EX_TEMPFAIL), 알 수 없음 65. 기다림을 1 로 두지 않는다: 잡히지 않은 Python 오류도 1
   이라, 고장이 영원한 `waiting` 으로 숨는다(로컬 시험에서 문법 오류의 fake 가 실제로 그렇게 읽혔다). 75 만 `waiting` 으로
   기록하고 끝나며(exit 75, 유닛은 성공, 알림 없음), 나머지는 실패해서 알린다.
4. **자동 승격의 백오프.** 마지막 시도가 `waiting` 으로 끝났으면(그 시도의 mark 이후에 시작한 run 의 그 레인 마지막 단계가
   `waiting`) 백오프 없이 다음 틱(15 분)에 다시 묻는다. 그 시도는 아무것도 잡지 않았고, 기다리는 것은 스스로 도는 적재다.
   두 배로 늘어나는 대기(1·2·4·… 시간)는 마지막 입력이 들어온 뒤에도 레인을 몇 시간 붙잡는다. `history` 로 기다리는 레인도
   같은 규칙을 받는다. 순서는 그대로 시도 횟수가 적은 레인이 먼저라, 자주 기다리는 레인이 다른 레인을 막지 않는다.

## 버린 대안

- **gold 단계의 거절 문구를 `waiting` 으로 분류.** 알림은 멈추지만 claim 과 Spark 대기(18:42Z 의 38 분+)는 그대로다.
- **자동 승격이 직접 준비를 판정.** 손으로 시작한 컷오버(`by-pnu-pack-operator.sh <lane> cutover`)도 같은 함정에 빠진다. 단계에
  두면 둘 다 받고, 증거 파일(`cutover/*.jsonl`)에 이유가 남는다.
- **계약에 레인별 "필요한 Silver" 목록을 새로 적기.** 생산자 상수와 갈라질 두 번째 정본이다(ADR-0130).
- **Spark 로 계획기 dry-run.** 그것이 바로 claim 을 요구하는 일이다.

## Consequences

- 입력이 없는 레인의 컷오버: claim + Spark 대기(잰 값 46 s ~ 38 분+) + 실패 알림 → claim 없음, REST 읽기 몇 번(1 초 안팎, 계산값),
  알림 없음. 입력이 들어온 뒤 첫 시도까지: 최대 백오프(첫 배포 뒤 1·2·4 시간) → 최대 15 분(계산값).
- 입력이 다 있는 레인은 단계가 하나 늘 뿐 같은 길을 간다.
- 실거래처럼 `history` 로 오래 기다리는 레인은 15 분마다 짧은 run 이 하나씩 생긴다(mark 한 줄, 증거 파일 하나, 유닛 수 초).
  비용 0.
- 시험: `orchestration/tests/test_gold_input_readiness.py`(계획기와 같은 목록·허용된 선택 입력은 기다리지 않음·카탈로그 없으면 65),
  `test_by_pnu_lane_cutover.py`(입력 없는 레인은 75 로 끝나고 claim·유닛·미리보기 요청이 없음, 실제 검사가 알 수 없으면 실패),
  `test_by_pnu_auto_promote.py`(`waiting` 뒤에는 백오프 없음, 그 시도의 run 이 아니면 두 배 백오프 유지).
