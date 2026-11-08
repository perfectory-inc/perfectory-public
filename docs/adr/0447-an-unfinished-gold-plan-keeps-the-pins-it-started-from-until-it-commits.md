# ADR 0447: 끝나지 않은 Gold 계획은 커밋할 때까지 시작한 핀을 지킨다 — 새 Silver 머리가 계획 이름을 바꿔 쌓은 단계를 지우지 않는다

- Status: Accepted
- Date: 2026-11-08
- Amends: [ADR-0438](./0438-an-incremental-gold-merge-keeps-its-stages-under-its-plan-so-a-killed-run-resumes-and-a-refused-plan-is-refused-at-once.md)(계획 이름 = 핀·코드), [ADR-0440](./0440-a-whole-gold-build-longer-than-its-unit-is-built-one-sido-at-a-time-and-resumes-from-the-shards-it-staged.md)(시도 샤드로 잇는 전량 빌드)
- Builds on: [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md)(Silver 핀), [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분), [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md) §5.2(기록한 것부터 잇기)
- 관련 사건: W20 발견 (b) "중단된 Gold 재생성이 약 2시간을 잃는다"의 남은 구멍, 2026-11-08 측정

## Context

ADR-0438 은 증분 병합의 단계를 계획 이름(`gold_incremental.plan_key`: 표·**Silver 핀**·이전 핀·코드 판·표본 경계의 digest)
아래 두었다. ADR-0440 은 유닛 시간 상한보다 긴 전량 빌드를 시도 샤드로 쌓아, 죽은 run 은 쌓은 샤드부터 잇게 했다. 둘 다
**다음 run 이 같은 핀으로 계획할 때만** 잇는다. 그런데 계획기(`gold_rebuild.plan`)는 run 마다 핀을 각 입력의 **지금 머리**로
정한다(`pins = {name: silver[name]["head"]}`). 그리고 `Stages` 는 같은 표의 다른 이름 계획을 "죽은 run 의 것"으로 지운다.

실측(읽기 전용, ai-server, 2026-11-08 06:10Z):

| 무엇 | 측정값 |
|---|---|
| `gold.parcel_panel` 샤드 run(02:35Z, ADR-0440 첫 적용) | 160분 상한에 05:15Z 종료. 쌓은 것 `full-keys`·`full-11`(서울) 하나, 492 MB |
| 그 run 의 핀 | 11-06 12:09Z·11-07 16:01Z·21:31Z run 과 **같다** — Silver 갱신이 W19 의 gzip 회귀로 멈춰 있어서 |
| `gold.parcel_panel` 입력 | Silver 10개 + reference 2개. 그중 `silver.land_transfer_history`(실거래)는 매일 append |
| 시도 샤드 수 | 17. 첫 샤드만 약 2시간 12분(입력 쌓기 포함, #1172 전 코드) |

지금 샤드가 이어지는 것은 **Silver 가 멈춰 있기 때문**이다. W19 의 수정이 배포되어 Silver 가 다시 매일 커밋하면, 다음 run 은
새 머리로 계획한다. 계획 이름이 바뀌고, `Stages` 가 어제의 샤드를 지우고, 처음부터 다시 쌓는다. 하루에 몇 샤드밖에 못 쌓는
빌드는 **영원히 끝나지 않는다**(11-03..06 의 라이브락이 이름만 바꿔 돌아온다). 같은 일은 ADR-0438 의 증분 단계에도 생긴다.

스트리밍과 배치 엔진은 이것을 "입력 범위를 먼저 적고, 재시작하면 같은 범위를 다시 처리한다"로 푼다. Spark Structured
Streaming 은 배치를 처리하기 전에 그 배치의 끝 오프셋을 offset log 에 적는다. 재시작한 run 은 새 데이터가 있어도 **적힌
범위로 그 배치를 다시 실행하고**, 그 다음 배치에서 따라잡는다. Flink 의 checkpoint 는 소스 위치와 상태를 함께 저장한다.
Iceberg 를 읽는 쪽에는 스냅숏 ID 가 그 위치다.

## Decision

1. **계획은 자기 핀을 자기 디렉터리에 적는다.** `build_or_merge` 는 계획 이름을 정하자마자
   `gold-resumable-<table>.<digest>/pins.json` 에 그 핀을 적는다(`record_plan_pins`). 커밋(`stages.remove`)과 관문 거부
   (`ValueError` → `remove`)는 디렉터리째 지운다. 병합 거부(`refuse`)는 ADR-0440 대로 같은 계획의 전량 빌드로 이어지므로
   거부 표식과 함께 핀도 남긴다.
2. **계획기는 끝나지 않은 계획의 핀을 지킨다.** `gold_rebuild.main` 이 `spark.local.dir` 에서 그 표의 `pins.json` 을 읽고
   (`plan_in_progress`), `plan` 은 다음을 모두 만족할 때만 머리 대신 그 핀으로 계획한다(`resumable_pins`):
   - 기록이 이번 run 의 입력 집합과 같다(입력이 늘거나 줄면 다른 빌드다);
   - 핀마다 그 스냅숏이 아직 표의 기록에 있다(만료된 스냅숏은 읽을 수 없다);
   - 다른 Gold 를 따르는 핀(`pins_from`)이 그 Gold 의 지금 핀과 같다(ADR-0190·0193 의 같은 판 규칙);
   - Gold 가 그 핀을 이미 커밋하지 않았다(커밋한 계획의 디렉터리는 남은 찌꺼기다. 분할하지 않은 전량 빌드는 커밋해도
     디렉터리를 지우지 않는다).
   계획(`plan.json`)과 로그 줄 `gold-rebuild-plan` 은 `resumed_plan_pins` 에 머리와 다른 핀을 적는다.
3. **따라잡기는 다음 run 의 증분이다.** 지킨 핀으로 커밋하면 디렉터리가 지워지고, 다음 run 은 머리로 계획한다. 그 사이
   커밋된 Silver 는 ADR-0180 의 증분(바뀐 PNU 만)으로 들어온다. 전량을 다시 하지 않는다.
4. 코드 판이 바뀌면 계획 이름이 바뀌고 단계는 지워진다(ADR-0438 그대로: 다른 코드는 다른 행을 만든다). 그래도 핀은 지킨다.
   새 코드로 처음부터 쌓되, 같은 핀으로 쌓아 다음 run 들이 이어 갈 수 있게 한다.
5. 새 스위치는 없다. 기록은 단계가 이미 사는 디스크(`/scratch`, 계획기와 생산자가 같이 마운트)에 있고, 계약·작업 정의는
   바뀌지 않는다.

## Consequences

- 시도 샤드로 쌓는 parcel 패널 전량 빌드는, Silver 가 매일 움직여도 run 마다 쌓은 샤드부터 잇는다. 커밋까지 걸리는 run 수는
  "샤드 총시간 ÷ 160분"이고 입력 커밋 빈도와 무관하다.
- 그동안 서빙되는 Gold 는 계획을 시작한 때의 핀까지다. 늦어지는 만큼은 다음 run 의 증분이 메운다. 끝나지 않는 계획보다
  하루 늦은 커밋이 사용자에게 낫다(사장 원칙: 보여 주는 것이 이긴다).
- 비용(계산): 쓰는 파일은 계획마다 수백 바이트 JSON 하나다. 읽기는 계획기가 `/scratch` 를 한 번 glob 한다.
- 위험: 같은 지점에서 결정적으로 죽는 계획(같은 샤드에서 매번 OOM)은 핀을 지키는 동안 같은 자리에서 반복된다. 이것은 지금도
  핀이 안 움직이면 똑같다. 상한에 멈춘 Gold 는 ADR-0435 b·ADR-0440 대로 1시간 뒤 다시 시도하고, 실패는 유닛 알림(ADR-0445)이
  주인을 부른다.
- 시험: `test_an_unfinished_plan_keeps_its_pins_until_it_commits`(머리가 움직여도 지킨다; 이미 커밋함·만료된 핀·다른 입력
  집합이면 머리), `test_a_plan_records_its_pins_for_the_next_run_and_keeps_them_through_a_refusal`.
