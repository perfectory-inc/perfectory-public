# ADR 0365: Spark 런타임 시험은 변경이 닿는 파일만, 병렬 러너 여러 대에 나눠 돈다

- Status: Accepted
- Date: 2026-10-31
- Builds on: [ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md)(PR·큐는 바뀐 것만, 밤에 전체), [ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(일 없는 잡은 러너를 잡지 않는다)
- Amends: Foundation CI 의 `spark-runtime` 잡이 "선택되면 모든 런타임 시험 파일을 한 러너에서" 돌던 것. 게이트 선택(`foundation_ci_scope.py`)과 `required/foundation` 은 그대로.

## Context

`Spark and Iceberg runtime tests` 는 Foundation CI 에서 가장 긴 잡이 됐다. Silver 레인이 하나 늘 때마다 시험
파일이 늘고(2026-10-30 기준 46개), 한 러너(4 CPU, 2개씩)에서 전부 돈다.

| 측정 (2026-10-29~30, Foundation CI) | 값 |
|---|---|
| 성공한 잡 벽시계 | 11.4–19.9분 (37회, 중앙값 16.4분, p90 19.8분) |
| 시험 벽시계(로그 `wall`) | 빠른 러너 689 s, 느린 러너 1125–1137 s |
| 20분 timeout 취소 | 6회(20.0–20.3분); 10-30 하루에 #971 큐·#973·#974 PR |
| 가장 긴 파일 | `test_gold_incremental_iceberg.py` 447–781 s — 혼자 꼬리 |

PR 은 바뀐 경로가 Spark 게이트에 걸리면 46개를 전부 돌렸다. 공매 사진 한 군데를 고친 #881 도 그랬다.
timeout 만 올리는 것은 고침이 아니다: 레인이 늘면 다시 닿고, PR 하나가 20분을 기다린다(목표 1크레이트 PR
≤ 10분, 큐 ≤ 20분). 비용은 0 이어야 한다(표준 무료 러너, 유료 캐시 없음).

큰 프로젝트의 방식: 영향 받은 시험만 고르고(Bazel/Buck 타깃 결정, Chromium `analyze`), 남은 것은 측정한
시간으로 균형 잡아 병렬 샤드에 나눈다(pytest-split·Knapsack·CircleCI `tests split --split-by=timings`:
저장소에 둔 시간표, 모르는 파일은 평균값).

## Decision

1. **고르기.** PR 과 머지 큐(ADR-0313 의 좁히는 이벤트)는 그 변경이 닿는 런타임 시험 파일만 돈다. 닿음은
   Python·Node 스위트가 이미 쓰는 의존 추적(`change_scope.py suites`: 바뀐 파일을 import 하거나 따옴표로
   읽는 시험)을 그대로 쓴다 — 새 선택기를 만들지 않는다. 다음은 전부 돈다: 다른 이벤트(밤 전체 실행, 수동,
   큐가 검증 안 한 main 푸시), 비교 불가, run-all 입력(`foundation_ci_scope.RUN_ALL_RULE`), 런타임 자체나 이
   선택의 입력(`foundation_ci_scope.RULES["spark-runtime"]["exact"]`: 이미지·엔진 계약·러너·샤드 계획·시간표),
   스위트 선택이 `all` 이거나 읽을 수 없을 때.
2. **나누기.** 잡은 4샤드 matrix(`fail-fast: false`)다. `scripts/ci/foundation_spark_shards.py` 가 고른 파일을
   `scripts/ci/foundation-spark-runtime-durations.json`(잡 로그의 "durations, slowest first" 표에서 옮긴 초)으로
   긴 것부터 가장 덜 찬 샤드에 넣는다(LPT). 시간표에 없는 파일은 중앙값. 한 슬롯의 고른 몫(합 ÷ 샤드 × 2)과
   60 s 를 둘 다 넘는 파일은 테스트 클래스로(클래스가 하나면 테스트 메서드로) 쪼갠다 — 각 조각은 자기 JVM 으로
   `python -m unittest module.Class[.method]`. 클래스가 모두 `TestCase` 를 직접 상속하지 않거나 모듈이
   `load_tests` 를 가지면 쪼개지 않는다(상속된 시험을 놓칠 수 있으므로).
3. **러너.** `foundation-spark-runtime-files.sh` 는 대상 목록을 받으면 그것만 그 순서로 돈다. 없으면 예전처럼
   전부(로컬 실행). 런타임 시험 파일의 정의(skip 사유 정규식)는 이 스크립트 한 곳이고, 계획기는 `--list` 로
   묻는다. 런타임을 원해 skip 된 시험은 여전히 잡을 실패시킨다.
4. **필수 체크.** 필수 체크는 `required/foundation` 하나이고 이름은 그대로다(`tools/github/main-ruleset.json`).
   matrix 잡의 `needs.spark-runtime.result` 는 샤드 하나라도 실패하면 실패다. 샤드 잡 이름은
   `Spark and Iceberg runtime tests (shard N)`. 할 일 없는 샤드는 계획만 찍고 성공한다.
5. **timeout 20분은 그대로 둔다** — 이제 예상(4–6분)의 3배가 넘는 안전 여유이고, 고침은 1·2 다.
6. **시간표 갱신.** 새 파일은 중앙값으로 계산되므로 갱신은 균형이 눈에 띄게 틀어질 때(샤드 하나가 나머지의
   1.5배) 로그 표를 옮겨 한다. 갱신은 이 선택의 입력이라 전 파일을 돌린다.

## Consequences

- 예상(시간표 기준, 러너 준비·이미지·Ivy 약 1분 포함): 전체 실행 샤드당 시험 약 170 s(빠른 러너)–285 s(느린
  러너), 잡 4–6분(전: 중앙값 16.4분, p90 19.8분, timeout 6회). #881 같은 좁은 변경은 6조각, 샤드당 시험 약 25 s, 잡 약
  2분. 바닥은 가장 긴 조각(gold_incremental 메서드 하나 약 110–200 s)이다.
- 러너 수: 선택되면 4대(이전 1대). 비용 0(공개 저장소 표준 러너). 동시 한도 20(ADR-0324) 안에서 3대를 더 쓴다.
- 위험: 의존 추적이 못 보는 결합(동적 import, 이름으로 열지 않는 파일)은 PR·큐가 놓치고 매일 밤 전체 실행이
  잡는다 — ADR-0313 이 스위트에 대해 받아들인 것과 같은 위험이다.
- 위험: 메서드로 쪼갠 파일은 메서드마다 새 Spark 세션에서 돈다. 메서드끼리 순서에 기대는 시험은 여기서
  드러난다(그런 시험은 원래 `unittest` 의 이름순 실행에 기대고 있었다).
