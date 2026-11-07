# ADR 0438: 증분 Gold 병합은 단계를 계획 이름 아래 두어, 죽은 run 은 끝낸 단계부터 잇고 거부된 계획은 바로 거부된다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분 병합),
  [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md) §5.2(기록한 판은 건너뛰는 이행),
  [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(코드 판),
  [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 승격, 사람 단계 0)

## Context

`gold_incremental.plan_merge` 는 단계마다(바뀐 PNU, 표본, 키, 줄인 입력, 다시 만든 행, 병합 원천) Parquet 을
`spark.local.dir` 에 쓰고 되읽는다(`Stages`). 디렉터리 이름은 run 마다 새 `uuid4` 이고, 다음 run 은 앞 run 의
디렉터리를 "죽은 run 의 것" 으로 지운다.

2026-11-05..06 `gold.parcel_panel`(ai-server journal, run 디렉터리, 잰 값):

| run | 단계 | 결과 |
|---|---|---|
| 11-06 12:11Z | affected 4,541 s → restrict 2,409 s → build 8 s → parity 531 s | parity 1/20,258 불일치 → 전량 폴백 |
| 14:50Z | 전량 빌드 도중 유닛 시간 상한 160분 | `start operation timed out`, 커밋 0 |
| 9회(11-05..06) | 같은 핀 = 같은 계획 | 매번 처음부터; 시간초과 4회·배포 정지 5회 |

같은 핀과 같은 코드로 다시 도는 run 은 앞 run 과 같은 단계를 같은 결과로 만든다(표본 seed 도 핀에서 온다).
그런데 매번 2시간을 다시 써서 같은 거부에 이르렀고, 남은 시간으로는 전량이 끝나지 않았다. 사람이 끼지 않으면
영원히 같은 자리에서 죽는다(ADR-0435 가 없애려는 사람 단계).

## Decision

1. **단계의 디렉터리는 계획의 이름이다.** `plan_key(table, pins, previous, code_version, max_fraction,
   sample_keys)` 의 sha256 앞 32자. 단계를 정하는 입력이 모두 들어 있다. 핀·이전 핀·생산자 코드 판(ADR-0321)·표본
   한도 중 하나라도 바뀌면 새 계획이다. `build_or_merge` 가 이름을 지어 `plan_merge(resume_key=...)` 에 넘긴다.
2. **끝난 단계는 다시 만들지 않는다.** 단계 디렉터리에 Spark 가 커밋한 표시(`_SUCCESS`)가 있으면 되읽는다
   (`gold-incremental-resumed <stage>` 를 로그에 남긴다). 표시가 없으면 죽은 쓰기이므로 통째로 다시 쓴다(`overwrite`).
   ADR-0317 §5.2 의 "기록된 판은 건너뛴다" 와 같은 모양이다. 단위는 판이 아니라 단계다.
3. **거부는 계획에 남는다.** `IncrementalRefusal`(바뀐 키 비율 초과, parity 불일치)이 나면 단계는 지우고 이유 한 줄
   (`refused.txt`)만 남긴다. 같은 계획의 다음 run 은 단계를 다시 만들지 않고 그 이유로 바로 전량 빌드로 간다.
   11-06 의 경우 2시간 대신 0초 만에 폴백하므로, 전량 빌드(실측 5,555 s)가 160분 안에 든다.
4. **지우는 것.** 성공하거나 검사에서 실패하면 지금처럼 지운다(`Outcome.release`, 검사 실패). 그 밖의 예외와 kill 은
   단계를 남긴다. 새 계획의 `Stages` 가 다른 계획의 디렉터리를 지우므로 디스크에는 많아야 한 계획의 단계가 남는다.
   지금과 같은 상한이다. 계획 없는 `Stages`(키로 게시, 전량 덮어쓰기의 차이)는 지금처럼 run 마다 새로 만든다.
   그 단계는 새 브랜치나 바뀐 main 에서 읽으므로 이어 쓰면 안 된다.

## Consequences

- 시간 상한이나 배포로 죽은 증분 run 은 다음 run 에서 끝낸 단계를 건너뛴다. 11-06 의 run 이라면 affected·restrict
  6,950 s 를 건너뛴다(계산).
- 같은 계획을 다시 도는 것만 이어 쓴다. Silver 가 움직였거나 코드가 바뀌었으면 처음부터 한다. 이 경우가 옳다.
- 전량 빌드(`lane.build(...).persist(DISK_ONLY)`)는 이어 쓰지 않는다. Parquet 으로 되읽으면 모든 열이 nullable 이
  되어, Iceberg 의 null 검사가 필수 열에 쓰기를 거부할 수 있다. 전량은 W20 의 ADR-0437 이 정한 키 게시(WAP)로 끝까지
  간다. 전량을 샤드 커밋으로 이어 쓰는 것은 그 실측이 상한을 넘을 때 따로 정한다.
- 가드는 `test_a_plan_killed_by_its_timeout_resumes_from_the_stages_it_finished` 다. 같은 계획은 같은 이름이고,
  핀이나 코드가 바뀌면 새 이름이다. 끝난 단계는 되읽고 죽은 쓰기는 다시 쓴다. 거부는 남고, 다른 계획의 디렉터리는
  지운다.

## Rejected

- **유닛 시간 상한을 늘린다.** 같은 거부에 이르는 2시간이 그대로 남는다. 배포로 죽은 run 도 처음부터다.
- **parity 불일치를 별도 파일(호스트 상태)에 기억한다.** 계획의 단계와 같은 곳·같은 이름에 두면 지우는 규칙이 하나다.
  계획이 바뀌면 기억도 함께 사라진다.
- **Spark `checkpoint`/`localCheckpoint`.** SparkContext 와 함께 사라지는 run 범위다. 다음 run 이 읽을 수 없다.
