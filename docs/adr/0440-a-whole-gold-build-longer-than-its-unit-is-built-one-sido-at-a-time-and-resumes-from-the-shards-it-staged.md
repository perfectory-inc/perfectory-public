# ADR 0440: 유닛 시간 상한보다 긴 전량 Gold 빌드는 시도 하나씩 만들고, 쌓아 둔 샤드부터 잇는다

- Status: Accepted
- Date: 2026-11-07
- Builds on: [ADR-0438](./0438-an-incremental-gold-merge-keeps-its-stages-under-its-plan-so-a-killed-run-resumes-and-a-refused-plan-is-refused-at-once.md)(계획 이름 아래의 단계),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(바뀐 PNU 의 입력만 줄여 다시 만든다),
  [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md) §5.2(걸음마다 기록하는 이행),
  [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 승격, 사람 단계 0)

## Context

2026-11-07 자동 승격이 `parcel-regulation-zones` 의 첫 cut-over 를 시작했다. Gold 표가 없어 전국 전량 빌드다. 아래는 ai-server
run 로그에서 잰 값이다.

| 시각(UTC) | 무슨 일 |
|---|---|
| 07:42 | 첫 시도. 서빙 필지 39,396,906 개의 폴리곤을 순수 Python 으로 잘라 규제구역과 겹친다(`zone_overlap`) |
| 07:47–08:45 | 필지 모양 단계 62개 task 중 18개 완료(task 당 15–36분, 8 슬롯) |
| 08:58 | 메모리 저장소가 힙을 채워 JVM 이 멈춤 → Spark 종료(#1132 가 DISK_ONLY 로 고침) |
| 10:27 | 두 번째 시도(#1132 배포 뒤) |

모양 단계만 62 task ÷ 8 슬롯 × 평균 약 25분 ≈ 200분(계산)이다. 그 뒤에 구역 조각(200 task)과 셀별 겹침 단계가 남는다.
유닛 상한은 160분이다(`foundation-gold-panel-rebuild.service`, cut-over 의 gold 단계가 그대로 빌린다). 전량 빌드는 끝에
한 번 커밋한다. 상한에 죽은 run 은 다음 run 에서 처음부터 다시 하고 같은 자리에서 다시 죽는다. 백오프(ADR-0435)로는
끝나지 않는다.

## Decision

1. **레인이 전량의 키를 내면 전량은 샤드로 만든다.** `gold_incremental.Lane.full_shard_keys(frames)` 가 전량 빌드가
   행을 만드는 키(`pnu`) 프레임을 낸다. `build_in_shards` 는 키의 앞 `FULL_SHARD_DIGITS`(2, 시도) 자리마다 다음을 한다.
   `lane.restrict(frames, 그 키)` 로 입력을 줄이고, `lane.build` 한 뒤 그 키로 자른다. 증분 병합이 키 일부를 다시
   만드는 것과 같은 함수이고 같은 성질이다. 증분과 전량의 동등성은 레인마다 이미 시험한다.
2. **샤드는 계획의 단계다.** 각 샤드는 ADR-0438 의 계획 디렉터리(`Stages(spark, plan_key)`)에 `full-<시도>` 로 쌓인다.
   같은 계획(표·핀·이전 핀·코드 판·표본 한도)의 다음 run 은 `_SUCCESS` 가 있는 샤드를 되읽고, 나머지만 만든다.
   증분이 거부된 표시(`refused.txt`)도 같은 디렉터리에 있다. 그래서 "거부 → 전량 → 상한에 죽음" 이 다음 run 에서
   "바로 전량 → 남은 샤드만" 이 된다.
3. **Gold 는 쌓인 샤드의 합이다.** 검사(`validate_full`, 행 바닥, 단면 범위)와 커밋은 지금과 같다. 검사가 거부하면
   (`ValueError`) 샤드를 지운다. 같은 입력은 같은 거부를 낳기 때문이다. 커밋 뒤에는 `Outcome.release` 가 지운다. 그 밖의
   예외와 kill 에서는 남긴다.
4. **켜는 레인.** `parcel-regulation-zones` 하나다. 다른 레인의 전량은 유닛 상한 안에 든다(소유 1,449–2,109 s, 공매
   270–488 s, 잰 값). 새 레인의 전량이 상한의 절반을 넘으면 같은 한 줄(`full_shard_keys=`)로 켠다.

5. **계획 디렉터리의 범위는 표다.** `plan_key` 는 `<표>.<digest>` 다. 새 계획은 같은 표의 다른 계획만 지운다.
   ADR-0438 처음 구현은 모든 표의 계획을 지웠다. 그러면 죽은 표의 두 run 사이에 도는 매시 패널 작업·kinds 작업이 그
   표의 샤드를 지워, 이어 쓰기가 성립하지 않는다(`all` 에서 parcel 다음 building 도 같다).

## Consequences

- 규제구역 첫 빌드는 run 한 번에 끝나지 않아도 run 마다 앞으로 간다. 가장 큰 시도 하나가 상한 안에 들면 언젠가
  끝난다. 경기·전남·경북은 각각 필지의 약 1/8 이다. 전체가 약 5시간이라면 샤드 하나에 약 40분이다(계산).
- 샤드마다 입력을 다시 읽는다(디스크에 둔 필지, 구역의 상자 조인). 열일곱 번이고, 겹침 계산 자체보다 작다.
- ADR-0438 의 "전량은 이어 쓰지 않는다" 와 그 이유(nullable)는 틀렸다. Gold 열은 NOT NULL 이 없다. 그 ADR 에 개정
  각주를 달았다.
- 가드는 `test_the_whole_build_in_shards_equals_the_whole_build_and_resumes_from_the_shards_it_finished`(실제 Spark)다.
  샤드 합은 전량과 같다. `_SUCCESS` 를 지운 샤드만 다시 만들고, 나머지와 키는 되읽는다.

## Rejected

- **유닛 상한을 늘린다.** 이 레인만을 위해 모든 패널 재생성의 상한을 올리게 된다. 배포나 kill 로 죽은 run 은
  여전히 처음부터다.
- **샤드마다 Iceberg 에 커밋한다(load branch).** 샤드 커밋이 표의 스냅숏을 열일곱 개 만든다. 키 게시(WAP)와
  fingerprint, 변경 기록을 다시 짜야 한다. 로컬 Parquet 단계는 이미 증분 병합이 쓰는 방식이고, 커밋은 한 번으로 남는다.
- **겹침을 Apache Sedona 나 GEOS 로 옮긴다.** 올바른 장기 방향이다(대기업은 공간 조인을 Sedona·BigQuery GIS 로 한다).
  그러나 결과 동등성 시험과 이미지 변경이 함께 와야 하는 별도 결정이다. 이 ADR 은 길이가 얼마든 끝나게 만든다.
