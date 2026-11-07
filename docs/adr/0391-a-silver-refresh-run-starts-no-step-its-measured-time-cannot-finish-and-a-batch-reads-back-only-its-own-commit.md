# ADR 0391: Silver 새로 고침 run 은 잰 시간으로 끝낼 수 없는 단계를 시작하지 않고, 묶음은 자기 커밋만 되읽는다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)(비교는 묶음마다, 이어하기), [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(load branch 에 쌓고 한 번에 발행), [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) H(run 당 상한)
- Amends: `silver_refresh.rs` 의 `LARGEST_BUCKET_MINUTES = 40`. 이 값은 모든 레인에 하나였고, 잰 값이 아니었다(`silver.land_use_plan` 41 묶음 추정). 측정한 레인은 계약의 `run_budget` 을 쓴다.

## Context

Silver 새로 고침 유닛(`foundation-silver-refresh@.service`)의 한계는 150분이다. 세 레인이 그 안에 들지 않았다(읽기 전용
journal·Trino `$snapshots` 측정):

| 레인 | 측정 | 무엇이 시간을 썼나 |
|---|---|---|
| `building-register-apartment-price` | 10-20 04:10 시작, 06:40 timeout. 다음 run 이 이어서 12:54 에 끝 | 40M 행 묶음 하나(Spark 시작 → write-ok)가 1,120 → 1,340 → 1,702 → 1,979 → 2,774 s 로 자랐다. 커밋 시각과 비교하면 쓰기는 매번 9–13분이었다. **되읽기**만 8 → 13 → 18 → 24 → 33분으로 자랐다. 2M 행 마지막 묶음도 쓰기 1분, 되읽기 24분 |
| `land-transfer-history` | 10-21 timeout(ADR-0318 전). 11-03 첫 run 7,415 s(82%): 마감(110분)에서 멈추고 다음 run 이 3,786 s 에 끝냄 | 비교 묶음 `"41"` 하나가 2,779 s(시작 05:05:07 → 05:51:26). 기본값이 가정한 40분보다 길다. 110분에 시작했다면 150분 kill 에 걸렸다 |
| `building-register-unit-areas` | 10-20 한 run 8,574 s(95%) | 내보내기 22분, 그다음 Spark 한 번 7,246 s(PNU null 비율 검사 386 s, 쓰기, 되읽기). 한 run 이 load 하나(`load.unit = run`)라서 그 Spark 가 쪼개지지 않는다 |

**원인 1 — 되읽기가 쌓인 branch 전부를 읽었다.** `run_batched_input` 은 쓴 뒤 그 묶음을 검증한다. 그런데 검증이
ref 전체를 (source_snapshot_id, ingested_at_utc) 쌍으로 걸러 읽었다. 한 판의 묶음은 모두 같은 load branch 에 쌓인다.
그래서 k 번째 묶음은 앞의 k−1 묶음까지 읽었다. 판 전체 비용이 묶음 수의 제곱으로 컸다.

**원인 2 — 마감이 잰 값이 아니었다.** 마감(`LAST_BUCKET_START_MINUTES`)은 "가장 큰 묶음 40분"이라는 레인 공통 가정에서
나왔다. 실제로는 2,779 s 인 묶음이 있었다. 또 마감은 비교 묶음만 막았다. 부분 레인과 토지 레인의 load 묶음은 마감
없이 계속 시작됐다. 공동주택가격의 10-20 timeout 이 그것이다. 마감이 지난 뒤에도 비교 Spark 는 떴다. 떠서 양쪽을
읽고 나서야 시간이 없다고 알았다.

## Decision

1. **묶음은 자기 커밋의 파일만 되읽는다.** 이 run 이 append 한 묶음은 ingest 등록부(`read_ingested_objects`, 그
   커밋 요약이 객체를 적는다)에서 자기 snapshot 을 찾는다. 그리고 그 커밋만 Iceberg 증분 읽기
   (`start-snapshot-id`/`end-snapshot-id`, 기존 `read_recorded_append`)로 읽는다. 한 커밋이 아닌 기록은 거부한다.
   overwrite 된 묶음과 이미 있던 묶음은 예전 쌍 되읽기를 쓴다. 이 방식은 Iceberg 의 incremental append scan,
   Delta 의 `readChangeFeed`(커밋 범위만 읽기)와 같다.
2. **레인 계약이 잰 가장 긴 단계를 적는다.** `silver_refresh.run_budget`:
   - `step_seconds_max`: load 묶음이든 비교 묶음이든 한 단계가 걸린 가장 긴 시간.
   - `measured`: 언제, 어디서, 어떻게 쟀는지.

   run 은 이 시각 뒤에 새 단계를 시작하지 않는다: 시작 + (150분 × 90% − `step_seconds_max`). 그래서 마지막으로
   시작한 단계도 유닛의 90% 안에서 끝난다. 90% 는 #988 이 rt-molit Silver 적재에 둔 여유이고, ADR-0386 이 모든
   작업에 거는 기준이다.

   parse 가 거부하는 것:
   - 측정이 빈 예산;
   - 0 인 단계;
   - 단계를 시작할 시간이 30분보다 적게 남는 예산.

   이때 답은 단계를 작게 하는 것(묶음을 더 잘게, 묶음 접두사를 더 길게)이다. 유닛을 늘리는 것이 아니다.
   `run_budget` 이 없는 레인은 지금의 기본값(110분)을 그대로 쓴다.
3. **load 묶음도 마감을 지킨다.** 부분 레인(`refresh_parts`)과 토지 레인(`land::load`)은 마감 뒤 새 load 묶음을
   시작하지 않는다. 그때 run 은 `outcome=unchanged reason=load_resumes_next_run` 으로 끝난다. 쌓인 묶음은 branch 에
   남는다. 다음 run 은 그것을 다시 읽지 않고 나머지를 쌓는다. 이 이어하기는 이미 있던 장치다(`already staged on …;
   not read again`). `main` 은 판이 다 쌓이고 비교되기 전까지 바뀌지 않는다.
4. **마감이 지난 뒤에는 비교를 띄우지 않는다.** 판을 다 쌓았을 때 이미 마감이 지났으면, 비교 Spark 를 띄우지 않고
   다음 run 으로 넘긴다(`compare_resumes_next_run`).
5. **잰 값.** 바뀐 뒤 실측이 아니라 바뀌기 전 가장 나쁜 값을 적는다. 고친 뒤 판이 한 번 들어오면 다시 재서 줄인다.

   | 레인 | `step_seconds_max` | 근거 | 마지막 단계 시작 |
   |---|---|---|---|
   | `building-register-apartment-price` | 2,774 | 10-20 다섯째 묶음, 되읽기가 커진 값 | 5,326 s (88.8분) |
   | `land-transfer-history` | 2,805 | 11-03 묶음 `"41"` 시작 → run 끝 | 5,295 s (88.3분) |
6. **전유공용면적은 이 결정으로 들지 않는다.** 이 레인의 한 단계는 Spark 한 번 7,246 s 다. 그러면 시작에 854 s 만
   남고, 2항의 parse 가 거부한다. 합성 시험이 바로 이 값으로 그 거부를 증명한다. 이 레인을 맞추려면 load 단위를
   바꿔야 한다. 내보내기를 R2 부분 + manifest 로 하고(`HubParts`, 총괄표제부 ADR-0357 과 같다) `load.unit` 을 부분으로
   한다. 그러면 load 가 부분 묶음마다 쌓이고 이어진다. 그것은 다음 PR 이다. 그때까지 이 레인은 ADR-0386 의 열린
   예외로 남는다. 다음 판(2026-11 월 hub 판)이 측정한다. 4항 덕분에 그 판의 첫 run 은 쌓기만 하고 비교는 다음
   run 이 한다.

## Consequences

- 공동주택가격 한 판의 묶음 비용이 일정해진다: 쓰기 9–13분 + 자기 묶음 되읽기. 10-20 첫 묶음의 되읽기는 8분이었다.
  6묶음은 계산으로 약 110–130분이다(예전 약 214분, 측정 합). 한두 run 에 끝나고, 어느 run 도 유닛의 90% 를 넘기지
  않는다.
- 토지이동이력의 첫 run 은 88분 뒤 새 묶음을 시작하지 않는다(예전 110분). 계산상 최악 끝은 5,295 + 2,805 = 8,100 s
  로, 유닛의 90% 이다. 대신 판 하나가 run 셋에 걸칠 수 있다. 바뀐 키만 append 하는 비교는 이어하기라서 손실이 없다.
- `run_budget` 이 없는 열 레인은 예전 그대로다. 그 레인들도 재서 적는 것이 다음 일이다. ADR-0386 의 가드가 그 레인들의
  run 시간을 본다.
- 되읽기 검증의 내용(행 수·품질 지표가 쓴 것과 같은지)은 같다. 읽는 범위만 그 커밋으로 줄었다.

## Alternatives considered

- **유닛을 늘린다(150 → 240분).** 기각했다. 예약 접기가 굶주림 상한을 넘는다(ADR-0171 §5). 되읽기는 판이 커질수록
  계속 자라고, 다음 판에서 다시 닿는다.
- **되읽기를 끈다(`--defer-iceberg-readback-validation`).** 기각했다. 쓴 행 수와 품질을 확인하지 않는다.
- **마감을 레인 공통으로 더 일찍 당긴다.** 기각했다. 빠른 레인까지 판 하나를 여러 run 에 나누게 된다. 잰 값은 레인마다 다르다.

## 덧붙임 (2026-11-04): 전유공용면적은 부분으로 적재한다

결정 6항이 남긴 레인이다. 이 레인을 [ADR-0357](./0357-the-building-register-master-lands-in-silver-as-the-site-total.md)의 부분 레인과 같은 방식으로 바꾼다. 새 장치는 없다:
- R2 부분 + manifest(`HubManifest`, `validate_manifest`);
- `refresh_parts` 의 묶음 쌓기와 이어하기;
- 부분 하나 = 적재 한 단위(`load.unit = object`, 열 `source_part_id`).

1. **내보내기.** 전유공용면적 내보내기의 정규화는 그대로 둔다: 두 번 읽기, 자연 키 순위(ADR-0331), PNU 조립. 출력 방식에
   `r2-parts` 를 더한다. 행 100만 개(`export_chunk_rows`)마다 gzip JSONL 부분 하나를
   `silver-handoff/hubgokr__building_register_exclusive_common_area/<ZIP 이름>/attempt=<id>/part-NNNN.jsonl.gz` 에 쓴다.
   마지막 부분 뒤에 manifest 를 쓴다. 각 행은 자기 부분의 키를 `source_part_id` 로 갖는다. 이 키는 행 체크섬에 들어가지
   않는다: 같은 판이면 다른 부분에 실려도 같은 행이다. 원천 줄 하나가 행 하나이고, manifest 의 행 수가 그것을 맞춘다.
2. **표.** `silver.building_register_unit_areas` 에 `source_part_id` 를 더한다. 적재 열은 모두 필수다
   (ADR-0069, 가드 `every-table-declares-its-load`). 통째로 적재한 판은 부분 하나이고, 그 이름은 실행 이름
   `source_snapshot_id` 다. 그 표가 그 적재를 기록한 이름과 같다. 두 쓰기가 같은 값을 쓴다:
   - 배포의 `migrate`(ADR-0124)가 2026-10 까지의 행을 그 값으로 채운다. 등록된 backfill
     `building_register_unit_areas_silver.whole_run_part_column` 이다.
   - 통째로 쓰는 내보내기(JSONL·Parquet)도 같은 값을 쓴다.

   이 backfill 은 표 전체(114,461,830 행, 8.4 GB)를 **한 번** 다시 쓴다. 선언된 예외다: 필수 열은 비워 둘 수 없다.
   비용은 계산으로 약 1–1.5시간이다. 10-20 같은 표 overwrite 쓰기가 83분이었다. 그 동안 배포의 `migrate` 단계가
   그 시간만큼 길어진다. 적재 단위는 run 에서 object(`source_part_id`)로 바뀐다. 이미 실린 판은 실행 이름(`silver-refresh-building-register-unit-areas-<월>-…`)으로 기록돼 있다. 그래서 부분
   레인의 판정 앞에 실행 이름의 월을 먼저 본다. 같은 월이나 더 새 월이 있으면 `unchanged` 다. 202610 판을 다시
   내보내지 않는다.
3. **적재와 비교.**
   - 부분 20개(2,000만 행)씩 load branch 에 append 한다. 마감(결정 2·3항) 뒤에는 새 묶음을 시작하지 않고, 다음 run 이 잇는다.
   - 비교 묶음은 PK 앞 세 자리다. 두 자리면 `"10"` 묶음 하나가 69,314,636 행이다. 세 자리면 28묶음이고 가장 큰
     `"100"` 이 16,553,022 행이다(2026-11-04 읽기 전용 Trino).
4. **잰 값.** `run_budget.step_seconds_max = 2805`. 가장 느린 단계는 비교 묶음이다. 같은 크기에서 잰 가장 가까운 비교는
   `silver.land_transfer_history` 묶음 `"41"` 이다(17,746,890 행, 11-03, 2,805 s). load 묶음 하나는 계산으로 약 1,640 s 다:
   10-20 한 run 적재의 Spark 6,843 s 를 2,000만 행 몫으로 자르고, 묶음마다 다시 하는 PNU null 비율 검사 386 s 와 시작 1분을 더했다.
   시험 `the_unit_areas_load_part_by_part_and_each_step_fits_the_unit` 이 이 계산이 예산 안이고, 예산이 유닛의 90% 안인지 본다.
   합성 실패 시험은 세 계약을 parse 가 거부하는지 본다:
   - 2026-10 모양(overwrite, 입력 묶음 0);
   - 부분 크기가 없는 계약;
   - prefix 가 없는 계약.

| | 전 (측정) | 후 |
|---|---|---|
| 한 단계 | Spark 한 번 7,246 s(10-20) | load 묶음 약 1,640 s(계산), 비교 묶음 ≤ 2,805 s(같은 크기 비교 측정) |
| 한 run | 8,574 s = 유닛의 95%. kill 되면 아무것도 남지 않음 | 마감 5,295 s 뒤 새 단계 없음. 최악 8,100 s = 90%(계산). 끊겨도 쌓인 부분·묶음은 남는다 |
| 한 판 | 1 run | 3–4 run(계산): 내보내기 약 25–40분 + 부분 115개(6묶음, 약 2.7시간) + 비교 28묶음(약 2.2시간). 다음 run 은 실패·인입 뒤 또는 매일 07:00 |

**남은 위험.**
- 한 판이 main 에 닿기까지 며칠이 걸릴 수 있다. 매일 1 run 이면 3–4일이다(판은 한 달에 하나).
- R2 부분은 판마다 약 115개, 몇 GB 다. 보관은 다른 hub 부분 레인과 같다.
- 비교 묶음 값은 이 표에서 잰 값이 아니다. 첫 판이 들어오면 다시 잰다.
- 병합 뒤 첫 배포의 `migrate` 가 위 backfill 로 1–1.5시간(계산) 길어진다. 그 배포는 굽기가 쉬는 창에 하는 것이 좋다.

