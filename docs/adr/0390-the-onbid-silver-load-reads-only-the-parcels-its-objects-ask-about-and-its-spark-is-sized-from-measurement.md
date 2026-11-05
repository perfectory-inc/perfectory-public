# ADR 0390: 공매 Silver 적재는 그 객체들이 묻는 필지만 한 번 읽고, Spark 크기는 잰 값으로 정한다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md) §1(모든 필지 원천은 정본 해석기를 거친다), [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) H(run 당 상한), [ADR-0280](./0280-public-sale-court-auction-and-factory-registration-are-foundation-parcel-sources.md)(공매 Silver 적재)
- 같은 방식: #988(실거래 Silver 적재, 필지 읽기는 run 에 한 번)

## Context

`foundation-onbid-silver-load.service`(`onbid_public_auction_silver`, 40분 단위)는 10-31 부터 거의 매 run 이
죽었다. 서버 읽기 전용 실측이다.

| 실측 (ai-server, 2026-10-28 ~ 11-03) | 값 |
|---|---|
| 끝난 run (journal) | 성공 81, `status=137` 67, timeout(`status=15`) 8, `status=1` 1 |
| 11-01 ~ 11-03 | 62 run 중 57 run 이 137 (11-02 는 성공 0) |
| 커널 OOM (`journalctl -k`, 11-03 17:03:04) | `CONSTRAINT_MEMCG`, 컨테이너 2560m 한도에 붙음. java 상주 2,081 MiB + 스왑 760 MiB(힙 2g), 드라이버 python 393 MiB + 스왑 208 MiB |
| 물건목록 객체 하나 (8 표 `$snapshots` 837 커밋의 간격) | p50 482 s, p90 957 s, max 2,424 s (31 객체) |
| 그 밖의 객체 | p50 16 s, p90 54 s, max 563 s (702 객체) |
| `silver.parcel_boundaries` | 79,738,057 행, 14.76 GB, 29 파일(10-30 05:51 압축 `replace`) |

원인 둘.

1. **객체마다 전국 필지를 다시 읽었다.** `address_pnu.lakehouse_inputs` 의 `parcels` 는 게으른 DataFrame 이고, 물건목록
   객체마다 `resolve` 의 조인이 그것을 다시 계산했다. 계산 내용은 현재 필지 약 80M 행을 읽고 `distinct` 로 약 40M PNU 를
   모으는 것이다. 객체 하나 482 s 의 거의 전부가 이것이었다. 2 GB 힙 안에서 40M 문자열 집계가 반복되면 메모리가 남는다.
   그래서 run 이 물건목록 객체 한두 개를 커밋하고 죽었다(11-03 16:50 run: 16:54·17:02 커밋, 17:03 OOM). 필지 표가 10-30
   압축으로 500 MB 파일 29 개가 된 뒤부터 매 run 이 죽었다. 같은 실수를 실거래 적재가 10-31 에 고쳤다(#988).
2. **크기가 잰 값이 아니었다.** 스크립트가 `--driver-memory 2g` 를 줬다. 그러나 spark-small 컨테이너 전체가 2560m 이다.
   잰 OOM 기록으로는 JVM 이 힙 밖에 793 MiB, python 드라이버가 601 MiB 를 더 잡는다. 그래서 2 GB 힙은 처음부터 들어갈
   수 없었다.

run 에 마감도 없었다. 그래서 10-29·30 첫 적재 때 객체를 계속 시작하다 단위가 죽였다(timeout 8).

## Decision

1. **필지는 run 에 한 번, 묻는 것만.** 정본 해석기(`place_identity_resolver`)에 두 함수를 둔다.
   - `candidate_pnus(pnus, remap, ambiguous)`: 원천 코드마다 현재 법정동 코드로 옮긴 PNU 를 만든다. `resolve` 가 물을
     바로 그 값이다. 쪼개진 코드는 묻지 않는다.
   - `served_parcels_among(spark, catalog, pnus)`: 서빙 판의 현재 필지(`vworld_parcel_editions.served_rows`, ADR-0291 §5)를 그 PNU 들과 먼저 broadcast semi-join 하고 나서
     중복을 지우고 cache 한다. 40M 집계는 사라진다.
   공매 적재는 run 시작 때 계획된 물건목록 객체들의 원천 코드로 한 번 부른다. 요약에는
   `parcels_read{asked, held}` 를 남긴다. 결과(어느 PNU 가 현재 필지인가)는 전과 같다.
2. **마감.** 계약 `data-go-kr-onbid.contract.json` `silver_load` 에 다음을 둔다.
   - `run_time_budget_seconds` 1500: 이 시간이 지나면 새 객체를 시작하지 않는다. 시작하지 않은 객체는 기록이 없어서
     다음 run 이 받는다(`deferred`).
   - `run_start_seconds` 540: 단위 시작부터 첫 커밋까지 잰 31 s 에, 한 번 읽는 필지의 상한으로 바뀌기 전 객체당 읽기
     482 s 를 더한다.
   - `measured_object_seconds` p50 16·p90 54·max 563
   - `measured_objects_per_day` 104
   마감 객체(`Deadline`)는 실거래 적재와 같은 것을 `lakehouse_ingest` 로 옮겨 함께 쓴다.
3. **Spark 크기는 계약에서.** 계약 `silver_load.spark` 에 다음을 둔다.
   - `master`: `local[2,8]`
   - `driver_memory`: `1g`
   - 잰 `measured_jvm_non_heap_mib`(793), `measured_python_mib`(601)
   스크립트는 이 값을 읽고, 손으로 적은 크기를 두지 않는다. 1024 + 793 + 601 = 2,418 MiB ≤ 2,560 MiB 다.
4. **가드.** `test_data_go_kr_onbid.FitsItsUnitTest` 는 다음 셋 가운데 하나라도 어기면 CI 를 막는다. 셋 다 단위 파일과
   compose 를 읽는다.
   - 마감 + 가장 긴 객체 + 120 s < `TimeoutStartSec`
   - run 당 객체 수 × 24 ≥ 하루 유입 × 2
   - 힙 + JVM 비힙 + python ≤ spark-small `mem_limit`
   합성 실패 사례(2g 힙, 마감 없음, 객체마다 필지 읽기 p90 957 s)가 세 조건을 모두 어기는 것을 같은 시험이 증명한다.

## 막는 사고 (ADR-0001)

- 2026-10-28 ~ 11-03 이 단위의 OOM(137) 67 회. 11-01 부터는 사실상 매 run 이 죽었다. 물건목록 Silver 는 하루 8 패스 가운데
  0~2 패스만 쌓였다(커밋 실측: 11-01 2, 11-02 0, 11-03 2).
- 2026-10-29 ~ 30 timeout 8 회.

## Consequences

- 물건목록 객체 하나는 이제 비교 객체(상세·공고) 수준이라고 **계산**한다. 배포 뒤 첫 run 의 요약
  (`parcels_read`, 객체마다 커밋 간격)으로 잰 값을 계약에 다시 적는다.
- 필지 표 전체를 읽는 일은 그대로 run 에 한 번 남는다. 열 두 개(`pnu`, `valid_to_utc`)만 읽고 집계 없이 흘려보낸다.
  run 당 주기적 전량 읽기를 아예 없애는 것(현재 필지 PNU 만 담은 작은 색인 표)은 경매·등록공장 레인도 같은 읽기를
  하므로 그쪽과 함께 정한다. 이 ADR 의 범위가 아니다.
- 실거래 적재(`rt-molit-silver-load.sh`)도 같은 컨테이너에 `--driver-memory 2g` 를 준다. 그쪽은 OOM 기록이 아직 없어
  손대지 않았다. 같은 계산이면 들어가지 않으므로 그 레인의 다음 변경에서 같은 계약 칸으로 옮긴다.

## Alternatives

- **timeout 만 늘리기**: 메모리 OOM 은 그대로다. 기각.
- **spark-small 한도 올리기**: 호스트 메모리 예산(ADR-0138)이 by-PNU 굽기 옆에서 이 컨테이너를 2560m 로 셌다. 필요 없는
  40M 집계를 위해 예산을 쓰는 것이다. 기각.
- **필지를 run 마다 전부 cache**: 40M PNU 를 1 GB 힙에 둘 수 없다. 기각.
