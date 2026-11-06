# ADR 0386: 모든 작업은 run 당 상한을 한 어휘로 적고, 잰 run 시간이 단위 시간제한 안에 든다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md) H-cap, [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) P1, [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §1(`time_limit_minutes` 하나가 단위 `TimeoutStartSec`)
- Generalises: #988 의 rt-molit 시험 `FitsItsUnit`(한 작업) → 작업 목록 전부

## Context

같은 사고가 두 번 났다. 둘 다 시간제한은 있었고, 그 제한 안에 든다고 잰 run 당 상한이 없었다.

| 사고 | 무엇 | 잰 값(systemd 저널, 읽기 전용) |
|---|---|---|
| 공매 수집 2026-10-22~26 | 40분 제한에 걸려 죽음. 끝에서 한 번 커밋(`at_end`)이라 run 통째로 잃음 | timeout 87회(10-22T02:07Z ~ 10-26T03:19Z). #862·#864·#871 로 API 마다 `per_run_limit` 을 넣은 뒤 0회 |
| rt-molit Silver 2026-10-30 | 한 run 이 객체 60개를 잡아 40분마다 죽음 | timeout 2회(22:48Z, 23:33Z). #988 이 잰 상한(9건·30분)을 넣은 뒤 0회 |

ADR-0299 H-cap 은 "모든 수집 job 은 run 당 한도를 하나의 통일된 어휘로 선언하고, 미선언은 CI 가 거부한다"를
정했다. 지금 상한 이름은 계약마다 다르다: `per_run_limit`(공매), `max_objects`(Silver 적재 셋),
`max_requests_per_run`(법원경매), `max_downloads_per_run`·`run_time_budget_seconds`(rt-molit). 맞는지 보는
시험은 rt-molit 하나뿐이다.

작업 목록 전부를 저널로 쟀다(2026-10-16 ~ 11-03, 저널이 거기서 시작한다. `scripts/ops/job-run-times.py`).
작업 50개 중 46개가 돌았고, 5개는 지금 안 맞는다(아래 §5).

큰 곳의 방식: Kubernetes Job 은 `activeDeadlineSeconds` 를, AWS Batch 는 `timeout.attemptDurationSeconds` 를
작업 정의 한 곳에 둔다. 우리 시간제한은 이미 `jobs.v1.json` 한 곳(ADR-0270 §1)에 있다.
상한과 그 근거(잰 값)를 같은 곳에 둔다.

## Decision

1. **한 어휘, 한 곳.** `orchestration/jobs.v1.json` 의 모든 작업이 `per_run_cap` 을 적는다. 뜻은 같은 파일의
   `per_run_cap_means` 에 있다.
   - `unit`: run 이 몇 개까지만 하는 것 — `request`·`download`·`object`. 항목 반복이 없으면 `run`(run 전체가 한
     단위).
   - `limits`: run 당 개수가 사는 곳. `<파일>#<JSON pointer>` 로 원천 계약을 가리킨다. 계약은 제 이름
     (`per_run_limit`, `max_objects` …)을 그대로 쓰고, 어느 것이 run 당 상한인지는 이 목록이 말한다. 값은 한
     곳에만 있다(계약). 계약 키 이름을 바꾸지는 않는다: 수집기 코드가 읽는 이름이고, 어휘는 작업 목록 쪽에서
     하나로 모인다.
   - `deadline`: 그 시각이 지나면 새 항목을 시작하지 않는 초(`run_time_budget_seconds`)를 가리킨다. 없으면 null.
   - `measured_run_seconds`: 단위의 systemd 저널에서 시작부터 끝까지 잰 값(`runs`·`p50`·`p90`·`max`·
     `timeouts`·`from`·`to`). 실패한 run 도 센다. 창은 저널 처음부터다. 그 작업의 run 을 고친 변경이 있으면
     그 병합 시각부터 세고, `from_change` 에 그 PR 을 적는다(공매 수집 #871, rt-molit Silver #988).
2. **맞음의 정의.** 다음을 모두 지키면 맞는다.
   - 잰 run 중 timeout 이 0 이다.
   - 가장 긴 run 이 `time_limit_minutes` 의 90% 이하다. 90% 는 #988 이 rt-molit Silver 에 남긴 몫이다:
     1,800 + 206 + 120 s / 2,400 s.
   - `deadline` 이 있으면 그 값도 90% 이하다.
   - 그 계약 절이 항목 시간을 쟀으면(`measured_object_seconds`, `run_start_seconds`) `FitsItsUnit` 의 두 식을
     지킨다: `run_start + max_objects × p90 ≤ deadline`, `deadline + max + 120 < 제한`.
3. **가드.** `orchestration/tests/test_per_run_cap.py`(Foundation CI 의 `orchestration/tests` 스위트)가 다음을
   거부한다.
   - `per_run_cap` 이 없는 작업.
   - 어휘 밖의 `unit`.
   - 아무것도 가리키지 않거나 양의 정수가 아닌 pointer.
   - 잰 값이 없는데 켜진 작업. 잰 값이 없으면 `enabled: false` 와 `unmeasured_reason` 이 있어야 하고, 켜려면
     운영자 run 으로 먼저 잰다.
   - 안 맞는데 `open_exception`(`why`, `tracked_by`)이 없는 작업.
   - 맞는데 `open_exception` 이 남은 작업(낡은 예외).
   - 로드맵에 없는 예외.

   시험 안의 합성 사례가 각 거부를 증명한다: 상한 없음, timeout, 제한의 94%, 낡은 예외, 잰 값 없이 켬, 없는
   pointer, #988 이전 값(객체 60개)이 deadline 을 넘음 — 9개로 고치면 통과.
4. **다시 재기.** 상한이나 시간제한을 바꾸는 PR 은 `job-run-times.py` 로 다시 잰 블록을 넣는다. 사용법은 그
   스크립트 머리말에 있다. 서버에서는 `journalctl` 읽기뿐이고 쓰는 것은 없다. 오래된 잰 값을 날짜로 막지는
   않는다: 날짜가 지났다고 상관없는 PR 이 CI 에서 깨지면 안 된다.
5. **지금 안 맞는 작업 다섯 — 열린 예외로 적는다.** 숨기지 않는다. 각 작업의 `open_exception` 과 로드맵 §2 에 있다.

   | 작업 | 잰 값 | 다음 |
   |---|---|---|
   | `onbid_public_auction_silver` | 10-29T22:29Z ~ 10-30T01:50Z timeout 5회(#943·#945 뒤 따라잡기). 그 뒤 최장 1,418 s | `silver_load` 에 deadline 과 잰 객체 시간(#988 방식) |
   | `silver_refresh_building_register_unit_areas` | 10-20 한 run 8,573 s / 9,000 s(95%) | 갱신 레인에 run 당 버킷 상한 + deadline(ADR-0318 의 버킷 비교·이어하기 위에) |
   | `silver_refresh_building_register_apartment_price` | 10-20 timeout(9,008 s). 다시 돈 run 4,329 s. 아직 전량 | 같음 + 그 작업의 `full_tracked_by` |
   | `silver_refresh_land_transfer_history` | 10-21 timeout(9,009 s). 11-03 7,415 s(82%) | 갱신 레인 버킷 상한 |
   | `gold_panel_rebuild` | 10-20 이후 timeout 10회. 마지막 11-03T08:07Z·10:53Z | #951(바뀐 행만 MERGE) |

## Consequences

- 새 수집기나 적재 작업은 잰 상한 없이 켜질 수 없다. 공매·rt-molit 같은 run 통째 손실이 다시 오면 그 run 이
  아니라 PR 이 먼저 막힌다.
- 범위는 50개 전부다. `jobs.v1.json` 에는 배치 작업만 있고, 모두 무언가를 읽어 쓴다(수집·적재·갱신·발행·굽기·
  점검). "수집과 적재만"으로 나누면 경계를 또 따져야 하고, 시간제한을 넘는 사고는 어느 작업에나 같다
  (`gold_panel_rebuild` 가 지금 가장 자주 넘는다).
- 다섯 예외는 owner 결정이나 후속 PR 이 필요하다. 특히 `gold_panel_rebuild` 는 11-03 에도 두 번 제한에 걸렸다.
- 성공·실패를 세는 것은 이 결정 밖이다. `failed` 가 많은 작업(공매 Silver 62/83, rt-molit Silver 37/85,
  10-31 이후)은 시간이 아니라 다른 원인이다.
- 비용 0: 시험은 파일 읽기뿐이고, 재기는 저널 읽기다.
