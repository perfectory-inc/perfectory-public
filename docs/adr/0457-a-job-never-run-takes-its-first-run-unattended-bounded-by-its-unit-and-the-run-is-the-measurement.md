# ADR 0457: 한 번도 안 돈 작업은 사람 없이 첫 run 을 돈다 — 그 run 은 유닛의 상한에 묶이고, 그 run 이 곧 측정이다

- Status: Accepted
- Date: 2026-11-09
- Amends: [ADR-0386](./0386-every-job-states-its-per-run-bound-in-one-vocabulary-and-its-measured-runs-fit-its-unit.md) §3
  (잰 값이 없는데 켜진 작업은 거부 — "켜려면 운영자 run 으로 먼저 잰다"). 나머지(한 어휘, 맞음의 정의, 다시 재기, 열린 예외)는 그대로다.
- Builds on: [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)
  (모든 유닛의 MemoryMax), ADR-0270 §1(`time_limit_minutes` 하나가 유닛의 `TimeoutStartSec`), ADR-0301(`commits`),
  ADR-0435(정상 경로는 승인을 기다리지 않는다)

## Context

**사고 (W30d, #1200).** 2026-11-09 화면 표(로드맵 §10)에서 Silver 20 개가 "적재 꺼짐"이다. 채우는 적재 작업
10 개가 모두 `enabled: false` 이고, 꺼진 이유는 같다: ADR-0386 §3 이 잰 값 없이 켜진 작업을 거부하고, 잴 길은
"운영자 run"(사람이 서버에서 유닛을 손으로 시작)뿐이다.

| 작업 | Silver | Bronze 입력(장부, 2026-11-09) | 꺼진 날부터 |
|---|---|---|---|
| `silver_refresh_building_permit_*` 7 개, `…_regular_inspection_history` | 건축인허가 동별·층별·대지위치·대수선·가설·부설주차장·공작물, 정기점검이력 | 각 5 판, 10-20 수집 | 10-20 ~ 11-04 |
| `data_go_kr_file_data` | 물류 5·주변 생활 5·법정동 연계 | 이 작업이 스스로 수집 | 11-04 |
| `parcel_registry` | 필지 ID 원장 | 잰 run 2 회(최장 1,429 s), 표에 39,861,511 행 | 10-16 |

입력은 3 주째 있는데 표가 없다. 사람이 손으로 run 하나를 해야 하는 관문은 사람이 안 오면 영원히 닫혀 있다.
사장 규칙(2026-11-07, 11-09): 정상 경로에 사장·운영자 단계가 없어야 한다.

ADR-0386 §3 이 막으려던 것은 사람이 아니라 **잰 적 없는 run 이 시간제한에 걸려 일을 통째로 잃는 것**이다(공매 수집
10-22~26 timeout 87 회, `at_end` 라 0 커밋. rt-molit Silver 10-30 40 분마다 죽음). 그 위험은 첫 run 의 모양으로
막을 수 있다. 사람이 지켜본다고 그 run 이 더 짧아지지 않는다.

큰 곳의 방식: Kubernetes Job 은 이력이 없어도 `activeDeadlineSeconds` 와 `resources.limits.memory` 안에서 첫 run 을
돈다. AWS Batch 는 `timeout.attemptDurationSeconds` 와 `retryStrategy.attempts` 를 작업 정의에 두고 첫 시도도 그 안에서
돈다. Airflow 의 `execution_timeout`·`retries` 도 같다. 첫 run 을 묶는 것은 정의의 상한이고, 그 run 의 기록이 다음 판단의
근거다. 사람의 입회가 아니다.

## Decision

1. **한 번도 안 돈 작업도 켤 수 있다 — 첫 run 이 사람 없이 그 일정에 돈다.** `per_run_cap.measured_run_seconds` 가
   null 이고 `enabled: true` 인 작업은 `per_run_cap.first_run` 을 적는다.
   - `estimate_seconds`: 첫 run 의 계산값(초). 같은 유닛·같은 스크립트의 잰 이웃이나 같은 입력의 잰 단계에서 나온다.
     `time_limit_minutes` 의 90% 이하(ADR-0386 §2 의 몫)여야 한다. 계산이 상한을 넘으면 켤 수 없다 — 전국 Gold 첫 빌드
     같은 큰 일은 이 길로 켜지지 않는다.
   - `estimated_from`: 무엇에서 계산했는가(이웃 작업 이름과 그 잰 값, 입력 크기).
2. **첫 run 의 상한은 유닛이 이미 가진 것이다.** 새 장치는 없다.
   - 시간: `time_limit_minutes` → `TimeoutStartSec`(ADR-0270 §1). systemd 가 그 시각에 멈춘다.
   - 메모리: 유닛의 `MemoryMax`·OOM 등급(ADR-0455). 호스트를 쓰러뜨리지 못한다.
   - 잃는 것: `commits: as_it_goes` 만 허용한다. 멈춘 run 은 들고 있던 항목 하나만 잃고 다음 run 이 이어 간다.
     `at_end` 작업(공매 수집의 사고 모양)은 이 길로 켜지지 않는다.
   - 되풀이: `retries: 0`. 제한에 걸린 첫 run 이 곧바로 다시 시작되지 않는다(rt-molit 의 40 분마다 죽음 모양). 다음 시도는
     다음 일정이다.
3. **첫 run 이 곧 측정이다.** 그 run 이 끝나면 systemd 저널에 시작·끝·`memory peak` 이 남는다(ADR-0386 §1,
   ADR-0455 측정과 같은 출처). 그 값을 `job-run-times.py` 로 읽어 `measured_run_seconds` 를 적는 PR 이 `first_run` 을
   지운다. 그 뒤로는 ADR-0386 §2 의 맞음이 그대로 판정한다(timeout 이면 열린 예외).
4. **가드.** `orchestration/tests/test_per_run_cap.py` 가 거부한다: 켜졌는데 잰 값도 `first_run` 도 없는 작업, 계산이
   제한의 90% 를 넘는 `first_run`, `at_end` 이거나 `retries` 가 0 이 아닌 `first_run`, 잰 값이 생겼는데 남은 `first_run`
   (낡음). 꺼진 작업은 전과 같이 `unmeasured_reason` 을 적는다. 시험 안의 합성 사례가 각 거부를 증명한다.
5. **이 PR 이 켜는 작업.** 위 표의 10 개. 건축인허가 8 레인은 같은 템플릿 유닛의 잰 이웃(동별 기본개요 438 MB 판 357 s,
   주차장 59 MB 판 251 s, 멸실 15 MB 판 161 s)에서 판 크기 100 MB 넘는 셋은 900 s, 나머지는 600 s 로 계산했다(제한 9,000 s).
   `data_go_kr_file_data` 는 그 작업의 잰 단계(8 파일 365 MB 첫 수집, 적재 준비 139 s, 표준데이터 35 s)에서 1,800 s
   (제한 5,400 s). `parcel_registry` 는 이미 잰 run 이 있어(최장 1,429 s / 9,000 s) §3 이전 규칙으로도 맞고, 꺼 둔 이유
   ("감독 run 뒤 켬")만 남아 있었다.

## Consequences

- 새 적재 작업은 정의 PR 이 병합되면 그 일정에 첫 run 을 돈다. 사람 단계가 없다. 그 사이 시간은 ADR-0159 자동 배포 +
  다음 일정이다.
- 첫 run 이 실패하거나 제한에 걸리면 유닛의 OnFailure 가 그 주인을 부른다(ADR-0445). 잃는 것은 그 run 의 시간과 들고 있던
  항목 하나다. 다음 일정이 다시 시도하므로, 고쳐질 때까지 하루 한 번 실패가 남는다. 그 실패가 측정이 되고, 측정 PR 이
  열린 예외로 적는다.
- 측정 PR 을 날짜로 강제하지는 않는다(ADR-0386 §4 와 같은 이유: 날짜가 지났다고 상관없는 PR 이 깨지면 안 된다). 잰 값이
  저널에 있는데 `first_run` 이 남아 있으면 그 작업을 고치는 다음 PR 이 적는다.
- 계산값이 틀릴 수 있다. 틀려도 상한은 계산이 아니라 유닛의 `TimeoutStartSec`·`MemoryMax` 다. 계산은 "켜도 되는가"의 문턱일
  뿐이다.
- 비용 0: 시험은 파일 읽기뿐이다. 첫 run 의 비용은 그 작업이 원래 쓸 계산과 같다.
