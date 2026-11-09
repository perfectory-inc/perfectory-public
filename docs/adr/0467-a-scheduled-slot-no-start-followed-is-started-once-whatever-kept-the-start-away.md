# ADR 0467: 시작이 따르지 않은 일정은 한 번 시작된다 — 무엇이 시작을 막았든

- Status: Accepted
- Date: 2026-11-09
- Builds on: [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)
  (풀이 거절한 시작의 대기열), ADR-0333 §7(수준 기반 조정자), ADR-0138(`takes_turns`), ADR-0282 §3(멈춤), ADR-0239(`enabled_since`)

## Context

**사고 (W30f, 2026-11-09 16:20Z 잰 값).** `data_go_kr_file_data` 의 첫 run(11:40Z)이 spark 풀 claim 에 거절됐다. ADR-0462
의 대기열(`/var/lib/foundation-jobs/queue`)은 14:42Z 배포로 생겼다. 거절은 그보다 3시간 먼저였으므로 대기열에 기록이 없고,
조정기는 그 작업을 다시 시작할 근거가 없었다. 다음 시도는 11-10 11:40Z, Silver 11 개는 하루 더 비어 있다. 같은 날
07:00Z 에 거절된 첫 Silver 적재 9 개도 같은 모양이었다.

ADR-0462 는 **거절이라는 사건**을 기록한다. 사건 기록은 그 장치가 생기기 전의 사건, 기록이 실패한 사건(`cannot queue
the refused start` 줄), 호스트에 닿지 못한 시작(Airflow 정지, SSH 실패)을 모른다. 남는 흔적은 하나다: **일정이 지났는데
그 뒤로 시작이 없다.**

그 흔적을 읽을 곳이 없었다. 시작 기록(`start-scheduled-job.sh` 가 작업마다 쓰는 epoch 한 줄, `takes_turns` 규칙이 읽음)은
scheduler 의 0750 홈(`/var/lib/foundation-scheduler/starts`)에 있어 조정기(`foundation-platform`)가 못 읽는다. systemd 는
쉬는 유닛을 내리면서 시각을 잊는다(11-09 16:39Z 호스트: spark 작업 45 개 가운데 실패 상태인 하나 말고는
`ActiveEnterTimestamp` 가 비어 있음). 조정기 사용자는 journal 그룹이 아니다.

큰 곳의 방식: Kubernetes CronJob 컨트롤러는 `lastScheduleTime` 이후 놓친 일정을 세어 가장 최근 것 하나를 시작한다
(`startingDeadlineSeconds` 안에서). systemd 타이머의 `Persistent=true` 는 꺼져 있던 동안 놓친 일정을 켜질 때 한 번 돌린다.
cron 의 anacron 도 같다. 공통점은 **사건이 아니라 "마지막 시작 < 마지막 일정" 이라는 상태**를 본다는 것이다.

## Decision

1. **시작 기록을 대기열 옆으로 옮긴다.** `start-scheduled-job.sh` 의 시작 기록은 `/var/lib/foundation-jobs/queue/starts/<job>`
   (scheduler 소유, 0755)이다. 형식은 그대로(epoch 한 줄, 확인이 할 일이 없다고 답한 것도 시작, ADR-0305). 기록은 하나다:
   `takes_turns` 와 조정기가 같은 파일을 읽는다. 배포의 `timers`(install_scheduler_access)가 디렉터리를 만들고, 옛 홈의 기록을
   한 번 복사한 뒤(`cp -n`, runner 가 그 사이 쓴 더 새 기록은 덮지 않음) `.seeded` 표지를 쓴다. 옛 디렉터리는 지우지 않는다.
2. **조정기가 놓친 일정을 한 번 시작한다.** `asset_reconcile.py missed_starts`: 조정기가 다시 시작해도 되는 작업
   (`job_specs.requeued`, ADR-0462)마다, 15분(`MISSED_START_GRACE`) 이전의 가장 새 일정(`job_specs.previous_slot`)이
   `enabled_since` 뒤이고 그 뒤로 시작 기록이 없으면 그 일정을 키로 `foundation-asset-owed` 한 줄을 쓴다. 같은 일정은 같은 키라
   DAG 은 한 번만 시작한다(`reconcile__<job>__<key>`). 시작이 풀에 거절되면 그때부터는 ADR-0462 의 대기열이 맡는다.
3. **다른 규칙의 몫은 건드리지 않는다.** 대기열에 있는 작업(ADR-0462), `takes_turns` 작업(차례를 미룬 것은 설계, ADR-0138),
   멈춘 작업(`switches/<job>`, ADR-0282 §3), 그 유닛이나 확인 유닛이 도는 작업은 빼고, Airflow 에 이미 대기·실행 중인 run 이
   있으면 `select` 가 뺀다. `.seeded` 가 없으면 아무 일정도 판정하지 않는다(복사 전의 빈 디렉터리를 "한 번도 안 돎" 으로 읽지 않음).
4. **경보.** `foundation_job_missed_start_seconds`(놓친 일정부터 지금까지)와 `FoundationJobMissedStartNotTaken`(2시간 넘게
   남으면, 청한 시작이 돌지 않은 것)이다.

## 버린 대안

- **거절 기록만 과거로 채우기(일회성 백필).** 이번 하나는 고치지만 다음 "기록 못 한 사건" 은 또 하루를 잃는다. 상태를 보면
  원인을 몰라도 된다.
- **systemd 시각·journal 읽기.** 쉬는 유닛의 시각은 사라지고(잰 값), journal 은 조정기 사용자에게 권한을 더 줘야 한다.
- **조정기에 scheduler 홈 읽기 권한(그룹·ACL).** 홈에는 `.ssh` 와 run-now 사용 기록도 있다. 공유할 상태만 공유 위치
  (`/var/lib/foundation-jobs`, 대기열·지표·스위치가 있는 곳)로 옮기는 편이 경계가 분명하다.
- **Airflow 메타데이터에서 마지막 run 상태 읽기.** 작업은 Dagster 로 옮겨 가는 중이고(ADR-0431), 호스트의 시작 기록이
  어느 오케스트레이터가 시작했든 같은 답을 준다.

## Consequences

- 거절된 일일 작업의 다음 시도(이 ADR 이전에 거절된 것, 대기열 기록이 실패한 것 포함): 24시간 뒤(잰 값, 11-09) → 배포 뒤 첫
  조정 틱(5분), 풀이 차 있으면 대기열의 백오프(늦어도 1시간)(계산값).
- 첫 배포의 `timers` 뒤 첫 틱에, 마지막 일정 뒤로 시작이 없는 작업이 한 번씩 시작된다. 몇 개인지는 옛 기록을 조정기가 못 읽어
  미리 재지 못했다. 풀(슬롯 3)과 대기열이 순서를 정하므로 한꺼번에 돌지 않는다.
- 배포가 DAG 을 멈춘 동안 놓친 일정도 풀린 뒤 한 번 시작된다(`Persistent=true` 와 같은 따라잡기). 배포가 시작하는
  `started_once_after_deploy` 작업은 `systemctl start` 로 돌아 시작 기록을 남기지 않으므로, 그 전에 놓친 일정이 있으면 한 번 더
  시작될 수 있다. 작업은 바뀐 것만 하므로(ADR-0300) 그 run 은 대개 `unchanged` 다.
- 비용 0: 조정 틱마다 디렉터리 하나 읽기.
- 시험: `orchestration/tests/test_asset_reconcile.py::MissedStarts`(11-09 11:40Z 재현, 같은 일정 같은 키, 다른 규칙의 몫,
  유예·`enabled_since`, 복사 전), `test_job_units.py`(세 곳의 경로가 같음, `.seeded` 전에는 읽지 않음).
