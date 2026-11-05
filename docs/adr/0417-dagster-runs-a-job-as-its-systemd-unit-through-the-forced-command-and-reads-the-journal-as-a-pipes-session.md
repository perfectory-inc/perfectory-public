# ADR 0417: Dagster 는 작업을 강제 명령으로 그 systemd 유닛으로 돌리고, 스트리밍된 저널을 Pipes 세션으로 읽는다

- Status: Accepted (2026-11-05, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-05
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(스케줄러는 유닛을 시작하고 systemd 가 돌린다, SSH 강제 명령),
  [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(실행은 systemd 소유),
  [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(작업 결과 줄),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(엔진은 Dagster, airlift 로 DAG 하나씩)
- 관련 사건: 2026-10-28 손으로 이은 Airflow 자산 사슬에서 이벤트 3912 가 사라져 공매 굽기가 약 13시간 기다렸다(ADR-0333).
  이것을 고치는 Dagster 이전은 호스트의 보안 경계(강제 명령, systemd 유닛)를 넓히지 않고 해야 한다.

## Context

지금 Airflow 의 `RunJob`(`orchestration/dags/foundation_jobs.py`)은 SSH 로 `scripts/ops/start-scheduled-job.sh` 하나만 부를 수 있다.
그 강제 명령은 작업 id 하나를 읽어 유닛을 시작하고, 그 호출의 저널(`journalctl -f _SYSTEMD_INVOCATION_ID=…`)을 흘려보낸 뒤,
마지막 `foundation-job-outcome` 줄과 종료 상태로 끝난다. Airflow 는 그 줄로 하류 시작 여부를 정한다
(`job_specs.job_outcome`, `WITHHOLDS_EVENTS`).

Dagster Pipes 는 오케스트레이터가 외부 프로세스를 띄우고 그 보고(물질화·검사·로그)를 받는 표준 규약이다.
메시지 읽기는 사용자 정의 reader 로 바꿀 수 있다(dagster.io/docs/integrations/external-pipelines/dagster-pipes-details-and-customization).
dagster 1.13.27 로 확인한 제약: Pipes 세션을 열면 그 단계는 선택된 모든 자산을 보고해야 한다
(`requires_typed_event_stream`, `dagster/_core/execution/plan/compute.py`). 그래서 "바뀐 것 없음"으로 끝난 실행이 세션 안에서는 실패가 된다.

## Decision

1. **실행기는 그대로 systemd, 통로는 그대로 강제 명령이다.** `orchestration/dagster/systemd_pipes.py` 의 `SystemdUnitPipesClient` 는
   Airflow 와 같은 강제 명령에 `<job id>` 또는 `<job id> triggered` 만 보낸다. Dagster 는 호스트에서 다른 권한을 갖지 않는다.
   목록에 없는 작업 id 는 보내기 전에 거부한다.
2. **저널의 줄은 세 가지다.**
   - Pipes 메시지(`__dagster_pipes_version` 가 있는 JSON 한 줄): 보관했다가 세션에 넘긴다. 허용 방법은
     `report_asset_materialization`·`report_asset_check`·`log`·`report_custom_message` 뿐이다. `opened`/`closed` 는 reader 가 만들고,
     저널이 보내면 단계를 실패시킨다. 작업이 세션을 일찍 닫거나 다른 실행을 사칭할 수 없다.
   - 결과 줄: 마지막 것이 실행을 정한다. 규칙은 Airflow 와 같은 `job_specs.job_outcome` 이다.
   - 그 밖: 도착하는 대로 단계 로그가 된다.
3. **결과.**
   - `changed`: 세션을 열어 실행이 보고한 것을 메타데이터와 함께 넘기고, 나머지 자산은 암묵 물질화한다.
   - `unchanged`·`waiting`·`skipped`: 세션을 열지 않고 아무것도 내지 않는다. 자산은 `skippable` 이라 물질화되지 않고, 하류도 시작하지 않는다(위 제약 때문).
   - `failed`, 또는 종료 상태가 0 이 아님: 단계가 그 줄의 이유로 실패한다. `changed` 뒤의 0 아닌 종료도 실패다.
4. **작업은 바꾸지 않는다.** 지금의 결과 줄만으로 동작한다. 작업이 Pipes 보고 줄(행 수, 검사 결과)을 한 줄씩 더하면 그대로 자산에 기록된다.
   결과 줄 규약의 은퇴는 모든 DAG 가 옮겨진 뒤(ADR-0415 §5 3단계 끝)다.
5. **시험.** `orchestration/tests/test_dagster_systemd_pipes.py`(CI `dagster-observe` 레인, 코드 위치 이미지 안). 강제 명령을 대신하는 스크립트가
   저널 줄과 종료 상태를 내고, 그 뒤는 실제 Dagster 다(`materialize`, 실제 세션과 메시지 처리기). 시험은 7개다:
   - `changed` + 보고 하나 → 자산 둘 물질화, 보고한 메타데이터가 남음.
   - 보류 결과 셋 → 성공, 물질화 0.
   - `failed` → 단계 실패, 이유가 남음.
   - 0 아닌 종료 → 실패.
   - 저널의 `closed` → 실패.
   - 모르는 작업 → 실행 전 거부.
   - 결과 규칙이 Airflow 와 같음.

## Consequences

- airlift 의 다음 단계(proxy·migrate)에서 레인 하나를 옮길 때, 그 레인의 자산이 이 클라이언트로 작업을 돌린다. 호스트·유닛·키 규약은 그대로다.
  Dagster 용 SSH 키는 Airflow 키와 같은 강제 명령에 묶인 별도 키로 둔다(배치 PR 에서, 키는 운영자가 1회).
- 저널은 실행이 끝날 때까지 보관한 메시지를 끝에 한 번에 넘긴다. 로그는 실시간이다. 진행 중 물질화(긴 실행의 부분 발행)는 이 ADR 밖이다.
- 결과 줄과 Pipes 가 둘 다 있는 동안 진실은 결과 줄이다. 둘이 어긋나면 결과 줄을 따른다(보고는 `changed` 일 때만 쓰인다).

## 기각한 대안

- **`PipesSubprocessClient` 를 그대로 쓰기**: 컨텍스트를 환경 변수로 넣는데, 강제 명령은 작업 id 말고는 아무것도 받지 않는다. 받게 하면 보안 경계가 넓어진다.
- **Dagster 가 `systemctl` 을 직접 부르기(sudo 또는 D-Bus)**: ADR-0122 §2 의 경계를 깬다.
- **세션을 항상 열고 보류 결과에 빈 물질화를 내기**: 하류가 "바뀌었다"로 읽고 시작한다. 바뀐 것만 처리하는 원칙([ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md))에 어긋난다.
