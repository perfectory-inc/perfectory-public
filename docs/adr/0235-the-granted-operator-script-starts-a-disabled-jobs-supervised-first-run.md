# ADR 0235: 허가된 운영자 스크립트가 꺼진 예약 작업의 감독 첫 실행도 시작한다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0178](./0178-the-granted-operator-script-also-drives-the-silver-refresh-lanes.md)(Silver 레인의
  `silver start`), [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md) §4(작업은 꺼진 채 등록되고 감독 실행 뒤 켠다)

## Context

새 예약 작업은 `enabled: false` 로 들어오고, 데이터 호스트에서 한 번 감독 실행한 뒤에 켠다(ADR-0122 §4). Silver 레인은
ADR-0178 의 `silver start` 로 비밀번호 없이 그 실행을 한다. 다른 작업은 길이 없다: 런북은 `sudo systemctl start <유닛>`
인데 운영자 계정의 sudo 는 비밀번호를 묻고, 호스트의 forced command(`start-scheduled-job.sh`)는 켜진 작업만 시작한다.
2026-10-14 `vworld_parcel_edition`(꺼짐) 의 감독 실행이 이 때문에 막혔다. 열린 PR #483 의 `parcel_tile_bake` 도 같은
처지다.

## Decision

1. 같은 스크립트에 `job` 을 더한다: `by-pnu-pack-operator.sh job start|status <작업 id>`.
   - 받는 작업은 **릴리스의 `orchestration/jobs.v1.json` 이 `enabled: false` 로 적은 것**뿐이고, Silver 레인(템플릿
     유닛 `foundation-silver-refresh@`)은 `silver` 로 보낸다. 켜진 작업은 Airflow 의 것이라 받지 않는다. 그 밖의 이름,
     다른 동작, 남는 인자는 64 로 거부한다.
   - `start`: 그 작업의 `systemd_service` 를 한 번 시작한다. 자동배포(`foundation-autodeploy.service`)나 같은 유닛이
     돌면 75 로 거부한다. `spark` 풀 작업은 Gold 재생성·Silver 레인이 돌 때도 거부한다(같은 20g 상한).
   - `status`: 그 유닛의 상태와 journal 끝 40 줄.
2. sudoers 허가는 바뀌지 않는다(같은 스크립트 경로). 유닛 정의·환경은 릴리스가 정하고 인자로 바꿀 수 없다.

## Consequences

- 꺼진 작업의 감독 첫 실행을 운영자 계정(과 그 계정의 AI 작업자)이 비밀번호 없이, 배포와 겹치지 않게 한다.
- 허가 범위가 그만큼 넓어진다: 꺼진 작업 유닛의 시작. 작업을 켜면(배포) 이 길에서 빠진다.
- 시험: `orchestration/tests/test_by_pnu_pack_operator.py` 의 job 시험 셋(유닛 이름, 배포·Spark 작업 중 거부, 켜진 작업·
  Silver 레인·목록 밖 이름 거부). `enabled` 검사를 빼면 거부 시험이 실패한다.
