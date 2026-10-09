# ADR 0173: 배포가 멈춘 DAG 는 배포가 어떻게 끝나든 조용히 멈춘 채로 남지 않는다

- Status: Accepted
- Date: 2026-10-10
- Amends: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md) §4 마지막 문장
  ("배포 뒤 작업이 실패하면 DAG 는 멈춘 채로 둔다")

## Context

`foundation-deploy.sh` 는 0 단계에서 모든 DAG 를 멈추고, 1–5 단계에서 새 릴리스를 받아 빌드·활성화·마이그레이션하고,
6 단계에서 `started_once_after_deploy` 작업을 한 번씩 돌리고, 7 단계에서 DAG 를 다시 켠다. ADR-0159 §4 는 6 단계의
실행이 실패하면 배포를 실패(exit 1, `state/failed/<sha>`)로 끝내게 했다. 그 exit 는 7 단계 전에 일어난다.

**사고(2026-10-09).** 그날 연달아 세 번의 배포에서 매일 훑기의 배포 뒤 실행이 실패했다. 밀린 파일이 하루 예산을
넘어 받지 않고 실패한 것으로, 설계대로의 거부였다(ADR-0168). 배포는 매번 6 단계에서 끝났고 7 단계에 닿지 않아
**등록된 DAG 17개 전부**(매시간 접기, 30분 outbox 포함)가 멈춘 채로 남았다. 약 3시간 동안 예약 파이프라인 전체가
아무것도 돌리지 않았고, 18:00 UTC 쯤 운영자가 `jobs.v1.json` 의 켜진 작업을 손으로 다시 켜서 풀었다.

- 작업의 실패는 이미 알려진다: 모든 작업 유닛은 `OnFailure=foundation-unit-failed@%n.service` 로 Slack 에 간다.
- 멈춘 DAG 는 아무 알림도 내지 않는다. 그래서 "한 작업이 실패했다"가 "모든 작업이 조용히 멈췄다"가 됐다.
- 같은 구멍이 0 단계 뒤의 어느 조기 종료에도 있었다: 기다림 5시간 초과, 제어 체크아웃·빌드·FLOOR 설정 실패,
  활성화·마이그레이션 실패 — 모두 DAG 를 멈춘 채로 끝났다.

## Decision

1. **배포 뒤 한 번 돌린 작업의 실패는 배포를 실패로 만들지 않는다.** 6 단계는 각 작업을 돌리고(실패하거나 한도를
   넘겨도 다음 작업으로 간다) 결과를 모은 뒤, 7 단계가 **언제나** 켜진 작업의 DAG 를 모두 다시 켠다. 배포는 성공
   (exit 0)이고 autodeploy 는 `state/deployed/<sha>` 를 남긴다. 실패는 그 유닛의 `OnFailure` 가 Slack 에 알리고, 배포
   로그(autodeploy 유닛의 저널)에 한 줄이 남으며, 다음 예약 실행이 다시 한다:
   `foundation-deploy: the post-deploy run did not succeed under <sha>: <유닛…>; the deploy stands and the DAGs are unpaused, …`
2. **배포 중 멈춤은 그대로다.** 0 단계는 여전히 모든 DAG 를 멈추고 돌던 작업이 끝나길 기다리며, 6 단계의 작업은 DAG
   가 멈춘 동안 돈다.
3. **0 단계 뒤 조기 종료는 EXIT trap 이 처리한다**(신호로 끝나도 같다). 처리는 어디서 멈췄느냐로 갈린다:
   - **전환 전**(0–3 단계: 기다림, 제어 체크아웃, 빌드, FLOOR 설정) — 새 릴리스는 아무것도 켜지지 않았고 돌던 릴리스는
     그대로다. 켜진 작업의 DAG 를 다시 켜서 돌던 릴리스에 예약을 돌려준다. 배포는 실패로 남는다(autodeploy 의
     `OnFailure` 가 알린다).
   - **전환 중**(4–5 단계: 활성화, 마이그레이션, 타이머, Airflow 적재) — 반쯤 적용된 스키마 위에서 작업이 쓰면 안
     되므로 DAG 는 멈춘 채로 두고, 대문자 한 줄(`THE DAGS STAY PAUSED: …`)로 그렇다고 말한다. 배포 실패 자체는
     autodeploy 의 `OnFailure` 로 Slack 에 간다. 고쳐서 다시 배포하거나, 릴리스가 괜찮음을 확인한 뒤 손으로 켠다.
   - **전환 뒤**(7 단계에서 DAG 를 켜다 실패) — `DAGS MAY STILL BE PAUSED: …` 로 말하고 배포는 실패다.
4. 이 처리는 `scripts/deploy/deploy-start-once.sh`(`start_once_then_unpause`, `deploy_paused_dags_on_exit`)에 있고,
   `foundation-deploy.sh` 는 1 단계가 제어 체크아웃을 옮기기 전에 그것을 읽는다.
   `orchestration/tests/test_foundation_autodeploy.py` 의 `TheWholeDeploy` 가 `foundation-deploy.sh` 전체를 대역
   (sudo·systemctl·git·Airflow·foundation-release.sh)으로 돌린다: 사고의 순서 그대로(배포 뒤 작업 하나 실패 → 켜진
   DAG 전부 켜짐, 배포 성공), 빌드 실패(→ 다시 켜짐), 마이그레이션 실패(→ 멈춘 채, 대문자 줄), DAG 켜기 실패. 옛 6
   단계로 되돌리거나, trap 을 빼거나, 전환 중 표시를 늦추면 시험이 깨진다(확인함).

## Consequences

- 새 릴리스가 정말로 작업을 망가뜨렸다면 그 작업은 예약마다 실패하고 예약마다 알린다. 조용히 멈추는 것보다 낫다.
  배포를 되돌리는 길은 그대로 main 의 새 커밋이다(ADR-0159 §4 앞 문장들은 바뀌지 않는다).
- 이 변경을 담은 커밋의 첫 배포는 아직 이전 스크립트가 한다(ADR-0159 §3: 배포는 지금 믿는 체크아웃의 코드가 한다).
  그 배포에서 배포 뒤 작업이 실패하면 한 번 더 DAG 가 멈출 수 있다 — 그 배포 뒤에는 DAG 상태를 확인한다. 새 동작은
  그다음 배포부터다.
- 전환 중 실패는 여전히 DAG 를 멈춘 채로 둔다. 조용하지 않을 뿐이다. 그 경우의 Slack 문구는 autodeploy 유닛 실패
  하나이므로, 받은 사람은 `journalctl -u foundation-autodeploy.service` 에서 대문자 줄을 찾는다.
- 같은 날의 다른 절반 — 실패 이유가 운영자가 못 읽는 파일에만 있었던 것 — 은
  [ADR-0174](./0174-a-jobs-journal-lines-reach-the-units-journal.md) 가 다룬다.
