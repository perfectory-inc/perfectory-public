---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-10
---

# 예약 작업 운영 (Airflow)

[ADR-0118](../../../../docs/adr/0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md),
[ADR-0122](../../../../docs/adr/0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md).
Airflow 가 일정·재시도·실행 이력·계보를 맡고, 작업 자체는 지금처럼 systemd 서비스가 돌린다.

## 어디에 무엇이 있나

| 무엇 | 정본 |
|---|---|
| 작업이 무엇을 어떤 계정·환경으로 돌리나 | `infra/systemd/*.service` |
| 언제 돌리나, 자원 묶음, 데이터 지도 연결, Airflow 가 돌리나(`enabled`) | `orchestration/jobs.v1.json` |
| DAG | `orchestration/dags/foundation_jobs.py` (작업 목록에서 만들어짐) |
| Airflow 컨테이너 | `compose.orchestration.yml`, 실행은 `scripts/deploy/airflow-runtime.sh` |
| 서버 쪽 권한 | `foundation-release.sh timers` 가 만드는 계정 `foundation-scheduler`, `/etc/sudoers.d/foundation-scheduler` |
| Airflow 가 서버에서 돌릴 수 있는 유일한 명령 | `scripts/ops/start-scheduled-job.sh` |
| 작업이 시작 직전에 받는 검사 | `infra/systemd/foundation-release-admission.conf` — `timers` 가 각 작업 unit 에, 템플릿 인스턴스는 템플릿(`foundation-x@.service.d/`)에 설치한다 ([ADR-0134](../../../../docs/adr/0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)) |
| 배포기를 sudo 로 실행할 수 있는 경로 | 제어 체크아웃의 `foundation-release.sh` 하나 (`deployer-access` 가 설치) |

## 시각으로 시작하는 작업과 입력으로 시작하는 작업

[루트 ADR-0171](../../../../docs/adr/0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md).
`jobs.v1.json` 의 `started_by` 가 정한다.

- `schedule`: `schedule` 시각에 돈다(DAG 하나에 태스크 `run`, 출력이 있으면 `publish_outputs` 가 뒤따른다).
- `inputs`: 다른 작업의 실행이 이 작업의 입력 자산을 바꿨다고 하면 곧 돌고, `schedule` 시각에도 돈다(대체).
  Airflow 화면의 Assets 에서 자산(`iceberg://silver.…`, `perfectory://…`)과 그것을 기다리는 DAG 가 보인다.
- 실행이 바뀐 것이 있는지는 유닛 저널의 마지막 `foundation-job-outcome changed|unchanged` 줄이 말한다. `run` 은 그
  낱말만 XCom `outcome` 으로 남긴다. `unchanged` 면 `publish_outputs` 가 건너뛰어(skipped) 하류가 시작되지 않는다.
  줄이 없으면 `changed` 로 센다.
- `publish_outputs` 는 데이터 카탈로그에 실행을 보고하지 않는다. 카탈로그에는 지금처럼 DAG 와 `run` 의 실행만 있다.
- 하류가 안 돌았을 때 볼 것: 상류 실행의 `run` 로그 끝 줄, `publish_outputs` 가 skipped 인지, 하류 DAG 가 멈춰
  있었는지(멈춘 DAG 몫의 이벤트는 큐에 쌓이지 않는다). 대체 시각에는 어쨌든 돈다.
- 손으로 이벤트를 만들 수 있다: Airflow 화면의 자산 페이지에서 "Create asset event". 손 적재 뒤 Gold 를 바로
  돌릴 때 쓴다.

## 처음 설치

```bash
# 1. 로그인 앱(staff-console-applications.v1.json 의 airflow)을 Zitadel 에 만든다
cd ~/identity-platform/current && bash infra/zitadel/configure-zitadel.sh
# 2. 비밀값과 스케줄러 SSH 열쇠를 만든다 (값은 출력되지 않는다)
bash /opt/foundation-platform/current/scripts/deploy/airflow-runtime.sh init-secrets
# 3. Airflow 를 띄운다 (API·풀·DAG 켜짐 상태·Zitadel 로그인까지 확인)
bash /opt/foundation-platform/current/scripts/deploy/airflow-runtime.sh up -d
# 4. 서버 쪽 계정·열쇠·sudo 허용과 타이머를 맞춘다 (열쇠는 Airflow 망에서만 쓰인다)
sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh timers ~/airflow-state/scheduler_ed25519.pub
# 5. 직원 계정을 미리 등록한다
bash /opt/foundation-platform/current/scripts/deploy/airflow-runtime.sh provision admin@perfectory.io
```

화면은 `http://127.0.0.1:19080` (SSH 터널). 로그인은 Zitadel 만 된다.

## 작업 하나를 Airflow 로 옮기기

1. 꺼진 상태 그대로 한 번 돌려 본다: `airflow-runtime.sh exec airflow-scheduler airflow dags test foundation_<id>`.
   타이머와 겹치지 않는 시각에 한다. 아직 꺼진 작업은 서버의 허용 목록에 없으므로, 이 시험은 시작 단계에서
   거부되는 것이 정상이다 — 끝까지 돌리려면 2를 먼저 한다.
2. 한 변경에서: `jobs.v1.json` 의 `enabled` 를 `true` 로, `infra/systemd/<timer>` 를 지우고, `foundation-release.sh`
   `timers` 의 설치·켜기 목록에서 그 타이머를 뺀다. 시험(`orchestration/tests`)이 둘 중 하나만 바뀐 변경을 막는다.
3. 병합 뒤 배포: `install` → `migrate` → `timers`(서버 타이머 끄기·허용 목록·인증 drop-in 갱신) → `airflow-runtime.sh up -d`(DAG 켜짐).
   `install` 은 GitHub main 에 병합된 커밋만 받는다. 병합 전 브랜치 SHA 로는 설치되지 않는다.
   순서가 거꾸로면 DAG 가 켜지자마자 돈 첫 시도가 허용 목록에 없어 거부되고 5분 뒤 재시도로 넘어간다
   (2026-10-01 실제로 겪음). DAG 를 켜면 Airflow 는 지난 회차 하나를 바로 돌린다 — 켜는 시각이 곧 첫 실행이다.

## 확인

```bash
airflow-runtime.sh exec airflow-scheduler airflow dags list-runs foundation_outbox_publish -o plain | head
systemctl list-timers --all | grep foundation        # 옮긴 작업의 타이머는 없어야 한다
journalctl -u foundation-outbox-publish.service -n 20 # Airflow 가 시작한 실행도 여기에 남는다
```

데이터 카탈로그(DataHub)에서 작업 `foundation_<id>` 의 실행과 입력·출력이 보인다.

## 되돌리기

`jobs.v1.json` 의 `enabled` 를 `false` 로, 타이머 파일과 `timers` 목록을 되살린 변경을 배포한다. `timers` 가 타이머를
다시 켜고 허용 목록에서 그 서비스를 뺀다. Airflow 는 다음 `up` 에서 DAG 를 멈춘다.
