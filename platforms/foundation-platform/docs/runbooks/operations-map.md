---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-13
---

# 데이터 흐름 지도 — 읽기 전용 자격 파일·Slack 알림·확인 런북

루트 [ADR-0195](../../../../docs/adr/0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md)
(지도가 작업마다 다섯 신호를 계산)와 [ADR-0208](../../../../docs/adr/0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md)
(그 문제를 기존 Alertmanager 로 Slack 에 보냄)의 운영 절차다. 값은 여기에 적지 않는다. 이름과 경로만 적는다.
작업이 성공했는지는 그 실행의 마지막 `foundation-job-outcome` 줄로 정한다 — 줄이 없으면 실패다
([ADR-0259](../../../../docs/adr/0259-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md), 제안).

## 1. 서버에 만들 파일 하나 — `/etc/foundation-platform/operations-map.env`

지도는 DataHub 말고 세 곳을 더 읽는다: Airflow(실패 로그), 레이크하우스 카탈로그(표의 스냅숏), 그리고 직원이 여는 화면 주소.
그 자격과 주소를 이 파일 하나가 갖는다. 파일이 없어도 지도는 뜨고, 무엇을 못 읽었는지 말한다(`scheduler_error`,
`lakehouse_error`).

| 항목 | 값 |
|---|---|
| 경로 | `/etc/foundation-platform/operations-map.env` (`scripts/deploy/foundation-runtime.sh` 의 `FOUNDATION_PLATFORM_OPERATIONS_MAP_ENV_FILE` 기본값) |
| 소유·권한 | `root:foundation-platform`, `0640` — `config/runtime-secrets.contract.json` 의 `operations-map` 항목 |
| 누가 읽나 | `compose.orchestration-bridge.yml` 이 `foundation-api` 에만 싣는다 |
| 배포 검사 | `foundation-release.sh` 의 `verify_runtime_secrets` 가 파일이 있으면 아래 이름이 전부 있는지 본다(이름만) |

담을 키는 정확히 아래 아홉 개다(계약의 `holds` 와 같은 순서).

```text
FOUNDATION_PLATFORM_AIRFLOW_API_USERNAME=        # ~/airflow-state/operations-map-reader.env 의 Viewer 계정(이미 있음)
FOUNDATION_PLATFORM_AIRFLOW_API_PASSWORD=        # 같은 파일. 다시 만들 땐 airflow-runtime.sh api-reader
FOUNDATION_PLATFORM_AIRFLOW_UI_URL=              # 직원이 여는 Airflow 웹 주소
FOUNDATION_PLATFORM_DATAHUB_UI_URL=              # 직원이 여는 DataHub 웹 주소
FOUNDATION_PLATFORM_DAWNEER_UI_URL=              # 직원이 여는 더니어 주소(경로 없이). Slack 알림의 링크가 <이 주소>/#/flow?node=<노드> 가 된다
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=       # Spark 작업이 쓰는 값과 같다
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=         # Spark 작업이 쓰는 값과 같다
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=  # 읽기 전용 카탈로그 토큰(아래)
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
```

`FOUNDATION_PLATFORM_AIRFLOW_API_URL` 은 넣지 않는다 — `compose.orchestration-bridge.yml` 이 정한다.

**읽기 전용 카탈로그 토큰**: Cloudflare 계정 API 토큰 `foundation-platform-lakehouse-catalog-reader-prod`(변수 이름은 이 토큰 이름에서 만든다, 루트 ADR-0247), 권한 그룹은 **Workers R2 Data Catalog Read** 하나
(대시보드 → *Manage Account → API Tokens → Create Custom Token*). R2 토큰 화면만 쓸 수 있으면 **Admin Read only**.
지도는 카탈로그의 `loadTable`(표 메타데이터)만 부르고 데이터 파일은 읽지도 쓰지도 않는다. Spark 쓰기 토큰을 재사용하지 않는다.
만료·교체일은 토큰을 관리하는 곳에 적는다.

만드는 순서. 파일은 손으로 편집하지 않는다 — `foundation-release.sh env-set` 이 계약의 `operations-map` 그룹이 담는
이름만 받아 계약의 소유자·모드로 쓴다(루트 ADR-0247). 그룹이 담지 않는 이름(예: 쓰기 토큰)은 거부된다. 값은 출력되지
않고, 셸 기록에도 남기지 않는다. 새 이름을 읽는 코드가 배포되기 **전에** 넣는다: `foundation-api` 컨테이너는
만들어질 때만 이 파일을 읽으므로, 이름이 없던 때 만들어진 컨테이너는 다시 만들 때까지 카탈로그를 못 읽는다(2026-10-15 실측).

```bash
# 노트북: 값을 찍지 않고 이름=값 줄만 0600 파일로 서버에 보낸다
grep -E '^FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=' platforms/foundation-platform/.env.local \
  | tr -d '\r' | ssh <서버> 'umask 077; cat > ~/operations-map.set'
# 서버: 계약을 거쳐 쓰고, 원본을 지우고, 지도를 다시 띄운다
sudo -n /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh env-set operations-map ~/operations-map.set
shred -u ~/operations-map.set
/opt/foundation-platform/current/scripts/deploy/foundation-runtime.sh up -d foundation-api
```

확인:

- `GET /data-catalog/v1/scheduled-jobs` 의 `lakehouse_error`, `scheduler_error` 가 둘 다 `null`.
- `curl -s 127.0.0.1:<foundation-api 포트>/metrics | grep foundation_scheduled_jobs_readable` 이 `1`.

## 2. Slack 알림 — 무엇이 언제 오나

새 서비스나 새 발신자는 없다. 이미 `#alerts` 로 보내는 Alertmanager 가 보낸다(봇 토큰도 그쪽에 이미 있다,
`compose.observability.yml`).

```text
foundation-api /metrics ── foundation_scheduled_job_problem{job_id,kind,node_id,summary,link} 1
        │  (Prometheus 15초마다 수집)
        ▼
규칙 FoundationScheduledJobProblem (for 1m, keep_firing_for 15m)
        ▼
Alertmanager  route service="data-flow" ── 작업·종류마다 시작 1번, 해소 1번
        ▼
Slack #alerts   "FIRING FoundationScheduledJobProblem / <작업> [failed] 메모리 부족 · <로그 한 줄> — <더니어 링크>"
```

| 상황 | Slack |
|---|---|
| 문제가 새로 생김(1분 지속) | `FIRING` 한 번 |
| 같은 문제가 계속됨 | 아무것도 안 옴(`repeat_interval: 8760h`). 단, 알림 기록 보관(120h)이 지나면 한 번 더 |
| 문제가 사라짐(15분 동안 다시 안 보임) | `RESOLVED` 한 번 |
| 새 실행이 다른 원인으로 실패 | 원인 줄이 바뀌므로 새 문제로 `FIRING`, 옛 것은 `RESOLVED` |
| 앞 작업 실패로 멈춘(`blocked`) 작업 | 실패 알림이 떠 있는 동안 보내지 않는다(`inhibit_rules`) |
| 꺼진 작업 | 보내지 않는다 |
| 지도가 실행 기록을 못 읽음 | 문제 시계열을 내지 않는다. 30분 지속되면 `FoundationScheduledJobsUnreadable` 한 번 |

`systemd` 의 `OnFailure` 알림(`notify-unit-failure.sh`, "<유닛> 실패")은 그대로다. 그건 유닛이 죽었다는 사실을,
이 알림은 지도가 읽은 원인 한 줄과 링크를 준다.

## 3. 확인

```bash
# 시계열이 나오는지(문제가 없으면 readable 1 한 줄만)
curl -s 127.0.0.1:<foundation-api 포트>/metrics | grep -E '^foundation_scheduled_job'
# Prometheus 가 규칙을 읽었는지
curl -s 127.0.0.1:19090/api/v1/rules | grep -o FoundationScheduledJobProblem | head -1
# Alertmanager 가 지금 들고 있는 지도 알림
curl -s '127.0.0.1:19093/api/v2/alerts?filter=service%3D%22data-flow%22'
```

설정을 바꾼 뒤 Prometheus·Alertmanager 는 다시 읽어야 한다: `curl -X POST 127.0.0.1:19090/-/reload`,
`curl -X POST 127.0.0.1:19093/-/reload`.
