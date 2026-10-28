# ADR 0208: 데이터 흐름 지도의 문제는 이미 있는 Alertmanager 로 Slack 에 간다

- Status: Accepted
- Date: 2026-10-13
- Related: [ADR-0195](./0195-dawneer-shows-every-jobs-health-on-the-data-flow-map.md) (지도의 다섯 신호), [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md) (유닛 실패 알림)

## Context

ADR-0195 로 지도는 작업마다 실패·멈춤·늦음·느림·양 이상을 계산하고 원인 한 줄을 붙인다. 하지만 아무도 화면을 열지
않으면 아무도 모른다. 이미 Slack `#alerts` 로 가는 길이 두 개 있다.

- `systemd` `OnFailure=foundation-unit-failed@%n.service` → `notify-unit-failure.sh`: 유닛이 죽었다는 사실만,
  죽을 때마다 한 번. 원인·늦음·느림·양은 모른다.
- Prometheus → Alertmanager(`infra/observability`): 봇 토큰이 이미 마운트돼 있고, 2026-10-13 서버에서 둘 다 돌고 있다.

요구는 "상태가 바뀔 때만, 원인 한 줄과 노드 링크로" 이다.

## Decision

1. **발신은 Alertmanager.** foundation-api 의 `/metrics` 가 지도의 문제를 시계열로 낸다:
   `foundation_scheduled_job_problem{job_id,kind,node_id,summary,link} 1`(켜진 작업만), 그리고
   `foundation_scheduled_jobs_readable`. 계산은 지도와 같은 함수(`operations::jobs_status`)를 부른다 — 두 번째 판정은 없다.
2. **"바뀔 때만"은 레이블의 안정성으로 보장한다.** 레이블은 같은 문제인 동안 바뀌지 않는다: 도는 중인 느린 작업의
   경과 초, "몇 분 전" 같은 값은 넣지 않는다. 새 실행이 원인을 바꾸면 레이블이 바뀌고, 그건 새 문제다.
3. **규칙** `FoundationScheduledJobProblem`: `for: 1m`, `keep_firing_for: 15m` — 재시작·한 번 못 읽은 수집이
   해소→재발로 보이지 않게. 지도가 실행 기록을 못 읽으면 문제 시계열을 내지 않고(모른다), 30분 지속되면
   `FoundationScheduledJobsUnreadable` 하나.
4. **경로** `service="data-flow"`: 작업·종류마다 묶고 `repeat_interval: 8760h` — 시작 한 번, 해소 한 번. 알림 기록 보관
   (`--data.retention=120h`)이 지나면 열린 문제는 한 번 더 온다. **억제**: 실패 알림이 떠 있는 동안 `blocked` 는 보내지 않는다.
5. **링크**는 `FOUNDATION_PLATFORM_DAWNEER_UI_URL`(operations-map.env) + `#/flow?node=<노드>`(노드를 모르면 `?job=`).
   더니어는 그 주소로 열리면 그 패널을 연다.
6. **배포가 규칙을 실제로 읽게 한다.** Prometheus·Alertmanager 는 설정 파일을 `current` 링크로 바인드하는데 Docker 는
   컨테이너를 만들 때 링크를 풀어 버린다. 그래서 규칙을 바꾼 릴리스도 옛 파일을 읽고 있었다. `foundation-release.sh` 의
   `migrate_runtime` 이 둘을 `--force-recreate` 한다(데이터·알림 기록은 볼륨에 있어 잃지 않는다).

## Rejected

- **지도 옆에 작은 발신기 + 상태 파일**(마지막으로 보낸 문제 집합을 저장하고 비교): Alertmanager 가 이미 하는
  중복 제거·해소·묶음·억제를 다시 만든다. 토큰을 또 한 곳에 둬야 한다.
- **foundation-api 가 Slack API 를 직접 호출**: 같은 이유 + 재시작마다 상태를 잃거나, 상태를 위해 DB 표가 생긴다.
- **`notify-unit-failure.sh` 확장**: 유닛 실패 시점에만 돈다. 늦음(돌지도 않음)·느림·양 이상은 유닛이 실패하지 않는다.
- **레이블에 원인을 넣지 않고 주석 템플릿에서 질의**: 정체성은 더 안정하지만 템플릿 안 PromQL 이 시험 불가능해진다.
  원인이 바뀌면 새 알림이 오는 쪽이 운영상 맞다.

## Consequences

- 새 서비스·새 비밀값 없음. 서버에 필요한 사람 손은 `operations-map.env` 하나
  ([런북](../../platforms/foundation-platform/docs/runbooks/operations-map.md)).
- 유닛 실패는 두 알림이 온다(`OnFailure` 의 "유닛 실패"와 이 알림의 원인 한 줄). 앞의 것은 지도가 못 읽을 때의 안전망이다.
- 배포마다 Prometheus·Alertmanager 가 몇 초 재시작한다.

2026-10-28: 구현 확인 — `platforms/foundation-platform/services/foundation-api/src/routes/data_catalog/alerts.rs`, `platforms/foundation-platform/infra/observability/alertmanager/alertmanager.yml`
