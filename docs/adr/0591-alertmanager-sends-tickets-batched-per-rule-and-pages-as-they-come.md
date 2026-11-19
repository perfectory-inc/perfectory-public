# ADR 0591: Alertmanager 는 티켓을 규칙마다 묶어 보내고, 페이지는 오는 대로 보낸다 — #alerts 하루 480통을 읽을 수 있는 채널로

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0208](./0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md) 결정 4(데이터 흐름 문제의 작업별 경로),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md) (짧은 결과 없음 티켓의 12시간 재알림 → 묶음),
  [ADR-0458](./0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md) (이중 실행의 티켓 → 묶음)
- Related: [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md) (서빙 신선도 알림)
- Related: [ADR-0278](./0278-security-advisories-reach-slack-through-the-existing-alertmanager.md) (보안 권고는 그대로),
  [ADR-0445](./0445-a-failed-host-unit-names-its-owner-and-runbook-and-a-unit-that-stays-failed-calls-its-owner.md) (12시간 재호출),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md) (결과 없음 알림)

## Context

`#alerts` 는 사장이 쓰는 Slack 채널이다(ADR-0278). 받는 쪽은 서버의 Alertmanager 0.28.1 하나다. 수신자도 `prelaunch-audit` 하나이고,
모든 알림이 `send_resolved: true` 로 간다. 2026-11-19 서버에서 읽기만 해서 잰 값:

| 사실 | 값 |
|---|---|
| Slack 으로 보낸 수 | `alertmanager_notifications_total{integration="slack"}` 2,405 / 컨테이너 5일, 실패 0. 하루 약 480통 |
| 지금 떠 있는 알림 | 37개 |
| 측정 창 | 11-18 21:23Z..11-19 15:23Z (Prometheus 재시작 뒤 18시간). `ALERTS` 를 60초 간격으로 다시 그려 규칙마다 일화(episode)를 셌다 |
| 규칙별 (억제된 `blocked` 를 뺀 모형 추정 / 18h) | `FoundationScheduledJobProblem`(ticket) 65 (시계열 38, 일화 47), `FoundationJobOutcomeMissing`(ticket) 52, `FoundationUnitFailedLong`(page) 40, `FoundationReleaseMissing`(ticket) 18, `FoundationHostBehindMain`(ticket) 16, `FoundationJobOutcomeMissingLong`(page) 15, 나머지 규칙 각 15 이하 |
| `blocked` | 85 시계열(38 은 "앞 단계가 실패해서 멈춰 있어요 (source_sweep)"). source_sweep 의 `kind="failed"` 가 같은 시간(18T21:37..19T03:17) 떠 있어 기존 억제가 막았다. Prometheus 의 `ALERTS` 는 억제를 보이지 않으므로 세지 않는다 |
| 레이블이 바뀌어 새로 생긴 알림 | `summary`·`link` 가 레이블이라 문장이 바뀌면 새 알림이다. 123 중 31(예: court_auction_silver 14개, "보통 104133행인데 이번엔 30434행") |
| `FoundationJobMissedStartNotTaken` | 16개 모두 `pool="spark"`, 4.6..10.8시간 늦음. 원인 하나(W82b 가 11-19 15:30Z 부터 맡음) |
| 페이지의 재알림 | serving-freshness 4시간마다. 11-14 부터 떠 있는 bake 셋이 하루 18통 |

모형 추정 합계(억제된 blocked 제외)는 약 326 / 18h 다. 5일 평균 실제 계수는 약 360 / 18h 이고, 11-19 15:23..15:37Z 의 실제는 14분에 2통(하루 약 200통 속도)이다. 그래서 순위로만 쓴다. Alertmanager 에는 규칙별 발송 계수가 없다.

결론: 새 줄이 뜻을 갖지 못한다. 한 규칙이 아니라 여러 티켓 규칙이 항목마다 한 통씩, 해소마다 또 한 통씩 보낸다(티켓이 추정의 약 3분의 2). 페이지도 오래 열린 항목이 4시간마다 반복된다.

Alertmanager 0.28.1 문서(`docs/configuration.md` @v0.28.1)에서 확인한 것:
- `group_by` 는 알림을 묶는 레이블이다.
- `group_interval` 은 "이미 보낸 묶음에 새 알림이 더해졌을 때 다시 보내기까지 기다리는 시간"이다.
- `repeat_interval` 은 같은 알림을 다시 보내기까지의 시간이다.
- `slack_configs.send_resolved` 의 기본값은 false 다.
- 억제(`inhibit_rules`)의 `equal` 에서 "양쪽에 다 없는 레이블은 같은 것으로 본다". 그래서 빈 `equal` 은 원인이 하나라도 있으면 모든 대상을 억제한다.

## Decision

1. **티켓은 규칙마다 묶는다.** 첫 하위 경로가 `severity = "ticket"` 이고 `service != "security"` 인 알림을 받는다.
   - 수신자는 `slack-ticket` 이다. 같은 채널, 같은 봇이다.
   - `group_by: [alertname]`, `group_wait: 5m`, `group_interval: 1h`, `repeat_interval: 24h`.
   - 한 통이 그 규칙의 떠 있는 알림을 한 줄씩 적고, 그 사이 해소된 것을 "해소 N:" 으로 적는다. 묶음이 바뀌면(더해지거나 해소되면) 많아야 한 시간에 한 번, 그대로면 하루에 한 번이다.
   - **해소는 켠다**(`send_resolved: true`). 끄면 v0.28.1 `notify/notify.go` 의 `DedupStage.needsUpdate` 때문에 두 가지가 생긴다.
     - 묶음에서 빠지는 알림이 아무것도 보내지 않는다. 지난 메시지가 하루 동안 사라진 항목을 적고 있다.
     - 같은 알림이 다시 떠도, 지난 메시지의 firing 집합 안에 있으므로 하루 재알림 때까지 보내지 않는다.
   - 티켓에 속한 규칙: 운영 지도(ADR-0195), 짧은 결과 없음(ADR-0480), 이중 실행의 티켓(ADR-0458), 배포의 티켓, 서빙 신선도·foundation-api·호스트의 티켓.
   - 이 경로가 모든 서비스 경로보다 위라, 그 규칙들의 서비스별 재알림(12h 등)은 이 묶음의 24h 로 바뀐다.
   - ADR-0208 결정 4 의 작업별 경로(`service="data-flow"`, 작업·종류마다, `8760h`)는 지운다. 그 규칙들은 모두 티켓이라 이 경로로 온다.
2. **보안 권고는 그대로다.** ADR-0278 의 권고마다 한 통, 재알림 없음을 지킨다. 그래서 `service != "security"` 다.
3. **페이지는 그대로다.** 항목마다 한 통이고 해소도 보낸다.
   - 바꾸는 것은 serving-freshness 재알림 하나다. 4h 에서 12h 로 바꿔, 유닛이 계속 실패할 때 부르는 주기(ADR-0445, unit-state 12h)와 맞춘다.
   - 4h 는 a6ed902d(2026-11-05, 공매 191시간 정지 후속)가 정했다. 까닭은 "하루 한 번 재알림이 8일 동안 읽히지 않았다"(10-28..11-05)였고, 그 원인은 채널의 흘러넘침이었다. 이 ADR 이 그 흘러넘침을 줄인다.
4. **원인이 이미 알리는 증상은 억제한다. 원인과 증상을 잇는 레이블이 있을 때만이다.**
   - `FoundationJobOutcomeMissingLong` 이 떠 있으면 같은 `job_id`·`expectation` 의 `FoundationJobOutcomeMissing` 을 억제한다.
   - `FoundationPlatformApiScrapeDown` 이 떠 있으면 `FoundationSecurityAdvisoriesUnreadable` 을 억제한다. foundation-api 가 그 계열의 유일한 내보내는 곳이고 수집 대상도 하나라, 이을 레이블이 필요 없다.
   - 있는 억제 `failed`→`blocked` 는 `equal: [alertname]` 이었다. 양쪽 다 `alertname` 이 같으므로 빈 `equal` 과 같아, 어느 작업이든 실패하면 모든 `blocked` 가 조용해졌다. 이제 `equal: [failed_job]` 으로 좁힌다. foundation-api(`alerts.rs`)가 문제마다 `failed_job` 레이블을 낸다. 실패는 자기 작업이고, 막힌 작업은 기다리는 실패한 앞 작업 중 첫째(이름 순)다. 그래서 실패가 그 뒤에 막힌 작업만 조용하게 한다.
   - `blocked` 는 더 억제하지 않는다. 앞 작업의 `kind="failed"` 가 떠 있으면 기존 억제가 이미 막는다(11-18/19 의 source_sweep 뒤 76개). `FoundationUnitFailedLong` → `blocked` 는 넣지 않는다. 빈 `equal` 은 어느 유닛이든 실패하면 모든 `blocked` 를 억제한다. 남는 `blocked`(앞 작업의 실패 알림이 없는 것)는 결정 1 의 묶음으로 준다.
5. **바로 읽어야 하는 티켓 셋을 페이지로 올린다**(위 "티켓인데 바로 읽어야 하는 규칙"):
   - `FoundationHostDiskAlmostFull`: 디스크가 다 차 운영 DB 가 선 일이 두 번 있다.
   - `FoundationDeployDrainLong`, `FoundationDeployDagsPausedLong`: 그동안 모든 DAG 이 멈춘다. 이를 덮는 페이지가 없다.
   - 묶음에 들어가면 같은 규칙의 둘째 항목이 한 시간 늦을 수 있다.
6. **사일런스는 이름 있는 열린 PR 이 맡은 항목에만 건다.** 만료는 72시간 이하, 주석은 그 PR 번호다. 아무도 맡지 않은 것에는 걸지 않는다. 이 ADR 은 사일런스를 하나도 걸지 않는다(11-19 기준 그 조건을 채운 항목이 없다).
7. **경로는 CI 에서 증명한다.**
   - 경로마다 수신자 이름이 따로다(`slack-ticket`, `slack-security`, `slack-unit-state` …, 뿌리는 `prelaunch-audit`). Slack 설정은 YAML 앵커 하나라 목적지는 하나다. 그래서 수신자 이름이 경로를 가리킨다.
   - 가드 `alertmanager-routes-pass-their-tests` 가 고정된 Alertmanager 릴리스의 `amtool` 로 세 가지를 돌린다.
     - `check-config`.
     - `alertmanager.routes.test` 의 사례마다 `config routes test --verify.receivers`(21개, 경로마다 하나 이상).
     - **모든 알림 규칙**: `severity: ticket` 이고 security 가 아니면 `slack-ticket`, 아니면 `slack-ticket` 이 아니어야 한다. `severity` 가 page·ticket 이 아닌 규칙은 실패다(76개 규칙).
   - `amtool` 버전(`tools/amtool.env`)은 compose 의 Alertmanager 이미지와 같아야 한다.
   - 자기시험이 일곱 가지를 심어 거부되는지 본다. 티켓 matcher 오타, 티켓 경로 위에 서비스 경로, 틀린 수신자 사례, 없는 수신자, 규칙의 severity 오타, 페이지까지 묶는 matcher, 버전 어긋남.
   - 증명하지 않는 것: 간격(`group_*`, `repeat_interval`)과 `send_resolved`. amtool 이 경로만 푼다.
   - 막는 사고: matcher 오타나 경로 순서 하나로 티켓이 다시 작업마다 한 통이 되어도 저장소에서는 아무것도 말하지 않는 일.

## Rejected

| 대안 | 판정 |
|---|---|
| 티켓을 다른 채널로 | 가장 깨끗하다(#alerts = 페이지만). 그러나 채널을 만들고 봇을 초대하는 사장 손일이 든다. 나중에 수신자의 `channel` 한 줄로 옮긴다 |
| `UnitFailedLong` → `blocked` 를 빈 `equal` 로 억제 | 어느 유닛이든 실패하면 모든 막힌 작업이 조용해진다. 관계없는 막힘도 숨는다 |
| `mute_time_intervals` 로 밤에 티켓 끄기 | 문서상 꺼진 시간의 알림은 나중에 보내지 않는다. 묶음이 같은 효과를 잃지 않고 낸다 |
| Prometheus `metric_relabel_configs` 로 `summary` 레이블 지우기 | 알림이 원인 문장을 잃는다. 고칠 곳은 내보내는 쪽이다(후속) |
| 새 코드(다이제스트 봇) | Alertmanager 의 묶음·간격·억제로 된다. 발신자를 늘리지 않는다(ADR-0278) |

## Consequences

- 예상: 티켓은 규칙마다 한 시간에 많아야 한 통이다. 해소도 그 한 통에 든다(추정 약 215 → 많아야 약 60 / 18h). 증상 둘은 억제된다. 페이지는 serving-freshness 12h 로 약 110 → 약 80 / 18h. 합치면 하루 약 435통(추정)이 약 180통 이하로 준다. 배포 뒤 `alertmanager_notifications_total` 의 하루 증가로 잰다.
  - 아직도 "새 줄이 뜻을 갖는" 수준은 아니다. 남는 것은 열린 페이지 자체다(UnitFailedLong 넷, 서빙 지연). 그것들은 고쳐야 줄어든다.
- 티켓은 늦게 온다. 첫 통은 5분 뒤, 묶음이 바뀌면 한 시간 뒤다. 바로 읽어야 할 것은 페이지여야 한다. 티켓인데 바로 읽어야 하는 규칙은 `severity: page` 로 올린다.
- 운영 지도 런북(`operations-map.md`)의 Slack 표를 이 경로에 맞게 고쳤다.
- 후속:
  - `foundation_scheduled_job_problem` 의 `summary`·`link` 를 레이블에서 빼어, 문장이 바뀌어도 같은 알림이게 한다.
  - 페이지 자체를 고친다(UnitFailedLong·서빙 지연). 그것이 남는 몫의 대부분이다.
