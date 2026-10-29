---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-30
---

# 배포 경보 — 드레인·배포·DAG 멈춤이 길 때

[루트 ADR-0321 §5](../../../../docs/adr/0321-a-gold-commit-records-the-code-that-built-it.md) (ADR-0301 §3 의 구현).
배포(`scripts/deploy/foundation-deploy.sh`)는 단계마다 `/var/lib/perfectory/metrics/deploy.prom` 을 통째로 바꿔 쓴다.
`node-exporter`(textfile 수집기만, `compose.observability.yml`)가 그 파일을 Prometheus 에 내주고,
`infra/observability/prometheus/foundation-api.rules.yml` 의 `FoundationDeploy*` 규칙이 Slack `#alerts` 로 보낸다.

| 경보 | 뜻 | 기준 |
|---|---|---|
| `FoundationDeployDrainLong` | 0단계(드레인)가 끝나지 않음 — 모든 DAG 이 멈춰 있음 | 20분 |
| `FoundationDeployLong` | 배포 하나가 끝나지 않음 | 1시간 |
| `FoundationDeployDagsPausedLong` | 배포가 DAG 을 멈춰 둠(`scope=all` 전부, `scope=held` 이 릴리스가 잡은 것) | all 30분, held 1시간 |
| `FoundationDeployMetricsUnread` | 위 셋이 울릴 수 없음(node-exporter 가 없거나 파일을 못 읽음) | 30분 |

기준은 2026-10-27 실측(ADR-0301 1단계 뒤 배포 9회: 드레인 1.1–2.6분, 전부 멈춤 2.9–4.5분, held 11.3–14.5분, 전체
11.7–14.9분)의 몇 배다.

## main 이 호스트에 닿지 않을 때 ([루트 ADR-0345](../../../../docs/adr/0345-the-release-image-is-built-before-merge-and-a-host-behind-main-alerts-with-its-reason.md))

2026-10-29: #939 부터 main 의 릴리스 빌드가 전부 실패했다. 자동 배포는 매분 "no commit … has a published
release yet" 만 남겼고, 호스트는 약 11시간 a879a889 에 머물렀으며 병합된 PR 약 25개(보안 수정 포함)가 운영에
닿지 않았다. 위 경보들은 배포가 **도는 동안**만 지표가 있어서 아무것도 울리지 않았다.

자동 배포(`scripts/deploy/foundation-autodeploy.sh`)는 이제 잠금을 잡은 틱마다
`/var/lib/perfectory/metrics/autodeploy.prom` 을 통째로 바꿔 쓴다.

| 경보 | 뜻 | 기준 |
|---|---|---|
| `FoundationReleaseMissing` | 가장 새 릴리스보다 새로운, 릴리스 범위(ADR-0309) 안의 커밋 중 가장 오래된 것(`commit`)이 main 에 들어온 지 오래됐는데 릴리스가 없다 — **릴리스 빌드 실패** | 90분 |
| `FoundationHostBehindMain` | 호스트가 돌리지 않는 범위 안 커밋 중 가장 오래된 것(`commit`)이 main 에 들어온 지 오래됐다. `reason` 이 이유 | 6시간 |
| `FoundationAutodeployStopped` | 자동 배포 틱이 지표를 남기지 않는다(배포 중은 제외) — 위 둘이 울릴 수 없다 | 30분 |

기준의 근거(실측): main 릴리스 빌드 140회(10-18~10-29) p50 15분, p99 60분, 최장 62분. 배포 288회(10-07~10-29)
동안 호스트가 main 에 뒤처진 시간 p50 1.1시간, p95 4.2시간, p99 7.5시간(긴 것은 bake 가 배포를 잡은 경우).

`reason` 값:

| reason | 뜻 | 할 일 |
|---|---|---|
| `no_release` | 배포할 커밋에 릴리스가 없다 | Actions 의 foundation-release-build 실행을 본다. 실패면 고친다 |
| `checks_pending` / `checks_unreadable` | 체크가 아직 돌거나 GitHub 를 못 읽는다 | 대개 저절로 풀린다 |
| `checks_failed` | 체크가 실패해 그 커밋은 영구 거부 | 새 커밋이 길이다 |
| `deploy_failed` / `every_newer_commit_refused_or_failed` | 배포가 실패했거나 뒤의 커밋이 전부 거부·실패 | `journalctl -u foundation-autodeploy.service` 의 배포 로그 |
| `deploying` | 배포 중 | `FoundationDeployLong` 이 함께 본다 |
| `quiet_window` / `operator_unit` / `worker_deploy` | 조용한 창, 운영자 유닛(bake 등), Worker 배포를 기다림 | 운영자 유닛이 오래면 그 작업을 본다 |
| `off` | `/etc/foundation-platform/autodeploy.off` 가 있다 | 의도한 것인지 확인 |
| `main_unreadable` / `releases_unreadable` / `systemd_unreadable` | GitHub·레지스트리·systemd 를 못 읽는다 | 네트워크·systemd 상태 |

`off`·`main_unreadable` 틱은 main 의 커밋 목록을 읽기 전에 끝나므로 뒤처짐 시계를 남기지 않는다. 그 동안은
`FoundationAutodeployStopped` 가 아니라 틱 지표의 `reason` 으로만 보인다.

## 울렸을 때

1. 무엇을 기다리는지: `journalctl -u foundation-autodeploy.service -n 50`. 드레인이면 마지막 줄의 작업이 그것이다.
2. 지금 상태: `cat /var/lib/perfectory/metrics/deploy.prom`. 줄이 없으면 배포도 멈춤도 없다.
3. 배포가 끝났는데 `foundation_deploy_dags_paused_since_timestamp_seconds` 가 남아 있으면, 배포가 DAG 을 멈춘 채
   끝난 것이다(전환 중 실패). 배포 로그의 대문자 줄을 읽고, 릴리스가 정상이면 `orchestration/jobs.v1.json` 의 켜진
   작업을 unpause 한다. 다음 배포의 0단계가 파일을 새로 쓴다.

## 확인

- `cat /var/lib/perfectory/metrics/autodeploy.prom` 에 틱 줄이 1분 안의 시각으로 있다.

- `curl -s localhost:19090/api/v1/query --data-urlencode 'query=up{job="node-exporter"}'` 가 1.
- 배포 중 `deploy.prom` 에 `phase="drain"` 등 한 줄이 보이고, 끝나면 HELP/TYPE 줄만 남는다.
