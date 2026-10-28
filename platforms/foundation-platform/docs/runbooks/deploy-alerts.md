---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-28
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

## 울렸을 때

1. 무엇을 기다리는지: `journalctl -u foundation-autodeploy.service -n 50`. 드레인이면 마지막 줄의 작업이 그것이다.
2. 지금 상태: `cat /var/lib/perfectory/metrics/deploy.prom`. 줄이 없으면 배포도 멈춤도 없다.
3. 배포가 끝났는데 `foundation_deploy_dags_paused_since_timestamp_seconds` 가 남아 있으면, 배포가 DAG 을 멈춘 채
   끝난 것이다(전환 중 실패). 배포 로그의 대문자 줄을 읽고, 릴리스가 정상이면 `orchestration/jobs.v1.json` 의 켜진
   작업을 unpause 한다. 다음 배포의 0단계가 파일을 새로 쓴다.

## 확인

- `curl -s localhost:19090/api/v1/query --data-urlencode 'query=up{job="node-exporter"}'` 가 1.
- 배포 중 `deploy.prom` 에 `phase="drain"` 등 한 줄이 보이고, 끝나면 HELP/TYPE 줄만 남는다.
