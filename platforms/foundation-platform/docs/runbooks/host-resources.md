---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-04
---

# 호스트 자원 경보 — 디스크·메모리·OOM·상한

[루트 ADR-0385](../../../../docs/adr/0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md).
규칙은 `infra/observability/prometheus/foundation-api.rules.yml` 의 `foundation-host` 무리, 시험은 같은 폴더의
`host-resources.rules.test.yml`(가드 `prometheus-rules-pass-their-tests`). 지표는 node-exporter 의 호스트 수집기와
cAdvisor(`compose.observability.yml`)가 쓴다. 모두 기존 Alertmanager → Slack `#alerts` 로 간다.

| 경보 | 뜻 | 먼저 볼 것 |
|---|---|---|
| `FoundationHostDiskAlmostFull` | 디스크가 10% 미만 남음, 30분 | `du -xh --max-depth=2 <마운트>`; Docker 로그·남은 임시 파일·`/data/docker` |
| `FoundationHostDiskFillingUp` | 6시간 속도로 하루 안에 다 참 | 무엇이 커지는지: `du` 를 두 번, 몇 분 간격으로 |
| `FoundationHostMemoryLow` | 사용 가능 메모리 10% 미만, 15분 | `docker stats --no-stream`, `systemctl status ollama`, 손으로 띄운 컨테이너 |
| `FoundationHostOomKill` | 지난 1시간에 커널이 프로세스를 죽임 | `journalctl -k --since -1h \| grep -E "Killed process\|memory: usage"` — `limit` 이 그 상한, `oom_memcg` 가 그 cgroup |
| `FoundationContainerMemoryNearCap` | 작업 집합이 상한의 90% 넘게 15분 | 그 작업의 잰 피크(`memory.peak`)와 상한; 상한은 `tools/host-memory-budget.contract.json` 예산 안에서 고친다 |
| `FoundationHostMetricsUnread` | cAdvisor 나 호스트 수집기 시계열이 30분째 없음 | `docker compose ps cadvisor node-exporter`, 그 로그 |

OOM 의 `limit` 이 `2621440kB`(2.5GiB) 면 공매 Silver 적재(`foundation-onbid-silver-load.service`)다. 2026-10-28 ~ 11-03
그 상한에서 99번 죽었다. 상한은 측정한 피크로 고치고, 같은 PR 에서 메모리 예산 가드를 통과시킨다.

컨테이너는 cgroup id(`/system.slice/docker-<id>.scope`)로 수집된다(cAdvisor 는 엔진 소켓을 붙이지 않는다). 이름은
자산 조정기가 5분마다 쓰는 `/var/lib/foundation-jobs/metrics/docker-containers.prom` 에서 찾는다. 경보 문구에 id 가
나오면 그 컨테이너가 조정기의 마지막 tick 뒤에 생겼거나 이미 끝난 것이다.
