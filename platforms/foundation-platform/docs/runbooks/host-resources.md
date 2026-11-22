---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-19
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

## 유닛과 컨테이너의 한도 — 측정으로 정한다

[루트 ADR-0574](../../../../docs/adr/0574-every-limit-is-set-from-measurement-and-a-unit-near-its-limit-is-named-before-it-fails.md).
규칙은 `foundation-unit-limits` 무리, 시험은 `foundation-unit-limits.rules.test.yml`. `foundation-unit-limits.timer` 가 30분마다
`scripts/ops/unit_limits.py --write` 로 systemd 저널의 run 을 재어 `unit-limits.prom` 을 쓴다.

| 경보 | 뜻 | 먼저 볼 것 |
|---|---|---|
| `FoundationUnitNearItsLimit` | 유닛의 최근 run 3번 이상이 연달아 메모리나 시간 한도의 80% 를 넘음 | 아래 표를 다시 재고, 입력이 자랐는지(흘려 처리로 고친다) 한도가 측정보다 작은지(예산과 함께 고친다) 가른다 |
| `FoundationContainerMemoryNearCapLong` | 컨테이너가 하루의 절반 넘게 상한의 90% 위(page) | `journalctl -k --since -1d \| grep oom_memcg` 로 죽은 횟수; 상주 서비스면 무엇이 자랐는지(ADR-0573 의 아레나처럼) |
| `FoundationUnitLimitsUnread` | 측정 파일이 두 시간째 없음 | `journalctl -u foundation-unit-limits.service` |

표를 다시 만든다(호스트에서, 읽기만; 14일 저널 25초, 최대 RSS 129 MB):

```sh
python3 /opt/foundation-platform/current/scripts/ops/unit_limits.py --since=-14d --table --prometheus http://127.0.0.1:19090
```

`peak` 는 systemd 가 유닛이 끝날 때 남기는 cgroup `memory.peak` 이고 페이지 캐시를 포함한다. 최고점이 `MemoryHigh`
(상한의 0.9)에 붙어 있고 OOM 이 없으면 커널이 캐시를 되찾은 것이다. 상한에서 죽은 run 은 resource 줄을 남기지 않으므로
그 최고점은 상한 이상이다 — 그 유닛은 측정이 아니라 원인(입력이 자랐나)부터 본다. 경보가 세는 메모리 근접은 쥔 최고점뿐이다:
OOM 으로 죽었거나 `MemoryHigh` 를 커널의 회수 여유(CPU 마다 64 쪽, ai-server 5 MiB)보다 넘은 run(ADR-0574 §1). `--json` 의
`held_memory_share` 가 그 값이고, `memory_share` 는 캐시까지 센 원래 비율이다.

한도를 바꾸는 곳은 작업 정의 하나다(ADR-0574 §8): `definitions/<무리>.definition.json` 의 그 작업에 `time_limit_minutes`
옆의 `memory_max`·`memory_measured`(·`memory_measured_peak`·`memory_headroom_why`). 템플릿(`x@.service`)의 인스턴스 작업은
제 값을 적을 수 있다; 템플릿보다 작으면 생성기가 `infra/systemd/<instance>.service.d/50-time-limit.conf`·`50-memory.conf` 를
쓴다. 고친 뒤:

```sh
bash scripts/ci/generate-declared.sh                     # 저장소 뿌리에서: 작업 목록과 메모리 계약
python3 scripts/ci/unit_memory.py --write                # 유닛과 인스턴스 드롭인의 메모리 줄
cargo run -q -p foundation-jobs -- generate              # platforms/foundation-platform 에서: TimeoutStartSec 과 그 드롭인
bash scripts/guard/every-container-has-a-memory-cap.sh   # 합이 호스트에 드는지
```

배포는 드롭인을 깔고 `daemon-reload` 한다; 새 한도는 그 인스턴스의 다음 시작부터다. 드롭인을 모르는 이전 릴리스로 되돌렸다면
남은 드롭인을 지운다(그 인스턴스는 템플릿 값으로 돌아간다):

```sh
sudo rm -f /etc/systemd/system/foundation-*@*.service.d/50-time-limit.conf /etc/systemd/system/foundation-*@*.service.d/50-memory.conf
sudo systemctl daemon-reload
```
