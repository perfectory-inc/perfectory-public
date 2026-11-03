# ADR 0385: 호스트는 디스크·메모리·상한이 바닥날 때 스스로 말하고, 그 규칙은 promtool 로 시험된다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md)·[ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(위생 C·J 계열 P1 "호스트 자원 경보"), [ADR-0343](./0343-an-always-on-service-holds-only-what-it-is-using.md) §6·실행표 3번(cAdvisor 와 node-exporter 수집기), [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md) §6(모든 컨테이너 상한), [ADR-0197](./0197-ci-downloads-pinned-release-binaries-only-through-one-cached-retrying-fetcher.md)(고정 바이너리 한 통로), [ADR-0001](./0001-monorepo-governance-and-conventions.md)(가드는 막는 사고를 댄다)

## Context

ai-server 의 node-exporter 는 textfile 수집기만 켜 있었다(`--collector.disable-defaults --collector.textfile`).
디스크·메모리·OOM 은 어떤 시계열도 아니었고, 컨테이너별 작업 집합은 아예 수집되지 않았다(cAdvisor 없음).
그래서 아래 사고는 모두 사람이 손으로 찾았다.

| 날짜 | 사고 | 알아챈 방법 |
|---|---|---|
| 2026-10-01 | 상한 없는 Trino 가 한가할 때 62GB 중 27.7GB 를 잡음 | 손으로 `docker stats` |
| 2026-10-02 | 상한 없는 json-file 로그 하나가 32GB 가 되어 운영 DB 아래 루트 디스크를 두 번 채움 | 장애 뒤 |
| 2026-10-28 | by-PNU 굽기 유닛 `MemoryPeak` 14.17GB, `MemoryMax=14G` | ADR-0343 조사 |
| 2026-10-28 ~ 11-03 | 커널이 프로세스 100개를 죽임: 99개가 2.5GiB 컨테이너 상한의 Spark java, `foundation-onbid-silver-load.service` 가 137 로 65번 끝남, 11-02 하루 39번 | 이 ADR 의 측정 |

마지막 줄은 이 ADR 을 쓰며 처음 드러났다. 유닛 실패 알림은 "실패"만 말하고 OOM 이라고 말하지 않았다.

호스트 측정(2026-11-03 15:42 UTC, 읽기 전용 `ssh ai-server`):

| 항목 | 값 |
|---|---|
| `/` (ext4) | 232GB 중 35% 사용, 144GB 남음 |
| `/data` (ext4) | 1.97TB 중 47% 사용, 1.06TB 남음 |
| 메모리 | 67.2GB 중 34.9GB 사용 가능(52%), swap 4.29GB 중 3.83GB 사용 |
| `/proc/vmstat oom_kill` | 112 (부팅 130일) |
| 상한 대비 작업 집합 최대 | 소유권 Gold 재구축 컨테이너 0.87(17.4/20GiB), datahub-opensearch 0.80 |
| cgroup | v2, systemd 드라이버(`/system.slice/docker-<id>.scope`), 파일은 누구나 읽기 가능(0444) |
| 메모리 예산 가드 | 61.02g / 62g → cAdvisor 192m 를 더해 61.20g |

큰 곳의 방식: Kubernetes 는 kubelet 안의 cAdvisor 로 컨테이너 작업 집합을, node_exporter 로 호스트를 재고,
경보는 node-mixin(`NodeFilesystemAlmostOutOfSpace`·`NodeFilesystemSpaceFillingUp`)과 kubernetes-mixin 이 쓴다.
두 mixin 은 모든 규칙을 `promtool test rules` 로 시험한다. 이 저장소는 규칙을 문자열로만 확인했다
(`deploy_contract.rs`, `test_asset_reconcile.py`): 지표 이름을 잘못 쓰거나 문턱을 틀리게 쓰면 영원히 울리지
않는 규칙이 되는데, 그것은 사고 때의 침묵과 같다.

## Decision

1. **node-exporter 는 호스트 수집기를 켠다.** `filesystem`·`meminfo`·`vmstat`(`oom_kill`)·`cpu`. 호스트 루트를
   `/host` 에 읽기 전용(`rslave`)으로 붙이고 `--path.rootfs=/host --path.procfs=/host/proc` 으로 호스트의
   마운트를 읽는다. `pid: host`·`network_mode: host` 는 쓰지 않는다(`container-runtime-policy` 가 금지).
2. **cAdvisor 를 더한다.** `ghcr.io/google/cadvisor:v0.60.6`(digest 고정), 65534 사용자, 읽기 전용, 192m,
   `/sys` 읽기 전용만 붙인다. 엔진 소켓은 붙이지 않는다(정책이 금지하고, 붙이면 호스트 root 와 같다).
   `--docker_only=false --raw_cgroup_prefix_whitelist=/system.slice/` 로 컨테이너와 systemd 유닛을 같은 길로
   읽고, `--enable_metrics=cpu,memory` 로 그 둘만 남긴다. 소켓이 없으니 컨테이너는 cgroup id 로만 보인다:
   자산 조정기(`asset_reconcile.py`, 5분)가 `docker ps` 로 id→이름 표(`docker-containers.prom`)를 쓰고,
   경보 문구가 그 표에서 이름을 찾는다.
3. **경보 여섯 개(`foundation-host` 무리, `service: host`)가 기존 Alertmanager → Slack 기본 경로로 간다.**

   | 경보 | 식 | 지속 | 막는 사고 |
   |---|---|---|---|
   | `FoundationHostDiskAlmostFull` | ext4/xfs 남은 비율 < 10% | 30분 | 10-02 로그가 루트 디스크를 채움 |
   | `FoundationHostDiskFillingUp` | 6시간 기울기로 24시간 안에 0, 남은 비율 < 40% | 1시간 | 같은 사고를 하루 먼저 |
   | `FoundationHostMemoryLow` | `MemAvailable/MemTotal` < 10% | 15분 | 10-01 Trino 27.7GB |
   | `FoundationHostOomKill` | `increase(node_vmstat_oom_kill[1h]) > 0` | — | 10-28~11-03 OOM 100번 |
   | `FoundationContainerMemoryNearCap` | 작업 집합 / 상한(0 은 상한 없음이라 제외) > 0.9 | 15분 | 10-28 굽기 14.17/14GB |
   | `FoundationHostMetricsUnread` | cAdvisor 가 안 뜨거나 호스트 수집기 시계열이 없음 | 30분 | 위 다섯이 조용히 못 울림 |

   문턱은 node-mixin 값을 따르되 위 측정에 맞췄다(지금 0.87 인 컨테이너는 0.9 아래, 루트 35% 사용은 10% 남음과 멀다).
   OOM 경보는 한 시간 안에 죽은 것이 있으면 울리고 한 시간 동안 없으면 풀린다: 시간당 한 번 죽던 공매 Silver 는
   한 번 알리고 기본 경로의 4시간마다 다시 알린다.
4. **가드 `prometheus-rules-pass-their-tests`.** 모든 `*.rules.yml` 은 `promtool check rules` 를, 모든
   `*.rules.test.yml` 은 `promtool test rules` 를 통과한다. 시험이 하나라도 있는 무리는 모든 경보에 시험이 있다
   (새 경보가 시험 없이 들어오지 못한다). promtool 은 고정 릴리스(`tools/promtool.env`, ADR-0197 의 한 통로)이고,
   그 버전은 compose 의 Prometheus 이미지 버전과 같아야 한다(규칙을 평가하는 엔진으로 시험한다).
   `host-resources.rules.test.yml` 이 여섯 경보 각각을 사고의 합성 시계열로 울리고, 건강한 시계열로 조용함을 보인다.
   자기 시험이 문턱 오타·지표 이름 바뀜·시험 없는 새 경보·promtool 버전 어긋남 네 가지를 거부함을 보인다.
   막는 사고: 10-28~11-03 의 OOM 100번처럼, 규칙이 있어도 식이 틀리면 아무도 모른다.

## Consequences

- 상시 서비스가 하나 는다(cAdvisor 192m). 메모리 예산 가드는 61.20g / 62g 로 통과한다.
- Prometheus 시계열이 는다: cgroup 약 60개(유닛 25 + 컨테이너 28 + 루트) × cpu·memory 지표. 15일·1GB 보존 안이다.
- 배포에 필요한 것: 호스트 `/` 가 shared 마운트여야 `rslave` 가 붙는다(측정: `findmnt -no PROPAGATION /` = shared).
  sudo·권한 변경은 필요 없다. 공매 Silver 의 2.5GiB 상한이 시간마다 OOM 인 것은 이 ADR 이 고치지 않는다 —
  첫 경보가 그것을 알릴 것이고, 상한을 잰 값으로 고치는 일은 그 레인의 몫이다.
- swap 사용(89%)과 CPU 포화는 경보하지 않는다. 막을 사고가 아직 없다(ADR-0001).
- 남은 일: 규칙 파일의 다른 무리(foundation-api, deploy 등)도 시험을 갖게 되면 이 가드가 바로 그 무리 전체를 요구한다.
