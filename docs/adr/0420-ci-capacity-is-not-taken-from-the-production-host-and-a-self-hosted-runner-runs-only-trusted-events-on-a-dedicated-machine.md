# ADR 0420: CI 용량은 운영 호스트에서 빌리지 않는다 — 자체 러너는 전용 기계에서 신뢰된 이벤트만 돌린다

- Status: Proposed (2026-11-05). 측정이 끝났다. 남은 것은 소유자의 비용 결정 하나다(§4).
- Date: 2026-11-05
- Builds on: [ADR-0007](./0007-public-code-private-operations-boundary.md)(공개 저장소의 CI 는 비밀 없는 GitHub 호스트 러너),
  [ADR-0203](./0203-pull-request-ci-has-one-wall-clock-budget.md)(PR CI 시간 예산),
  [ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md)·[ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(일 없는 작업은 러너를 잡지 않는다),
  [ADR-0335](./0335-batch-compute-leaves-the-serving-host-at-a-declared-trigger.md)(배치는 서빙 호스트를 떠난다),
  [ADR-0385](./0385-the-host-says-when-its-disk-memory-or-a-cap-runs-out-and-each-rule-is-tested-by-promtool.md)(호스트 자원 지표)
- Would change: ADR-0007 의 "공개 workflow 에서 self-hosted runner·변수 label 금지"(§4 의 B 를 고를 때만, 그 조건 아래로 좁힌다)

## Context

2026-11-05 지시: PR 이 30~60분 기다리고, 러너가 BlobNotFound·디스크 부족·JVM CodeCache 오류로 죽는다. 자체 러너로 대기를 없앤다.
CI 지출은 0 을 지킨다. 후보 호스트는 ai-server 다.

### 측정 1 — 대기는 러너 수에서 오고, 몰릴 때만 생긴다

GitHub API 로 최근 완료 실행의 작업마다 `started_at − created_at`(호스트 러너 배정 대기)를 쟀다(`gh api`, 읽기 전용).

| 구간 | 이벤트 | 표본 작업 | 대기 p50 | p90 | p99 | 최대 |
|---|---|---:|---:|---:|---:|---:|
| 최근 1,000 실행, 5개에 1개 표본(약 2026-11-05 03~08 UTC) | pull_request | 206 | 268 s | 1,231 s | 2,557 s | 2,860 s |
| 같음 | merge_group | 132 | 154 s | 683 s | 1,122 s | 1,294 s |
| 같음 | push(main) | 88 | 41 s | 520 s | 688 s | 688 s |
| 한가한 구간(2026-11-05 07:40~08:54 UTC, 150 실행 전수) | pull_request | 138 | 3 s | 4 s | — | 28 s |
| 같음 | merge_group | 76 | 3 s | 17 s | — | 49 s |

- 가장 나쁜 시각은 03~06 UTC 였다. 작업 하나가 최대 47.7분을 기다렸다. 에이전트 PR 이 한꺼번에 열리는 시간대다.
- 한가하면 대기는 수 초다. 그러니 대기의 원인은 러너 하나의 느림이 아니라 **동시 작업 수 상한**이다. GitHub Free 조직은 표준 러너 동시 작업이 20개다([usage limits](https://docs.github.com/actions/learn-github-actions/usage-limits-billing-and-administration)).
- 같은 날 merge queue 를 쟀다. 마지막 투입부터 병합까지 p50 20분, p90 136분, 최대 184분이다(병합된 PR 100개, GraphQL timeline). 큐는 PR 을 차례로 검증하므로, 큐 앞에서 기다린 시간과 실행 시간의 합이 PR 의 체감 대기다.
- 러너 쪽 장애: 단계 없이 실패한 작업(BlobNotFound 등)은 표본 426 작업 중 1개였다.

### 측정 2 — 운영 호스트에는 CI 를 넣을 자리가 없다

ai-server 의 Prometheus(node-exporter, ADR-0385)를 읽기 전용으로 질의했다(2026-11-05, 최근 7일).

| 지표 | 값 |
|---|---:|
| 물리 메모리 | 62 GiB(`tools/host-memory-budget.contract.json` `host.physical_memory`) |
| `MemAvailable` 7일 최저 | **0.9 GiB** |
| `MemAvailable` 7일 5% 분위 | 19.1 GiB |
| OOM kill 7일 | **30번** |
| CPU | 20 vCPU, 그 시각 load average 14 |
| 계약상 최악 조합 | 예약 작업의 메모리 조합만으로 60.95 GiB / 62 GiB(ADR-0333 측정) |

CI 작업 하나(Foundation 작업 공간의 clippy·nextest)는 메모리 수 GiB 와 vCPU 4개를 오래 쓴다. 7일 최저 여유가 0.9 GiB 이고
OOM 이 매일 나는 호스트에 cgroup 상한을 걸어도 결과는 둘 중 하나다. 상한이 작으면 CI 가 죽고, 상한이 크면 운영 작업이 OOM 에 걸린다.
우선순위(`CPUWeight`·`MemoryLow`)를 낮춰도 **메모리는 나눠 쓸 수 없다.**

### 위협 — 공개 저장소의 자체 러너

GitHub 의 강화 지침은 이렇게 말한다: "Self-hosted runners should almost never be used for public repositories on GitHub, because any
user can open pull requests against the repository and compromise the environment"([Secure use reference](https://docs.github.com/en/actions/reference/security/secure-use)).

| 위협 | 경로 | 운영 호스트에서의 결과 |
|---|---|---|
| fork PR 코드 실행 | 외부인이 workflow 의 `runs-on` 을 자체 러너로 바꾼 PR 을 연다 | 러너 사용자 권한으로 임의 코드 실행 |
| 러너 사용자 → 운영 비밀 | 같은 커널·같은 파일시스템, `docker` 그룹(= root 와 같다) | `/etc/foundation-platform/*.env`·R2 키 노출 |
| 잔류물 | 재사용되는 러너에 남은 파일·프로세스·캐시 | 다음 작업(merge_group, main push)이 오염된다 |
| 자원 고갈 | CI 의 메모리·디스크·CPU | 운영 작업 OOM·지연(위 측정) |
| 등록 자격증명 | 러너를 계속 만들려면 조직 관리 자격증명이 호스트에 있어야 한다 | 그 자격증명으로 저장소 설정 변경 |

운영 호스트에는 이미 `docker` 그룹 사용자가 있다. 그리고 CI 작업의 절반 이상(`services:`·compose 스모크·Postgres/Kafka
통합·Spark 런타임·릴리스 빌드)이 Docker 를 쓴다. 그런 작업을 운영 호스트에서 격리하려면 작업마다 별도 커널(VM)이 있어야 한다.
그 VM 의 메모리가 측정 2 의 바로 그 메모리다.

## Decision

1. **CI 는 운영 호스트(ai-server)에서 돌지 않는다.** 측정 2 로 격리를 안전하게 만들 수 없다. 메모리 여유가 없고, 운영 비밀과
   커널을 공유한다. 이 판단은 호스트 자원 계약이 바뀌기 전까지 유지한다(ADR-0335 가 배치를 서빙 호스트 밖으로 옮기면 다시 잰다).
2. **대기의 원인은 동시 작업 상한이다. 그래서 상한을 올리는 것이 먼저다.** 같은 문제에 대한 해법 둘을 비교한다.

   | | A. 조직을 GitHub Team 으로 | B. 전용 CI 기계(자체 러너) |
   |---|---|---|
   | 동시 작업 | 20 → 60(표준 러너, [limits](https://docs.github.com/actions/learn-github-actions/usage-limits-billing-and-administration)) | 20 + 기계의 슬롯(예: Hetzner AX42 8코어·64 GB → vCPU 4·16 GB 슬롯 3개) |
   | 비용(계산) | 사용자 1명 기준 월 $4(연간)·$5(월간) | AX42 월 €57.30~€97.30 + 설치비 €49(2026 년 가격 인상 반영, [AX42](https://www.hetzner.com/dedicated-rootserver/ax42)) |
   | 공개 저장소 위협 | 없음(GitHub 가 작업마다 새 VM) | 아래 §3 의 통제가 모두 필요 |
   | 운영 부담 | 없음 | 기계·이미지·러너 갱신·감시 |
   | 러너 장애(BlobNotFound 등) | 그대로(측정상 0.2%) | 기계 장애가 새로 생긴다 |
   | ADR-0007 | 그대로 | §3 조건 아래로 좁혀야 한다 |

   권장은 **A** 다. 측정된 병목(동시 작업 상한)을 그대로 세 배로 늘린다. 공개 저장소의 실행 경계도 바뀌지 않고, 비용이 가장 작다.
   GitHub 자신의 지침도 공개 저장소에는 자체 러너를 거의 쓰지 말라고 한다(위 위협 절).
3. **B 를 고를 때의 규칙**(이 ADR 이 고정하는 조건이다. 하나라도 빠지면 B 를 켜지 않는다).
   - **기계**: 운영 비밀이 없는 전용 기계. 운영 호스트와 네트워크 신뢰를 공유하지 않는다(ssh 키·VPN 없음).
   - **러너**: JIT 러너만 쓴다(`POST /orgs/{org}/actions/runners/generate-jitconfig`, 작업 하나 뒤 자동 제거,
     [REST](https://docs.github.com/en/rest/actions/self-hosted-runners)). 슬롯마다 작업 하나가 끝나면 컨테이너를 버리고 새로 만든다.
     - 비루트 전용 사용자의 rootless 컨테이너이고, 호스트 마운트는 없다.
     - Docker 가 필요한 작업은 B 의 1단계에 넣지 않는다. 그런 작업은 작업마다 VM(예: Firecracker·KVM 일회용 VM)이 생긴 뒤에만 옮긴다.
   - **등록 자격증명**: 조직 권한 "Self-hosted runners: write" 하나만 가진 GitHub App. 개인 토큰은 쓰지 않는다.
     - 개인키는 기계의 root 소유 파일에 두고, 러너 사용자는 읽지 못한다.
     - 감독기가 1시간짜리 설치 토큰으로 JIT 설정을 그때그때 받는다. 등록 토큰은 저장하지 않는다.
   - **러너 그룹**: 조직 러너 그룹 하나에 이 저장소만 넣는다(`visibility: selected`, `allows_public_repositories: true`).
     `restricted_to_workflows: true` 로 정해진 workflow 만 쓴다. 설정은 `tools/github/` 의 desired-state 파일과 `scripts/github`
     configurator 로만 바꾼다(ADR-0007 의 설정-코드 원칙).
   - **경로**: 자체 러너로 가는 작업은 다음 세 경우뿐이다.
     - `merge_group`
     - main 의 `push`
     - 같은 저장소 브랜치에서 온 `pull_request`(`github.event.pull_request.head.repo.full_name == github.repository`)

     fork PR 은 언제나 GitHub 호스트 러너로 간다. 이것을 두 겹으로 지킨다.
     - workflow 의 `runs-on` 식.
     - 러너의 작업 시작 훅(`ACTIONS_RUNNER_HOOK_JOB_STARTED`): 이벤트 페이로드를 읽고 위 세 경우가 아니면 실패한다.
     - 외부 기여자 PR 의 workflow 실행에는 승인이 필요하다(`tools/github/fork-pr-approval.json` `all_external_contributors`, 이미 적용).
   - **비밀**: 자체 러너 작업은 ADR-0007 그대로 repository secret 을 받지 않는다. `GITHUB_TOKEN` 은 workflow 권한(읽기)뿐이다.
   - **되돌림**: 경로는 저장소 변수 하나(`CI_SELF_HOSTED_POOL`, desired state 는 `tools/github/`)로 켜고 끈다.
     - 변수가 없거나 `off` 이면 모든 작업이 호스트 러너로 간다.
     - 기계가 죽으면 변수를 끄는 것이 되돌림이다. 감시는 러너 그룹의 온라인 러너 수를 보고, 0 이 5분 넘게 이어지면 경보한다.
   - **가드**(ADR-0001, 막는 사고는 위 위협 표의 "fork PR 코드 실행"):
     - `check-workflow-policy.sh` 가 받는 `runs-on` 은 `ubuntu-24.04`·`windows-2022` 와 위 정확한 식 하나뿐이다.
     - 식이 fork 조건을 빼거나 다른 label 을 쓰면 실패한다.
4. **소유자 결정**: A(월 $4~5) 또는 B(월 €57~97 + 설치비)를 고른다. 둘 다 "CI 지출 0" 을 넘는다. A 는 CI 사용료가 아니라 조직 요금제다.
   결정 전까지 CI 는 지금처럼 GitHub 호스트 러너만 쓴다. 비용 없이 바로 할 수 있는 일은 §5 다.
5. **비용 없이 체감 대기를 줄이는 일**(이 ADR 의 범위 밖 후속, 각자 PR): merge queue 의 그룹 크기를 1 보다 크게 둔다
   (`main-ruleset.json` 의 merge queue 설정, 큐 앞 대기를 줄인다). 단계 없이 실패한 작업은 한 번 자동 재실행한다.

## Consequences

- 운영 호스트의 메모리·비밀 경계가 그대로 남는다. 30번의 OOM 위에 CI 를 더하지 않는다.
- A 를 고르면 코드 변경은 없다. 소유자가 조직 요금제를 바꾸고, `tools/github/billing-budget-policy.json` 의 Actions 예산 0 은 그대로
  둔다(요금제 요금은 Actions 사용료가 아니다).
  - 조치 후에는 같은 측정(`started_at − created_at`, 03~06 UTC 몰림 구간)을 다시 잰다. 기대(계산): 동시 상한 20 → 60 이면,
    측정 구간의 대기 작업 수가 상한을 넘던 시간이 사라져 p90 이 한가한 구간 수준(수십 초)으로 내려간다.
- B 를 고르면 §3 의 구현(기계 준비 스크립트, systemd 유닛, JIT 감독기, 시작 훅, desired-state 파일과 configurator, 가드 갱신)을
  별도 PR 로 올린다. 그 PR 이 ADR-0007 의 해당 줄을 이 ADR 로 대체한다.
- 측정은 재현 가능하다. 실행·작업 목록 REST API(`started_at − created_at`)와 merge queue timeline(GraphQL `AddedToMergeQueueEvent`)을
  읽기만 한다. 주간 `ci-time-budget` 워크플로(ADR-0203)가 이 대기 분위를 함께 보고하게 넓히는 것이 다음 변경이다(같은 데이터를 읽는다).
- 이 ADR 작성 중 측정 스크립트 하나를 운영 호스트 `/tmp` 에 잠시 복사했다가 바로 지웠다(읽기 전용 원칙 위반, 내용은 Prometheus 질의뿐).
  이후 질의는 표준 입력으로만 보냈다.

## 기각한 대안

- **운영 호스트에 rootless 러너 + cgroup 슬라이스**: 측정 2. 메모리 7일 최저 0.9 GiB·OOM 30번인 호스트에서 상한은 CI 를 죽이거나
  운영을 죽인다. Docker 작업은 rootless 로도 운영 커널을 공유한다.
- **운영 호스트에 KVM 일회용 VM**: 격리는 충분하다. 하지만 VM 메모리가 같은 62 GiB 에서 나온다.
- **actions-runner-controller(ARC)**: Kubernetes 가 필요하다. 이 규모에서는 같은 일을 JIT + systemd 가 한다. B 가 여러 기계로
  커지면 다시 본다.
- **larger runner(유료 GitHub 호스트)**: 분 단위 과금이다. ADR-0007 의 비용 경계 밖이다.
