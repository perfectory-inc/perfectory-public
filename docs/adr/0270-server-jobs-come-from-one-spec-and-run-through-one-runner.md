# ADR 0270: 서버 작업은 작업 명세 하나에서 만들고, 러너 하나가 돌린다

- Status: Accepted (2026-10-17, 사장 결정). 결정 2 "빌드를 서버 밖으로"는 열려 있다 — [부록 A](#부록-a-열린-결정--빌드를-서버-밖으로)
- Date: 2026-10-17
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(작업 = systemd 유닛,
  Airflow 는 시작만), [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(바뀐 데이터로 잇기),
  [ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(불변 릴리스),
  [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(결과 한 줄),
  ADR-0266(작업의 쓰기 경로는 한 목록, PR #648 — 아직 병합 전)
- Changes: ADR-0266 §2·§3 — `ReadWritePaths` 를 만드는 것은 `job_specs.py --write-units` 가 아니라 크레이트 `foundation-jobs` 다(#679); `job_specs.py` 는 스크립트 검사만(`--check-state-roots`)
- Would change: [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md) §1(결과 어휘에
  `skipped <이유>` 를 더함), [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md) §4(빌드는 혼자 돈다),
  [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) §1·§2(손으로 정한 슬롯, `takes_turns`),
  [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md) 의 0단계("돌던 작업이 끝날 때까지 기다림")
- Stories: [도메인 스토리](../domain-stories/README.md) — 지금 4개, 바뀐 뒤 4개

## Context

2026-10-15 ~ 10-17 의 운영 사고는 대부분 한 자리에서 났다: **서버 작업을 어떻게 정의하고, 돌리고, 서로 맞추는가.**

| 사고 | 직접 원인 | 같은 부류 |
|---|---|---|
| #619 → #625 모든 배포 멈춤, DAG 26개 정지 | Spark 임시 폴더를 `timers` 단계에서만 만들고, `migrate` 가 먼저 마운트 | 폴더 목록이 배포 스크립트 안에 손으로 |
| #647·#648 필지 ID 등록 첫 실행 5초 만에 실패 | 스크립트의 쓰기 경로가 유닛 샌드박스(`ReadWritePaths`) 밖 | 경로가 유닛·스크립트·env 파일 세 곳에 |
| DataHub 상태가 배포 계정 홈에 | 계약 밖의 상태 경로 | 상태 경로가 명세에 없음 |
| #656 결과 줄 `changed`, 종료 코드 1 | 작업이 결과 줄을 직접 찍음 | 판정이 작업마다 따로 |
| Gold 재구성 2.5시간이 배포를 막고, 배포가 작업을 막음 | 배포 0단계 "등록된 작업이 하나도 안 돌 때까지", 빌드 잠금이 모든 작업 시작 거부 | 배포와 작업이 전역 잠금 하나 |
| `takes_turns`, 손으로 짠 대기자 | Airflow 풀은 Airflow 가 시작한 것만 셈 | 동시성 규칙이 여러 곳에 |
| 10-17 by-PNU 굽기가 6시간+ `activating`, #661 배포가 0단계에서 3시간+ 대기 | 굽기의 1초짜리 첫 단계(`show-parcel-zoning-by-pnu-serving-state`, CPU 2.4초, 00:31:42Z 이후 로그 없음)가 R2 요청 하나에 멈춤. R2 SDK 클라이언트에 요청 시간 상한이 없고, 유닛 상한은 20시간(굽기 전체 기준). 운영자가 멈출 NOPASSWD 길이 없어 사장 sudo 만 가능 | 시간 상한이 작업 전체 하나뿐; 요청마다 상한이 없음; 멈춤 권한이 명세 밖 |
| #660 광주·전남 토지권리 조용히 빠짐 | 행 하한은 행 수만 셈, 빈 섹션은 못 봄 | 데이터 검사가 섹션·지역을 모름 |
| 셸 262개 | 작업마다 경로·시간 상한·결과가 systemd 유닛·스크립트·Airflow·계약에 반복 | — |

실물로 잰 반복(origin/main e975badc):

- 작업 32개의 시간 상한이 두 곳에 있다: `jobs.v1.json` 의 `timeout_minutes`(예: 165)와 유닛의 `TimeoutStartSec`(예: 160m).
  시험이 둘의 순서를 맞출 뿐 한 값에서 만들지 않는다.
- 작업 32개 중 유닛 `ExecStart` 가 `scripts/ops` 셸 스크립트인 것이 29개(나머지는 Python 둘, `bash -ec` 한 줄 하나).
- 릴리스 고정은 FLOOR 한 작업에만 있고, 그것도 유닛 `ExecStart` 의 `bash -ec` 한 줄(`readlink -e .../current` → 설정
  스냅숏 → 같은 릴리스인지 확인)로 손으로 적혀 있다. 다른 작업은 `/opt/foundation-platform/current/...` 로 시작하고,
  정리(`ExecStopPost`)도 `current` 를 다시 읽는다. 그 사이 `current` 가 바뀌지 않는 것은 "빌드가 도는 작업이 있으면
  시작하지 않는다"(ADR-0137)는 전역 잠금 덕분이다(`foundation-gold-panel-rebuild.service` 주석).
- 배포는 `foundation-deploy.sh` 0단계에서 10초마다 최대 5시간 "돌고 있는 작업"을 묻고, 5시간을 넘으면 실패한다.
  그런데 `by_pnu_serving_bake` 의 시간 상한은 1,260분이다.

불변식(이것이 깨지면 같은 사고가 다시 난다):

1. 작업에 대한 사실(명령, 경로, 시간, 자원, 입력·출력, 결과, 재시도, 알림)은 **한 곳**에만 적힌다.
2. 작업의 결과와 종료 코드는 **한 프로그램**이 정하고, 서로 어긋날 수 없다.
3. 배포는 **돌던 작업을 기다리지 않는다**. 기다리는 것은 "그 작업이 쓰는 표를 바꾸는 마이그레이션" 하나뿐이다.
4. 무거운 작업의 동시 실행은 **한 대기열**이 정한다. 누가 시작했든(Airflow·운영자·배포·빌드) 같은 대기열을 지난다.
5. 데이터가 줄어도 "행 수"가 아니라 **섹션·지역별 채움률**로 잡는다.

## Decision

### 1. 작업마다 명세 하나(SSOT)

`platforms/foundation-platform/orchestration/jobs.v1.json` 을 `jobs.v2.json` 으로 올려, 작업 하나가 아래를 모두 갖는다.
지금 이 파일이 이미 일정·풀·데이터 지도 연결의 정본이므로 새 파일을 만들지 않고 넓힌다.

| 칸 | 뜻 | 지금 있는 곳 |
|---|---|---|
| `command` | 릴리스 안의 실행 파일과 인자(argv) | 유닛 `ExecStart` |
| `run_as`, `environment_files` | 실행 계정, 읽는 env 파일 | 유닛 |
| `state_paths` | 실행이 끝나도 남는 쓰기 경로 (ADR-0266) | 유닛 `ReadWritePaths`·스크립트·env |
| `scratch` | 실행마다 새로 만들고 지우는 폴더의 뿌리(`/data/...`), 실패 시 보존 시간 | 스크립트마다 따로 |
| `time_limit_minutes` | **도는** 시간 상한 하나 | `timeout_minutes` + `TimeoutStartSec` |
| `stall_limit_minutes` | 출력 없이 지나도 되는 시간(진척 없음 상한) | 없음 |
| `wait_limit_minutes` | 자리·표 리스를 **기다리는** 시간 상한. 도는 시간과 따로 센다(Maestro `WAITING_FOR_PERMITS` ≠ `RUNNING`) | 없음(배포 0단계의 5시간이 유일) |
| `starting_deadline_minutes` | 예정 시각에서 이만큼 지나면 그 회차는 돌지 않고 `skipped late` 로 남긴다(재부팅·배포 뒤 몰아 돌기 방지, K8s `startingDeadlineSeconds`) | 없음 |
| `idempotent` | 같은 입력으로 두 번 돌아도 결과가 같다(`true`/`false`). **재시도·따라잡기·배포 뒤 한 번 실행은 `true` 인 작업만**(GitLab `idempotent!`). CI 가 거부 | 없음 |
| `resources.memory` | 계약(`tools/host-memory-budget.contract.json`)이 적은 측정 상한을 **가리킨다**(값을 베끼지 않는다) | 예산 계약 + 슬롯 손 매핑 |
| `concurrency` | 무거운 묶음 `heavy` 에 드는지, 우선순위 | `pool`·`pool_slots`·`priority_weight`·`takes_turns` |
| `pipeline_graph_edges` | 입력·출력 자산(데이터 지도 연결). **Airflow 가 실제로 읽는다**: `foundation_jobs.py` 의 `AssetOrTimeSchedule`(입력이 바뀌면 시작)·`outlets`(출력 자산 이벤트)·OpenLineage `inlets`/`outlets`. 쓰는 표 목록(§3 리스)도 여기서 **계산**한다. 새 데이터 의존 칸은 만들지 않는다 | 그대로 |
| `outcome` | ADR-0260 어휘(고정). 러너가 강제 | 스크립트마다 |
| `on_failure` | `next_period`(기본: 다음 주기가 재시도, omicron 배경 작업 방식) 또는 `retry {count, delay}`(주기가 길고 `idempotent` 인 작업만) | Airflow `retries`(22개 작업) |
| `alert` | 알림 경로(기본: Alertmanager 하나) | `OnFailure` + Alertmanager 두 길 |
| `schedule`, `started_by`, `started_once_after_deploy`, `enabled` | 그대로 | 그대로 |

명세에서 **생성**하고, CI 가 "다시 생성해서 저장소와 같은가"를 본다(지금 `job-dependencies.v1.json` 이 하는 방식 그대로).

| 생성물 | 비고 |
|---|---|
| `infra/systemd/foundation-<job>.service` 전체 | `ExecStart` 는 러너, `TimeoutStartSec` = 상한 + 정리 여유, 샌드박스(`ProtectSystem=strict`, `ReadWritePaths` = `state_paths` + `scratch`) |
| `tmpfiles.d` 한 파일 | 모든 `state_paths`·`scratch` 뿌리를 소유자·모드와 함께. 배포의 **첫 단계**가 `systemd-tmpfiles --create` 를 돌려, 어떤 마운트보다 먼저 폴더가 있다(#625 의 부류를 없앰) |
| sudoers 줄 | Airflow 문지기의 작업 유닛 start·stop 만(지금 그대로). 운영자 조작은 sudo 를 쓰지 않는다(§2 스위치) |
| Airflow DAG·풀·자산 | `orchestration/dags/job_specs.py` 가 명세를 읽는 지금 방식 유지. `execution_timeout` = 상한 + 여유 |
| 데이터 흐름 지도 노드 | `job-dependencies` 그대로 |

생성기는 Foundation 크레이트 `foundation-jobs` 다(Rust, `cargo run -p foundation-jobs -- generate|check`; CI 검사는 그 크레이트의 시험). `cargo xtask` 가 아닌 이유: xtask 는 표준 라이브러리만 쓰는 규칙이라 JSON 명세를 읽지 못한다. 같은 명세 타입을 러너가 그대로 쓴다. **작게 둔다**: 만드는 것은 위의 유닛·tmpfiles·sudoers
세 가지뿐이고, 템플릿 언어·플러그인·조건부 생성은 없다(GitLab 도 `all_queues.yml` 하나를 rake 작업 하나로 만들고 CI 가
대조한다). Airflow DAG 파일은 Airflow 가 요구하므로 Python 으로 남아 명세를 **읽기만** 한다 — 생성물이 아니다.

### 2. Rust 러너 하나 — `foundation-job-run`

모든 작업 유닛이 `foundation-job-run run <job>` 하나를 부른다. 러너가 하는 일(순서대로):

1. **릴리스 고정**: `current` 를 한 번 읽어 `releases/<sha>` 실제 경로로 바꾸고, 이후 모든 경로를 그 릴리스에서 읽는다.
   그 릴리스에 공유 잠금(리스)을 잡는다.
2. **같은 작업 겹침 금지**: 작업마다 배타 잠금. 이미 돌면 새로 시작하지 않고 그 실행에 붙는다(Nomad `prohibit_overlap`).
3. **자리 얻기**: 무거운 작업이면 §4 대기열에서 자기 무게만큼 자리를 얻는다. 기다린 시간을 실행 기록에 남긴다.
4. **표 리스**: 계산된 "쓰는 표"마다 공유 잠금. 마이그레이션은 같은 잠금을 배타로 잡는다(§3).
5. **실행 폴더**: `scratch` 아래 `<job>/<invocation>` 를 만들고 명령에 넘긴다. 성공하면 지우고, 실패하면 명세의 시간만큼 남긴다.
6. **시간 상한 둘**: (a) 작업 전체 상한 `time_limit_minutes`, (b) **진척 없음 상한** `stall_limit_minutes` — 명령이
   그 시간 동안 출력 한 줄도 내지 않으면 멈춘 것으로 본다(systemd `WatchdogSec` 의 생각을 출력 줄로). 10-17 굽기처럼
   20시간짜리 작업의 1초짜리 단계가 멈추면 (a) 만으로는 20시간을 잡힌다. 둘 중 먼저 닿은 것에서 프로세스 그룹에
   TERM → 여유 뒤 KILL. Docker 컨테이너는 실행마다 붙인 이름표로
   정리한다(지금 `ExecStopPost ... cleanup` 이 하던 일). systemd `TimeoutStartSec` 는 러너가 죽었을 때의 안전망이다.
7. **결과 한 줄**: 명령의 결과 줄을 읽어 판정하고, **러너가** `foundation-job-outcome ...` 을 마지막 줄로 찍는다.
   종료 코드는 그 줄에서 정한다(ADR-0260 표 그대로: `changed`·`unchanged`·`waiting` → 0, 그 밖 → 1).
   명령은 마지막 줄을 쓸 수 없으므로 #656 같은 어긋남이 생길 수 없다.
8. **실행 기록**: `/var/lib/foundation-jobs/runs/<job>/<invocation>.json` 에 릴리스·대기 시작·실행 시작·끝·결과·기다린 이유.

**운영자 스위치(작업마다, sudo 없음, 감사 기록)**

| 명령 | 뜻 | 본뜬 것 |
|---|---|---|
| `pause <job> --reason <이유>` | 새 회차를 시작하지 않는다. 회차는 `skipped paused` 로 남는다 | K8s CronJob `suspend`, Nomad `enabled=false`, GitLab `run_sidekiq_jobs_X=false` |
| `pause <job> --drop-pending` | 위 + 쌓인 회차(미뤄진 것·따라잡기)를 버린다 | GitLab `drop_sidekiq_jobs_X` |
| `resume <job>` | 다시 돈다 | — |
| `run-now <job>` | 지금 한 번 돈다(같은 대기열·리스를 지난다) | omicron `omdb nexus background-tasks activate` |
| `stop <job>` | 도는 회차를 멈추고 `failed stopped by <운영자>` 로 끝낸다 | — |

스위치는 `/var/lib/foundation-jobs/switches/<job>` 파일이고, 운영자 그룹(`foundation-operators`)이 쓸 수 있다. 러너는
시작할 때와 도는 중에 이 파일을 읽는다. 모든 조작은 `/var/lib/foundation-jobs/switches.log` 에 **덧붙이기만** 한다(누가·언제·
무엇을·이유). 그래서 사장 sudo 없이, 기록을 남기며 멈추고 켤 수 있다. Airflow DAG 정지는 운영자 스위치로 쓰지 않는다 — Airflow
밖에서 시작한 회차를 못 막기 때문이다.

**늦은 회차와 기다림**

- 러너는 Airflow 가 넘긴 예정 시각을 받는다. 지금이 예정 시각 + `starting_deadline_minutes` 를 넘었으면 돌지 않고
  `skipped late <예정 시각>` 으로 끝난다. 조용히 사라지지 않고 지도에 보인다.
- 자리·표 리스를 `wait_limit_minutes` 넘게 기다리면 `failed wait limit <무엇을 기다렸나>` 로 끝난다(용량 문제이므로 알림).
  도는 시간 상한은 실제로 돌기 시작한 때부터 센다.
- ADR-0260 의 결과 어휘에 `skipped <이유>` 하나를 더한다: 실패가 아니고, 하류를 깨우지 않고, 지도에 이유와 함께 보인다.

**네트워크 요청마다 시간 상한**: 러너의 상한은 마지막 안전망이다. 바깥으로 나가는 모든 클라이언트는 요청마다 연결·첫
바이트 상한을 갖는다. 지금 `outbound-http-infrastructure` 의 정책(`ICEBERG` 등 10초/30초)이 HTTP 클라이언트에는 있지만,
R2 SDK 클라이언트(`foundation-outbox/src/object_storage/r2.rs`)에는 없었다 — 10-17 굽기를 멈춘 것이 이 요청이다(따로
고치는 PR #668). 가드는 SDK 설정을 만드는 곳마다 시간 상한 설정을 요구한다.

`explain <job> [<invocation>]` 은 명세·고정된 릴리스·경로·실행 기록·로그 끝을 한 번에 보인다(진단을 한 명령으로).

잠금은 모두 `/run/foundation-jobs/` 의 `flock` 이다. `/run` 은 tmpfs 라 재부팅에 남지 않고, 프로세스가 죽으면 커널이 잠금을 푼다.
새 서비스·새 데이터베이스는 없다.

**옮기는 순서**: 작업 하나씩. 지금 `scripts/ops` 의 작업 감싸개 셸(유닛이 부르는 것)을 러너의 단계로 흡수하고, 데이터 일
자체(Spark 제출, 수집 바이너리)는 명령으로 남긴다. 가벼운 작업 하나(`shape_ledger_check`) → 무거운 작업 하나(Silver 갱신 한
레인) → 나머지 순서다. 옮긴 작업의 셸은 그 PR 에서 지운다.

### 3. 배포는 돌던 작업을 기다리지 않는다

릴리스는 이미 `releases/<sha>` 에 불변으로 있다(ADR-0184 `chattr +i`). 빠진 것은 "돌던 작업이 자기 릴리스에 머문다"는 보장과
"정리가 그 릴리스를 지우지 않는다"는 보장이다.

| 지금 | 바뀐 뒤 |
|---|---|
| 0단계: DAG 전부 멈춤, 등록 작업이 하나도 안 돌 때까지 최대 5시간 대기 | **지운다.** DAG 를 멈추지 않는다 |
| 빌드 중 모든 작업의 `ExecStartPre` 가 거부 → "미뤄진 작업" 기록 → 7단계에서 다시 시작 | 빌드는 무거운 작업 하나로 §4 대기열에 선다(무게 = 빌드 측정 상한). 미뤄진 작업 기록·재시작 로직을 지운다 |
| 유닛이 `/opt/foundation-platform/current/...` 를 실행 중에도 여러 번 읽음 | 러너가 시작 때 한 번 고정(§2-1). 정리도 같은 릴리스에서 |
| `prune` 이 옛 릴리스를 지움 | 리스가 남은 릴리스는 건너뜀 |
| 3단계: 옛 FLOOR 설정을 복사해 `sed` 로 고침 | 명세의 env 에서 생성 |
| 마이그레이션은 아무 작업도 안 돌 때 | 마이그레이션이 바꾸는 표마다 배타 리스. **그 표를 쓰는 작업이 돌 때만** 그 마이그레이션이 기다린다(상한 = 그 작업의 남은 시간 상한). 그동안 그 표를 쓰는 새 실행은 대기열에 선다 |
| Airflow 재시작·SSH 끊김이 돌던 유닛을 멈춤 | 끊겨도 작업은 돈다. 다음 시도는 §2-2 로 그 실행에 붙는다 |

표 리스가 필요한 이유: Iceberg 는 읽기가 스냅숏이라 읽는 쪽은 안전하지만, 옛 스키마로 쓰는 작업이 새 필수 칸을 모른 채
커밋할 수 있다(10-13 #469 의 부류). Postgres 마이그레이션은 확장-축소(expand/contract) 규칙과 ADR-0259 승인 게이트를
그대로 두고, 파괴적인 것만 같은 리스를 잡는다.

배포 순서(바뀐 뒤): 사전 점검 → 제어 체크아웃 → 빌드(자리 얻기) → staging smoke → `systemd-tmpfiles --create` → 생성된
유닛 설치 → `current` 전환 → 마이그레이션(표 리스) → Airflow(설정이 바뀐 것만) → 배포 뒤 한 번 작업 → DataHub.

### 4. 무거운 작업은 대기열 하나, 자리 수는 측정에서

- **묶음 하나(`heavy`)**, 자리 수 = 호스트의 측정 용량(GiB): 물리 메모리 − 상주 서비스 상한 − 예비. 셋 다 이미
  `tools/host-memory-budget.contract.json` 에 있다(지금 23.06g). 작업의 무게 = 그 작업의 측정 상한(같은 계약). 그래서
  "3자리, FLOOR 는 3, 접기는 2, 굽기는 1" 같은 손 매핑과 "슬롯 조합마다 메모리 합"을 따로 검사하는 일이 없어진다.
  지금 값으로 다시 세면 접기 8g + 굽기 14g = 22g 는 같이 돌고, FLOOR 20g·빌드 22g 는 혼자 돈다 — 지금 손으로 정한 조합과 같다.
- **우선순위**: 빌드 > FLOOR·계보 > 접기 > 굽기(지금 `priority_weight` 순서). 같은 우선순위는 먼저 온 순서.
- **예약(reservation)**: 대기열 앞의 작업이 자리를 기다리는 동안, 그보다 뒤의 작업은 남은 자리가 있어도 무거운 자리를 새로
  잡지 않는다. 그래서 긴 굽기가 FLOOR 를 굶길 수 없고, `takes_turns` 와 그 대기 기록을 지운다.
- Airflow 풀은 같은 수로 **생성**해 남긴다(Airflow 가 자리 없는 작업을 미리 띄워 LocalExecutor 자리를 쥐지 않게). 권위는 러너다.
- 가벼운 작업은 묶음에 들지 않는다. 그 합은 예산 계약의 예비 안에 있어야 하고, 예산 가드가 센다.

### 5. 조용한 데이터 손실 막기 (참조만)

섹션·지역별 채움률 하한(예: "토지권리 원천 행이 있는 필지 중 토지권리가 붙은 필지 ≥ X%, 시도마다")을 계약에 적고, Gold 를
**승격하기 전에** 막는 검사로 돈다. 하한 아래면 승격하지 않고 `failed <섹션> <지역>` 으로 끝난다. 이 검사는 데이터 가드
작업자가 따로 만든다. 이 ADR 은 러너·명세 쪽에서 (a) 그 검사를 작업의 출력 자산 검사로 명세에 연결하고, (b) 실패를 결과
한 줄로 보이는 것만 정한다.

### 6. 관측: 원인 하나에 묶고, 기다림과 실행을 나눈다

- 실행 기록의 세 시각(대기 시작, 실행 시작, 끝)으로 지도가 **기다린 시간**과 **돈 시간**을 따로 보인다. 기다린 이유
  (`slot`·`table_lease`·`inputs`)도 보인다.
- 하류가 늦거나 기다릴 때, 생산자 사슬(`job-dependencies`)에서 가장 최근에 실패한 위쪽 작업이 있으면 그 아래에 묶는다.
  알림은 **원인 하나에 하나**: Alertmanager 억제(inhibition)로 원인 아래 묶인 하류 알림을 누른다.
- 실패 알림 길은 하나다. Airflow 가 돌리는 작업은 `OnFailure=foundation-unit-failed@` Slack 을 떼고 Alertmanager 만 쓴다.
  Airflow 밖 타이머(백업 등)는 지금 길을 유지한다.

### 7. 셸 정책

Google Shell Style Guide 를 따른다: **100줄이 넘거나 단순하지 않은 흐름이 있는 셸은 구조화된 언어로 옮긴다.**

| 대상 | 옮길 곳 |
|---|---|
| 서버 작업 감싸개, 배포 단계 | `foundation-job-run`(작업), 배포는 같은 크레이트의 하위 명령 |
| 개발·CI·가드 도구 | `cargo xtask` |
| 100줄 이하의 단순 연결(진입점 등) | 셸로 둔다 |
| 아무도 안 부르거나 끝난 일회성 | 지운다 |

전수 목록: [`docs/domain-stories/shell-inventory.csv`](../domain-stories/shell-inventory.csv)(2026-10-17 스냅숏, 262개,
파일마다 줄 수·호출자·대상·이유). 2026-10-17 집계:

| 대상 | 개수 |
|---|---:|
| `xtask` (가드·CI·자기 시험 대부분, `scripts/guard` 129개 포함) | 172 |
| `runner` (`scripts/ops` 40개 중 37개, 배포·복구 스크립트) | 70 |
| `keep` | 16 |
| `delete` (`test_v001_full.sh` 가 대신하는 옛 마이그레이션 시험 4개) | 4 |

100줄을 넘는 셸은 124개다. 아무도 돌리지 않는 자기 시험 3개(`products/gongzzang/scripts/lefthook/*.tests.sh`)는
지우지 않고 CI 에서 돌게 한다(ADR-0011).

CI 가드는 **새** 셸 파일이 100줄을 넘거나 대상이 `runner`·`xtask` 인 기존 파일이
늘어나면 거부한다(줄 수는 가드가 직접 센다; 목록은 줄어들기만 한다).

## 실제 저장소와 비교 (2026-10-17 사장 승인으로 개정)

문서 요약이 아니라 각 저장소의 실제 파일을 읽고 비교했다.

| 우리에게 필요한 것 | omicron (Oxide, Rust) | GitLab (Sidekiq) | Netflix Maestro | Kubernetes CronJob / Nomad | 이 ADR 의 선택 |
|---|---|---|---|---|---|
| 작업 정의가 한 곳 | [`nexus/src/app/background/init.rs`](https://github.com/oxidecomputer/omicron/blob/main/nexus/src/app/background/init.rs): 모든 배경 작업을 한 곳에서 등록, 주기는 설정 | [`app/workers/all_queues.yml`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/workers/all_queues.yml): 워커 속성에서 **생성**(`lib/tasks/gitlab/sidekiq.rake` 의 `all_queues_yml:generate`), "손으로 고치지 말 것" | 워크플로 정의 JSON | Job spec 하나 | 명세 하나, 생성물 셋, CI 가 대조(§1) |
| 실패하면 | [`mod.rs`](https://github.com/oxidecomputer/omicron/blob/main/nexus/src/app/background/mod.rs): "재시도하지 말고 다음 주기의 재활성화에 맡겨라" | 재시도 + `idempotent!` 선언 | [`RetryPolicy.java`](https://github.com/Netflix/maestro/blob/main/maestro-common/src/main/java/com/netflix/maestro/models/definition/RetryPolicy.java): 단계마다 재시도 정책 | `backoffLimit` | 기본 `next_period`; 재시도는 주기가 길고 `idempotent` 인 작업만 |
| 두 번 돌아도 안전한가 | 작업 함수는 멱등이어야 한다(mod.rs) | [`worker_attributes.rb`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/workers/concerns/worker_attributes.rb) `idempotent!`, `deduplicate` | — | — | `idempotent` 필수 칸; 재시도·따라잡기·배포 뒤 한 번은 `true` 만 |
| 운영자 스위치 | `omdb nexus background-tasks activate` (지금 한 번) | `run_sidekiq_jobs_X=false`(미룸), `drop_sidekiq_jobs_X`(버림) — [feature flags 문서](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/development/feature_flags/_index.md#controlling-sidekiq-worker-behavior-with-feature-flags) | 워크플로 비활성/중지 | CronJob `suspend`, Nomad `periodic.enabled` | `pause`·`pause --drop-pending`·`resume`·`run-now`·`stop`, sudo 없음, 덧붙이기 기록 |
| 너무 늦은 회차 | 주기 재활성화라 따라잡기 없음 | — | — | `startingDeadlineSeconds`: 넘으면 그 회차를 건너뜀 | `starting_deadline_minutes` → `skipped late` 로 기록 |
| 기다림 vs 실행 시간 | — | — | [`TimeoutPhase.java`](https://github.com/Netflix/maestro/blob/main/maestro-common/src/main/java/com/netflix/maestro/models/definition/TimeoutPhase.java): `WAITING_FOR_PERMITS`·`RUNNING` 을 따로, [`StepTimeouts.java`](https://github.com/Netflix/maestro/blob/main/maestro-common/src/main/java/com/netflix/maestro/models/definition/StepTimeouts.java) | `activeDeadlineSeconds`(실행 전체) | `wait_limit_minutes` 와 `time_limit_minutes` 를 따로, 그리고 `stall_limit_minutes` |
| 겹침 | 한 Nexus 안에서 한 번에 하나 | `deduplicate` | [`RunStrategy.java`](https://github.com/Netflix/maestro/blob/main/maestro-common/src/main/java/com/netflix/maestro/models/definition/RunStrategy.java) `FIRST_ONLY` 등 | `concurrencyPolicy: Forbid`, Nomad `prohibit_overlap` | 작업마다 배타 잠금, 이미 돌면 붙음 |
| 무거운 일 몇 개만 | — | 큐·`resource_boundary`·`urgency` | tag permits (`TagPermitManager`) | — | 메모리 무게 대기열 하나(§4) |

**버린 것(이 비교로)**: 주기마다 다시 맞추는 작업의 Airflow 재시도(다음 주기가 재시도다); 소비자가 없는 데이터 의존 칸(우리
`pipeline_graph_edges` 는 Airflow 가 읽으므로 남긴다); 큰 생성기(템플릿·조건부 생성).

## Evidence — 가져온 것과 가져오지 않은 것

| 출처(1차 자료) | 가져온 것 | 가져오지 않은 것 |
|---|---|---|
| [systemd.service `WatchdogSec`](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html) | "살아 있다는 신호가 끊기면 멈춘 것" → 진척 없음 상한 | `sd_notify` 를 작업마다 심기(출력 줄로 대신) |
| [AWS SDK for Rust — timeouts](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html) | 기본은 연결 상한(3.1초)뿐, 첫 바이트·시도·작업 상한은 "없음"; 멈춘 본문 보호는 본문에만 → 응답 머리를 기다리는 요청은 클라이언트를 만드는 곳에서 상한을 건다 | — |
| [Kubernetes Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/) | `activeDeadlineSeconds` → `time_limit_minutes`; `backoffLimit` → `on_failure.retry`; `ttlSecondsAfterFinished` → 실행 폴더 보존 시간 | 클러스터·컨트롤러 |
| [Kubernetes CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/) | `suspend` → `pause`; `startingDeadlineSeconds` → `starting_deadline_minutes`; `concurrencyPolicy: Forbid` → 겹침 금지 | `Replace`(돌던 것을 죽이고 새로) |
| [Airflow pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html), [assets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html) | 풀·`pool_slots`·`priority_weight` 는 생성해서 유지; 자산으로 잇기(ADR-0171) 유지 | 풀을 동시성의 권위로 쓰기(Airflow 밖 시작을 못 봄) |
| [Nomad periodic](https://developer.hashicorp.com/nomad/docs/job-specification/periodic) | `prohibit_overlap`(같은 작업 겹침 금지), 작업 하나 = 명세 하나(jobspec) | Nomad 자체 |
| [systemd.exec](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html) · [tmpfiles.d](https://www.freedesktop.org/software/systemd/man/latest/tmpfiles.d.html) | `ProtectSystem=strict` + `ReadWritePaths` 를 명세에서 생성; 폴더는 tmpfiles 로 마운트 전에 | 유닛을 손으로 쓰기 |
| [Google SRE — Release Engineering](https://sre.google/sre-book/release-engineering/) | 밀폐(hermetic)·재현 가능한 빌드, 설정도 릴리스와 함께 버전 관리 → 생성된 유닛·tmpfiles 가 릴리스에 들어감 | 여러 데이터센터 단계 배포 |
| [Google SRE — Data Processing Pipelines](https://sre.google/sre-book/data-processing-pipelines/), [Workbook — Data Processing](https://sre.google/workbook/data-processing/) | 주기 파이프라인의 "모두 같은 시각에 몰림"·자원 굶김 문제 → 대기열·예약; 데이터 정확성 SLO → 채움률 하한 | 분산 작업 관리자 |
| [Dagster asset checks](https://docs.dagster.io/guides/test/asset-checks) | 막는(blocking) 자산 검사: 검사가 실패하면 하류를 진행하지 않음 → 승격 전 채움률 검사 | Dagster 이전 |
| [Great Expectations `expect_column_values_to_not_be_null`](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null) | `mostly`(비율 하한) 개념 → 섹션 채움률 하한 | GE 런타임 |
| [Bazel hermeticity](https://bazel.build/basics/hermeticity) | 같은 입력 → 같은 산출물, 산출물은 바뀌지 않음 → `releases/<sha>` 고정과 리스 | Bazel 도입 |
| [Alertmanager inhibition](https://prometheus.io/docs/alerting/latest/alertmanager/#inhibition) | 원인 알림이 있으면 하류 알림 억제 | — |
| [Google Shell Style Guide](https://google.github.io/styleguide/shellguide.html) | 100줄·복잡한 흐름이면 다른 언어로 | — |
| [Domain Storytelling](https://domainstorytelling.org/) (Hofer·Schwentner, 2021) | 설계 전에 지금/바뀐 뒤 이야기를 그림 문법으로 | — |

규모: 호스트 하나, 작업 약 30개. 그래서 클러스터 스케줄러·분산 잠금·새 서비스는 가져오지 않는다.

## Rejected

| 대안 | 기각 이유 |
|---|---|
| Nomad 또는 k3s 로 작업을 옮김 | 호스트 하나에 제어 평면을 하나 더 둔다. 필요한 보장(겹침 금지, 시간 상한, 재시도)은 러너 수백 줄로 된다 |
| Airflow 만으로 동시성(풀) | 운영자·배포·빌드가 시작한 것을 못 본다. 그래서 `takes_turns`·빌드 잠금·배포 0단계 같은 손 대기자가 생겼다 |
| systemd 만 쓰고 Airflow 를 뺌 | 자산으로 잇기, 실행 이력, 데이터 지도가 Airflow 기록을 읽는다(ADR-0171·0165) |
| 셸 공용 라이브러리로 감싸개를 정리 | 시험하기 어렵고, 이번 사고들이 셸의 경로·순서·결과 판정에서 났다 |
| 지금처럼 배포 전역 잠금 | 2.5시간·5시간 대기와 미뤄진 작업이 이 결정의 이유다 |
| 빌드를 지금 호스트 밖(CI)으로 | 이 ADR 에서는 정하지 않는다. ADR-0134 의 신뢰 모델을 바꾸므로 따로 정한다 — 부록 A |
| Dagster 로 이전 | 큰 이전이다. 막는 자산 검사 아이디어만 가져온다 |
| 유닛에서 명세를 거꾸로 생성 | 유닛은 자산·일정·재시도를 표현하지 못한다 |
| 러너가 매 실행 폴더를 `mkdir` 만 함(tmpfiles 없이) | 컨테이너 마운트가 러너보다 먼저 일어나는 경로(#625 의 migrate)를 막지 못한다 |
| Maestro 엔진 도입 | 워크플로 엔진·DB·API 서버를 하나 더 둔다. 시간 상한을 단계별로 나누는 생각과 tag permit 만 가져온다 |
| GitLab 식 비율 롤아웃 스위치(10%만 실행) | 작업 30개, 회차가 하루 몇 번이라 비율이 뜻이 없다. 켬/끔/버림만 |
| 슬롯 수를 손으로 유지 | 메모리 측정과 슬롯 매핑이 두 곳이 된다. 무게를 측정 상한에서 읽으면 하나다 |

## Consequences

- 단계별 이전 계획과 각 단계의 완료 기준은 [사장 요약](../domain-stories/owner-summary.md)에 있다. 시간 추정은 측정 전이라 적지 않는다.
- 좋아지는 것: 경로·시간·결과가 한 곳; 배포가 작업을 기다리지 않음; 손 대기자 0; 알림 원인당 하나.
- 대가: 러너가 핵심 경로가 된다 — 러너의 결함은 모든 작업을 멈춘다. 그래서 (a) 러너는 작업 하나씩 옮기고, (b) 각 단계는
  심은 위반으로 실패하는 시험과 함께 들어오며, (c) systemd 시간 상한은 안전망으로 남는다.
- 빌드는 무거운 대기열에서 자리를 기다린다. 굽기(14g)가 돌면 빌드(22g)는 기다린다 — 지금도 그렇지만, 바뀐 뒤에는 그 기다림이
  빌드 하나에만 걸리고 DAG·다른 작업을 멈추지 않는다. 이 기다림까지 없애려면 부록 A 를 따로 정한다.

## 부록 A (열린 결정) — 빌드를 서버 밖으로

사장이 검토 중이다(2026-10-17). 빌드 캐시를 R2 에 두는 것은 허락했다. **여기서는 정하지 않는다**; 정하면 ADR 을 따로 쓴다.

### 선택지

1. **CI 가 main 의 커밋마다 릴리스 산출물을 한 번 만든다.** 머지 큐를 통과한 커밋에 대해 GitHub 호스트 러너(공개 저장소, 무료)가
   지금 서버가 만드는 것(publisher 이미지, Rust 바이너리, tippecanoe 이미지)을 만든다.
2. **출처 증명을 붙인다.** [GitHub artifact attestations](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations/using-artifact-attestations-to-establish-provenance-for-builds)(Sigstore 서명, SLSA 출처 증명 형식)로 "이 다이제스트는 이
   저장소의 이 워크플로가 이 커밋에서 만들었다"를 서명한다. [SLSA v1.0 Build L2](https://slsa.dev/spec/v1.0/levels)에 해당한다.
3. **서버는 빌드하지 않고 확인한다.** 산출물을 받아 (a) 커밋이 정본 main 의 조상인지(지금 그대로), (b) 다이제스트, (c) 서명과
   서명한 워크플로(`gh attestation verify --repo … --signer-workflow …`, [설명](https://cli.github.com/manual/gh_attestation_verify))를
   확인하고 설치·봉인한다. 22g 빌더가 없어지므로 배포는 무거운 대기열을 기다리지 않는다.
4. **컴파일 캐시는 R2.** [sccache 의 S3 백엔드](https://github.com/mozilla/sccache/blob/main/docs/S3.md)(`SCCACHE_ENDPOINT`,
   `SCCACHE_REGION=auto`, `SCCACHE_S3_KEY_PREFIX`)를 R2 버킷 하나에 둔다. **쓰기는 main·머지 큐 실행만**, PR 실행은 읽기만
   (오염된 캐시가 릴리스로 들어가는 길을 막는다).

### ADR-0134 에서 바뀌는 것

| ADR-0134 | 지금 | 이 선택지 |
|---|---|---|
| §1 소스 인증 | 서버의 제어 코드가 정본 main 을 직접 fetch 하고, 그 커밋의 바이트로 **서버가 빌드** | 정본 main 조상 확인은 그대로. 빌드 대신 **서명된 산출물의 다이제스트와 서명 워크플로**를 확인 |
| 믿는 대상 | 서버의 root 소유 제어 코드 + 서버의 빌드 | 위에 더해 GitHub Actions 러너와 Sigstore(서명 발급) |
| §2 설치 뒤 바이트 재검사, 봉인 | 그대로 | 그대로(`chattr +i`, ADR-0184) |
| 빌드 상한(ADR-0137) | 서버 메모리 예산에 22g 빌더 | 서버 예산에서 빠짐 |

### 열린 위험 (정하기 전에 잴 것)

- **러너 크기**: 공개 저장소의 무료 Linux 러너는 4 CPU·16 GB 메모리·14 GB SSD 다. 서버에서 잰 publisher 빌더 상한은 22g 다
  (ADR-0137). 16 GB 에서 빌드되는지 먼저 재야 한다. 더 큰 러너는 돈이 들므로 이 선택지 밖이다.
- **믿는 대상이 늘어난다**: GitHub Actions·Sigstore 가 뚫리면 서명된 나쁜 산출물이 생긴다. 완화: 서명한 워크플로 경로와 ref 를
  고정해 확인하고, 서버가 가끔 같은 커밋을 다시 빌드해 다이제스트를 대조(재현성 표본 검사)할 수 있다.
- **R2 쓰기 열쇠가 CI 에 들어간다**: main·머지 큐 작업에만 주고, 캐시 전용 버킷·접두어로 범위를 좁힌다.

## 진행 기록 (덧붙임 — 결정 본문은 바꾸지 않는다)

### 2026-10-18 — 단계 1 완료: 명세에서 만들고, CI 가 어긋남을 거부한다

| 무엇 | 결과 (main, 2026-10-18 측정) | PR |
|---|---|---|
| 실행 시간 상한 | 작업 32개 모두 `time_limit_minutes` 한 곳. 유닛 19개의 `TimeoutStartSec=` 는 생성물 | #678 |
| 시작 하나의 상한 | `timeout_minutes` 0개. `time_limit_minutes + stop_grace_minutes` 를 넷(DAG, 배포, foundation-api, 시험)이 같은 방식으로 더함. 실제 값 변화 0건(32개 대조) | #685 |
| 쓰기 경로 | `ProtectSystem=strict` 유닛 19개의 `ReadWritePaths=` 는 `state_paths` 에서 생성. Python 생성기 삭제(생성기 하나) | #648, #679 |
| 작업 폴더 | `state_directories` 13개 → 생성 tmpfiles 파일. 배포 스크립트의 손 `install -d` 0줄. `activate` 가 `current` 전환 **전에** 만듦 | #680, #688 |
| 누구 소유인가 | Spark 컨테이너의 uid 185 에 이름 `foundation-spark` 를 줌(sysusers, 배포가 185:185 인지 확인). #688 은 `lakehouse-ivy` 를 이 계정 소유 0755 로 만들었으나 **전제가 틀렸다**: 어떤 작업도 그 경로를 쓰지 않는다(모든 Spark 실행이 릴리스의 jar 를 읽기 전용 ivy 캐시로 마운트, ADR-0134). #708 이 작업 7개의 상태 경로에서 뺀다. 만들지 않는 상태 경로 0개 | #688, #708 |
| 모든 작업 유닛의 공통 줄 | 8줄(oneshot, 계정, 실패 알림, 샌드박스 4줄)을 19개 유닛 모두 갖고, 빠지면 거부 | #683 |

- 생성·검사는 크레이트 `foundation-jobs` 하나다(Rust, 시험 18개). 모든 검사는 위반을 심어 거부까지 확인했다.
- sudoers 는 배포가 이미 `jobs.v1.json` 에서 만들고 `visudo` 로 검사하므로 바꾸지 않았다.
- 첫 배포(2026-10-18, 릴리스 50a57536): `systemd-sysusers` 가 activate 단계에서 `foundation-spark`(185:185)를 만들었다(배포 로그). 같은 날 Silver 측정 작업의 Spark 컨테이너(uid 185)가 생성된 작업 폴더 `lineage-stewardship/lakehouse`(0777)에 쓰며 정상으로 돌았다. uid 185 와 호스트 스크립트가 함께 쓰는 lakehouse 작업 폴더 둘(0777)의 최소 권한 소유는 분석 중이다.
- 단계 2 진행 중: ADR-0270 이 승인된 범위이고, 사장이 2026-10-18 2a+2b 와 첫 작업 2개(`shape_ledger_check`, `lakehouse_maintenance`)를 정했다. 2a 러너 핵심 병합(#705), `job-run` 명령(#711). 범위·완료 기준: [사장 요약 "단계 2"](../domain-stories/owner-summary.md).
- 2b 에 규칙 추가(2026-10-18 사고): **모든 시작은, 예약이든 운영자든, 같은 대기열 자리를 차지한다.** 07:54 운영자가 꺼진 일회성
  작업(spark 3자리)을 `job start` 로 띄웠고, Airflow 는 자기가 띄운 실행만 세므로 08:20 FLOOR(3자리)를 옆에 띄웠다. /data 가 포화되어
  (iowait 37%, sda 95%) FLOOR 가 약 1분 대신 13.8분 걸렸고 자동배포가 17분 멈췄다. 운영자 스크립트는 Gold·Silver 단위만 손으로 골라
  보고 있었다. 이제 `job_units.py pool-full` 하나가 `jobs.v1.json` 의 대기열 구성원 중 systemd 가 돈다고 답하는 것(꺼진 작업, 템플릿
  작업의 다른 인스턴스 포함)의 자리를 세고, 예약 시작 문은 자리가 없으면 `waiting` 으로 시작하지 않으며, 운영자 `job`·`silver`·
  `parcel-tiles rehearse` 시작은 거부한다(75). 남은 틈: 세기와 시작 사이의 같은 순간 경쟁, 그리고 작업이 아닌 일시 유닛(운영자 Gold
  재빌드)은 러너의 호스트 자리 임대가 닫는다.
- 2b 운영자 스위치에 규칙 추가(2026-10-16 사고): **운영자 시작은 즉시이고 한정된다.** 에이전트가 두고 간 nohup 대기 스크립트가 호스트가
  한가해지자 10시간 뒤 `job start parcel_registry`(운영 Silver 쓰기)를 실행했다. 이제 `job start`·`silver start` 는 요청 시각
  (`--requested-at`, 10분 지나면 거부 77)과 이유(`--reason`, 한 줄 300자 이하)를 받고, 표준 입력이 터미널이 아니면 거부한다(77).
  기다렸다 시작하는 길은 없다: 자리가 없거나 배포 중이면 거부(75)하고 사람이 다시 묻는다. 시작마다 누가(`SUDO_USER`)·무엇을·
  언제 요청·왜를 시스템 저널(`journalctl -t foundation-operator-start`)에 먼저 남기고, 남기지 못하면 시작하지 않는다(70). 남은 틈:
  사람의 터미널에서 `&` 로 띄운 대기 루프는 터미널을 가진 채 기다릴 수 있다 — 10분 만료가 미리 찍은 시각은 막지만 실행 때 계산한
  시각은 못 막는다. 2b 의 `run-now`(운영자 그룹, sudo 없이)가 요청을 foundation-api 가 발급·만료시키는 한 번짜리로 받아 닫는다.
- 2b: 즉시·한정 규칙을 운영자 스크립트의 나머지 동작으로 넓혔다(2026-10-19). `status`·`plan`·`health`(읽기)를 뺀 모든 동작이
  마지막에 `--requested-at`·`--reason` 을 받고, 저널 기록은 동작별 거부보다 먼저 남는다(거부된 요청도 남는다). 묶음 전환
  유닛이 이 스크립트로 부르는 자기 단계는 그 전환의 것이다(`FOUNDATION_OPERATOR_WAIT=1` 이고 `SUDO_USER` 없음; sudo 실행은
  환경을 지우고 `SUDO_USER` 를 가지므로 흉내 낼 수 없다). 단계 배포의 `health` 점검은 읽기라 그대로 돈다.
- #688 바로잡기: `lakehouse-ivy` 는 어떤 작업도 쓴 적이 없다(모든 Spark 실행이 릴리스의 jar 를 읽기 전용 ivy 캐시로 마운트). #708 이 작업 7개의 상태 경로와 `state_directories` 에서 뺐다.

