# ADR 0308: 배포는 릴리스가 붙잡는 작업만 멈추고, 입력이 그대로인 단계는 건너뛴다

- Status: Accepted (2026-10-28, 사장 승인)
- Date: 2026-10-28
- Builds on: [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §3(배포는 돌던 작업을 기다리지 않는다, D1·D2·D5),
  [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md) §13(서버는 레지스트리에서 받는다),
  [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)(`interruptible`)
- Amends: [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md) §2 — 멈추는 대상은 "중단 가능한 모든 작업"이 아니라 "릴리스가 붙잡은 작업 중 중단 가능한 것"이다.
  [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md) §4 "빌드는 혼자 돈다" — 서버가 빌드할 때(`artifacts_from: server-build`)만 적용된다.
- 관련 사건: 2026-10-26 by-PNU bake 가 배포 0단계를 1.5시간 막아 수집이 2시간 멈췄다(ADR-0301). 2026-10-28 전량 필지 bake(20시간+) 동안 main 병합을 멈춰야 했다 — 병합 하나가 배포가 되고, 배포 0단계는 모든 작업을 멈추고 기다리기 때문이다.

## Context

`platforms/foundation-platform/scripts/deploy/foundation-deploy.sh` 의 0단계는 **모든** DAG 를 멈추고, 등록된 작업이
**하나도** 돌지 않을 때까지 최대 5시간 기다렸다. 그 이유는 둘이었다.

1. 서버가 릴리스를 빌드했다. 22g 빌더는 메모리 예산에서 "가장 큰 일회성 작업" 자리를 차지하므로 혼자 돌아야 했다(ADR-0137 §4).
2. 바뀌는 표를 쓰는 작업이 옛 스키마로 쓰면 안 된다.

1은 이미 사라졌다. `tools/release-build.contract.json` 의 `artifacts_from` 은 #771(ADR-0276 R3) 이후 `registry` 이고,
운영 서버의 `artifacts/77198c1f…/build.json` 에는 레지스트리에서 받은 이미지 넷(publisher·tippecanoe·runtime·recovery)이
적혀 있다(2026-10-28 읽기 전용 확인). 서버는 빌드하지 않고 받는다. 2는 pre-flight 의 `release_changes.py` 가 이미 답한다:
바뀌는 표를 쓰는 작업, 그래프가 자리를 모르는 작업, Postgres 마이그레이션이면 전부(`*`). 그런데 0단계는 이 답(hold 목록)을
계산만 해 두고 쓰지 않았다. 2b단계가 빌드 뒤에야 hold 밖 작업을 되살렸다.

릴리스 77198c1f 의 배포(2026-10-27 23:27–23:39, 11분 42초, autodeploy 저널) 가운데 릴리스가 바꾼 것이 없는데도 매번 든 시간:

| 단계 | 시간 | 이 릴리스에 필요했나 |
|---|---:|---|
| 0단계 DAG 33개 멈춤 | 56초 | 아니오 — hold 목록이 비어 있었다 |
| 2b단계 DAG 33개 다시 켬 | 58초 | 아니오 |
| 레이크하우스 마이그레이션(표 48개 대조) | 1분 51초 | 아니오 — `release_changes` 가 바뀐 표 0개라고 보고 |
| Postgres 사슬(bootstrap→migrate→grants→finalize, `up` 넷마다 다시 돎) | 약 77초 | 런타임 이미지가 릴리스마다 새것이라 예 |
| 배포 뒤 한 번 작업 여섯 | 2분 15초 | 새 릴리스가 처음 만나는 실제 입력(ADR-0173) |
| 7단계 DAG 33개 다시 켬 | 55초 | 아니오 — 0단계가 멈춘 것만 켜면 된다 |

고친 사실 하나: 2026-10-28 감사 메모는 "ADR-0301 이후 배포가 interruptible bake 를 SIGTERM 한다"고 적었지만, main 의 0단계에는
멈추는 코드가 없다. ADR-0301 은 1단계(#874, `jobs.v1.json` 의 `interruptible` 선언과 가드)만 들어갔다. bake 유닛 저널에서 10-25
이후 `signal` 종료는 10-26 12:11 의 운영자 수동 정지 하나다. 실제 위험은 "배포가 bake 를 죽인다"가 아니라 "배포가 bake 를 5시간
기다리다 실패하고, 그동안 모든 수집이 멈춘다"였다.

## Decision

### 1. 0단계는 릴리스가 붙잡는 작업만 비운다

- 비우는 대상(drain) = pre-flight 의 hold 목록. 같은 pre-flight 가 배포할 커밋의 `tools/release-build.contract.json` 을 함께 꺼내
  `artifacts_from` 을 읽는다.
  - `registry`: drain = hold. 다른 작업은 DAG 도 멈추지 않고, 도는 실행도 기다리거나 멈추지 않는다. 실행은 시작한 릴리스에
    머물고(D1, `prune` 은 프로세스가 쥔 릴리스를 지우지 않는다), 5단계 Airflow 재시작을 넘긴다(D2). D5 이후 2b단계가 이미 hold 밖
    작업을 릴리스 전환 동안 돌려 왔으므로, 이 결정은 그 창을 빌드 앞(0–2단계)까지 늘릴 뿐이다.
  - hold 가 비면 아무것도 멈추지도, 묻지도, 기다리지도 않는다.
  - `*`(Postgres 마이그레이션, 또는 무엇이 바뀌는지 알 수 없음)는 전부 — 안전한 기본값은 그대로다.
  - `server-build`, 계약을 읽을 수 없음: 전부(아래 2).
- 붙잡힌 작업이 돌고 있으면: `interruptible: true` 면 `systemctl stop --no-block` 하고 끝나길 기다린다(ADR-0301 §2, 부분 산출은
  append-only 라 안전하고 배포 뒤 다시 돈다). `false` 면 기다린다(상한 5시간, 넘으면 배포 실패, 지금과 같다). 수집 작업이 `false`
  여야 한다는 가드(`orchestration/tests/test_job_specs.py`)는 그대로다.
- `/run/foundation-deploy-hold` 는 0단계부터 drain 을 적는다. 운영자 시작(`by-pnu-pack-operator.sh`)은 그 목록에 든 작업만 거부한다.

### 2. 서버가 빌드할 때는 예산이 답한다: 전부

`server-build` 는 ADR-0276 의 되돌리기 스위치다. 그때는 빌더가 돈다. `tools/host-memory-budget.contract.json` 의 식으로
상주 상한 33.94g + 빌더 22g + 예약 5g = 60.94g ≤ 62g 이다. 예산에 남는 자리는 1.06g 뿐이다. 가장 작은 무거운 작업도 그보다 크다.
예를 들어 by-PNU bake 의 상한은 `MemoryMax` 14 GiB 이고, 2026-10-28 00:42 시작분의 실측 `MemoryPeak` 는 7,425,720,320 B(6.9 GiB)다.
그래서 "자리를 만들 만큼만 멈춘다"의 답은 **모든 작업**이다. drain 은 `*` 이고 지금과 같다. 2b 단계의 되살리기도 이 경우에만 의미가 있어
이 경우에만 돈다.

### 3. 다시 켜는 것은 멈춘 것만

7단계와 실패 때의 되살리기는 `airflow dags list -o json` 을 한 번 물어 멈춰 있는 enabled DAG 만 켠다. 목록을 읽지 못하면 예전처럼
enabled 전부를 켠다(멈춘 DAG 를 남기는 쪽으로는 실패하지 않는다). 새 릴리스에서 처음 켜진 작업의 DAG 도 멈춘 채 생기므로 함께 켜진다.

### 4. 매번 돌던 단계는 입력을 밝히거나, 이유와 시간 예산을 밝힌다

`platforms/foundation-platform/config/deploy-steps.contract.json` 이 정본이다. 단계마다 둘 중 하나를 적는다.

- `inputs`: Worker 배포 계약(`config/worker-deploys.contract.json`)과 같은 모양. 릴리스 트리 아래 경로 목록이고, 지문은
  `worker_autodeploy.py` 의 것 하나다(`platform_fingerprint`). 마지막 성공 때 기록한 지문
  (`/var/lib/foundation-platform/deploy-steps/<단계>.inputs`)과 같으면 건너뛴다.
  - 기록이 없을 때, 입력이 없을 때, 무엇이든 읽지 못할 때는 돌린다. `deploy_steps.py` 는 3 일 때만 "건너뛰라"이다.
  - 직전 릴리스와 비교하지 않는 이유가 있다. 마이그레이션이 실패한 배포 뒤에는 직전 릴리스도 이 계약으로 마이그레이션되지 않았다.
  - 기록은 "이 호스트에서 이 바이트로 성공했다"는 사실 하나다.
  - `lakehouse-migrate` 가 이것이다. 입력은 계약 디렉터리, Spark 작업 디렉터리(백필이 동적으로 import 된다), compose, 실행 스크립트,
    유닛이다.
- `always` + `budget_seconds`: 왜 매번 도는지와 걸려도 되는 시간이다. 넘으면 stderr 에 한 줄을 쓴다.
  - `postgres-chain`(77초 측정 → 150초): 런타임 이미지가 릴리스의 것이고 마이그레이션을 품는다. 사슬을 `--no-deps` 로 줄이는 것은
    2026-09-02 사고의 가드(`scripts/guard/the-deploy-does-not-restate-the-compose-chain.sh`)가 금지한다.
  - `start-once`(135초 측정 → 300초): ADR-0173.

DAG 멈춤·기다림·멈춤 결정은 이 계약의 단계가 아니다. 1의 hold 목록이 정한다.

## Consequences

- hold 가 빈 릴리스(코드만 바뀐 대부분의 배포)는 어떤 작업도 건드리지 않는다. 20시간 bake 가 돌아도 병합을 멈출 이유가 없다.
- 표를 바꾸는 릴리스는 그 표를 쓰는 작업만 멈춘다. Postgres 마이그레이션이 든 릴리스는 지금처럼 전부를 멈춘다. 이제 중단 가능한 작업은
  기다리지 않고 멈춘다. 그런 릴리스는 도는 bake 를 멈추고, bake 는 배포 뒤 이어 굽는다(같은 run 디렉터리).
- 77198c1f 같은 릴리스는 약 4분 20초가 줄어든다(멈춤 56초 + 2b 58초 + 레이크하우스 1분 51초 + 7단계 약 50초).
- ADR-0301 §3(긴 pause 경보)은 이 결정과 별개로 아직 열려 있다. 지금의 경보는 그대로다. 5시간을 넘긴 배포는 실패하고,
  `foundation-autodeploy.service` 의 `OnFailure` 가 알린다. drain 이 좁아져 "긴 pause"가 걸리는 작업도 hold 목록뿐이다.
- 위험: `release_changes.py` 가 어떤 작업의 쓰기를 놓치면 그 작업은 옛 계약으로 돈다. 그래서 그래프가 자리를 모르는 작업은
  붙잡힌 쪽으로 센다(D3). 판정을 넓히는 것은 그 스크립트의 일이다.

## 기각한 대안

| 대안 | 기각 이유 |
|---|---|
| 표 리스(ADR-0270 §3 의 완성형) | 옳은 끝 모양이지만 러너·대기열이 먼저다. hold 목록은 그 근사로 이미 운영에서 검증됐다(D5, 2b단계) |
| 직전 릴리스와의 차이로 레이크하우스 마이그레이션 건너뛰기 | 마이그레이션이 실패한 뒤의 배포가 바뀐 것 없음으로 보고 영영 건너뛴다 |
| Postgres 사슬에 `--no-deps` | 2026-09-02 사고(grants 미실행)의 가드가 금지한다. 77초는 예산으로 드러낸다 |
| `server-build` 에서도 hold 만 비우기 | 22g 빌더 옆에 남는 예산이 1.06g 이다. bake 하나(상한 14g)도 들어가지 않는다 |
| 중단 가능한 작업을 grace 뒤에 멈추기(ADR-0301 의 문구) | 붙잡힌 작업은 어차피 새 계약으로 다시 돌아야 한다. grace 동안 배포와 붙잡힌 DAG 만 늦어진다. systemd 가 유닛의 `TimeoutStopSec` 만큼 정리할 시간을 준다 |

## 검증

- `platforms/foundation-platform/orchestration/tests/test_foundation_autodeploy.py` `TheWholeDeploy` 가 다음을 본다.
  - 아무것도 붙잡지 않은 레지스트리 릴리스는 DAG 를 멈추지도 켜지도 않는다. 도는 작업을 묻지도 않는다.
  - 붙잡힌 작업만 멈추고 다른 작업이 돌아도 기다리지 않는다.
  - 붙잡힌 비중단 작업은 기다린다.
  - 붙잡힌 중단 가능 작업은 한 번 멈춘다.
  - `server-build` 는 전부를 비운다.
  - 기존 시험(알 수 없음 → 전부, 실패 시 되살리기)도 그대로 통과한다.
- `orchestration/tests/test_deploy_steps.py` 가 다음을 본다.
  - 계약의 모든 단계가 입력, 또는 이유와 예산을 갖는다. 입력 경로는 실재한다. 배포 스크립트가 단계 이름으로 묻는다.
  - 같은 바이트면 건너뛰고, 바뀌면 돈다. README 는 입력이 아니다.
  - 입력이나 계약이 없으면 절대 건너뛰지 않는다. 예산을 넘으면 그렇다고 말한다.
