# ADR 0308: 배포는 릴리스가 붙잡는 작업만 멈추고, 입력이 그대로인 단계는 건너뛴다

- Status: Accepted (2026-10-28, 사장 승인)
- Date: 2026-10-28
- Builds on: [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §3(배포는 돌던 작업을 기다리지 않는다, D1·D2·D5),
  [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md) §13(서버는 레지스트리에서 받는다),
  [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)(`interruptible`),
  [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(빚진 실행의 기록),
  [ADR-0310](./0310-a-long-bake-finishes-at-its-pin-then-patches-forward.md)(bake 는 자기 핀에서 이어 굽는다),
  [ADR-0259](./0259-ci-migrates-the-production-table-shape.md) §5(운영 모양 캡처)
- 개정: 2026-10-29 보안·정확성 리뷰 반영(병합 전) — 멈춘 실행은 기록하고 다시 시작, 목록에 없는 DAG 도 켬, 마이그레이션 skip 지문에 카탈로그·jar·드리프트,
  hold 는 마이그레이션 입력과 같은 정본, `commits` 선언.
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
| 배포 뒤 한 번 작업 여섯 | 2분 15초 | 그 작업이 실행하는 것이 바뀌었을 때만(ADR-0173 의 목적) |
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
- hold 목록의 레이크하우스 몫은 레이크하우스 마이그레이션 단계의 **입력 그 자체**로 계산한다(아래 4의
  `config/deploy-steps.contract.json` `lakehouse-migrate`, 하나의 정본). pre-flight 는 그 입력 목록을 계약에서 읽어 커밋에서 꺼낸다.
  - 바뀐 입력이 표 단위로 읽히는 파일(`per_table`: 표 계약 `contracts`, 운영 모양 원장 `tables`)뿐이면 항목이 바뀐 표의 작성 작업만 붙잡는다.
  - 그 밖의 입력(마이그레이션 프로그램, 백필이 import 하는 Spark 작업, compose, 엔진 핀이 든 계약 등)이 바뀌면 마이그레이션이 모든 표를
    다시 훑으므로 **모든 레이크하우스 표**의 작성 작업을 붙잡는다.
  - 이 호스트의 기록이 지금 도는 릴리스를 마이그레이션했다고 말하지 않으면(실패한 마이그레이션, 카탈로그 복원, 기록 삭제, 운영 모양 드리프트)
    기준선이 없으므로 역시 모든 레이크하우스 표다.
  - 2026-10-29 리뷰 전에는 표 계약 파일 하나만 비교했다. 그래서 백필 코드만 바뀐 릴리스는 마이그레이션을 다시 돌리면서도 작성 작업을 붙잡지 않았다.
  - 비용(git 이력 계산): 2026-10-22–28 플랫폼 커밋 44개 중 22개가 표 계약 밖의 마이그레이션 입력을 바꿨다. 그런 배포는 레이크하우스 작성 작업을
    모두 붙잡는다. by-PNU bake 는 서빙 표면을 쓰고 레이크하우스 표를 쓰지 않으므로 붙잡히지 않는다. 입력을 마이그레이션의 실제 import 폐포(71개 모듈 중 24개)로
    좁히는 일은 후속 과제다. 좁히면 건너뛰기와 붙잡기가 함께 좁아진다.
- 붙잡힌 작업이 돌고 있으면: `interruptible: true` 면 멈추고 끝나길 기다린다(ADR-0301 §2). 부분 산출은 append-only 라 안전하다.
  bake 는 자기 핀에서 이어 굽는다(ADR-0310). `false` 면 기다린다(상한 5시간, 넘으면 배포 실패, 지금과 같다).
  - **멈춘 실행은 잃지 않는다.** 멈추기 전에 그 작업을 "실행을 빚진 작업"으로 기록한다. 기록 위치는 릴리스 빌드가 미룬 작업을 러너가 적는 곳
    (`job_units.py deferred-dir`, ADR-0260)과 같다. 7단계(또는 전환 전 실패 시 EXIT trap)가 DAG 를 켠 뒤 그 작업을 시작한다.
    러너는 작업이 실제로 시작될 때 기록을 지운다. 그래서 시작이 실패해도 다음 배포가 다시 시작한다.
  - 2026-10-29 리뷰: `*` 배포(Postgres 마이그레이션)가 20시간 bake 를 멈췄는데, `retries: 0`·`takes_turns` 라 아무것도 다시 시작하지 않았다.
    "배포 뒤 다시 돈다"는 로그 줄이 거짓이었다.
  - 기록하지 못하면(실행 중인 릴리스에 `job_units.py` 가 없는 등) 멈추지 않고 기다린다.
  - 어떤 작업이 끝에서만 커밋하는지는 그 작업이 `jobs.v1.json` 에 스스로 적는다(`commits`: `at_end` | `as_it_goes`, 모든 작업 필수).
    `at_end` 작업이 `interruptible` 이면 `orchestration/tests/test_job_specs.py` 가 거부한다. 시험 안의 고정 목록 7개는 없앴다.
    새 수집기는 선언 없이 들어올 수 없다.
- `/run/foundation-deploy-hold` 는 0단계부터 drain 을 적는다. 운영자 시작(`by-pnu-pack-operator.sh`)은 그 목록에 든 작업만 거부한다.

### 2. 서버가 빌드할 때는 예산이 답한다: 전부

`server-build` 는 ADR-0276 의 되돌리기 스위치다. 그때는 빌더가 돈다. `tools/host-memory-budget.contract.json` 의 식으로
상주 상한 33.94g + 빌더 22g + 예약 5g = 60.94g ≤ 62g 이다. 예산에 남는 자리는 1.06g 뿐이다. 가장 작은 무거운 작업도 그보다 크다.
예를 들어 by-PNU bake 의 상한은 `MemoryMax` 14 GiB 이고, 2026-10-28 00:42 시작분의 실측 `MemoryPeak` 는 7,425,720,320 B(6.9 GiB)다.
그래서 "자리를 만들 만큼만 멈춘다"의 답은 **모든 작업**이다. drain 은 `*` 이고 지금과 같다. 2b 단계의 되살리기도 이 경우에만 의미가 있어
이 경우에만 돈다.

### 3. 다시 켜는 것은 멈춘 것만

7단계와 실패 때의 되살리기는 `airflow dags list -o json` 을 한 번 묻는다. 그 목록이 **켜져 있다고 말하는 DAG 만 빼고** enabled DAG 를 모두 켠다.
목록을 읽지 못하면 예전처럼 enabled 전부를 켠다(멈춘 DAG 를 남기는 쪽으로는 실패하지 않는다). 목록은 인자가 아니라 stdin 으로 넘긴다(인자 하나는 128 KiB 상한).

- 새 릴리스가 더한 작업의 DAG 는 Airflow 가 아직 파싱하지 않아 목록에 없을 수 있다. Airflow 는 그 DAG 를 멈춘 채 만든다.
  "멈춤으로 나열된 것만 켠다"였던 첫 판은 그 DAG 를 영영 멈춘 채 두었다(2026-10-29 리뷰). 목록에 없는 DAG 도 켠다.

### 4. 매번 돌던 단계는 입력을 밝히거나, 이유와 시간 예산을 밝힌다

`platforms/foundation-platform/config/deploy-steps.contract.json` 이 정본이다. 단계마다 둘 중 하나를 적는다.

- `inputs`: Worker 배포 계약(`config/worker-deploys.contract.json`)과 같은 모양. 릴리스 트리 아래 경로 목록이고, 지문은
  `worker_autodeploy.py` 의 것 하나다(`platform_fingerprint`). 마지막 성공 때 기록한 지문
  (`state_directory`/`<단계>.inputs`)과 같으면 건너뛴다.
  - 기록이 없을 때, 입력이 없을 때, 무엇이든 읽지 못할 때는 돌린다. `deploy_steps.py` 는 3 일 때만 "건너뛰라"이다.
  - 직전 릴리스와 비교하지 않는 이유가 있다. 마이그레이션이 실패한 배포 뒤에는 직전 릴리스도 이 계약으로 마이그레이션되지 않았다.
  - 기록은 "이 호스트에서, 이 바이트로, 이 카탈로그와 jar 에 대해 성공했다"는 사실 하나다.
  - `lakehouse-migrate` 가 이것이다. 입력은 계약 디렉터리, Spark 작업 디렉터리(백필이 동적으로 import 된다), compose, 실행 스크립트,
    유닛이다.
- 바이트만으로는 카탈로그 상태를 말하지 못한다(2026-10-29 리뷰). 그래서 `lakehouse-migrate` 의 지문에 호스트 쪽 셋을 더한다.
  - `environment`: 유닛이 읽는 환경 파일에서 **이름을 지정한 비밀 아닌 설정만**(카탈로그 URI·웨어하우스·프로바이더) 읽어 해시한다.
    다른 줄은 해석하지 않는다. 값은 출력하지 않는다. 자격증명처럼 생긴 이름(TOKEN·SECRET·KEY 등)은 계약에 적혀도 거부한다.
    이름이 그 그룹(`config/runtime-secrets.contract.json`)에 있는지는 시험이 맞춘다.
  - `release_jars`: 릴리스 `build.json` 의 동결 jar 목록과 다이제스트(`SPARK_RELEASE_JARS` 의 출처)다. 버전 핀
    (`lakehouse-engine.contract.json`)은 이미 입력인 계약 디렉터리에 들어 있다.
  - `reverify`: 주기적 재검증은 새 작업을 만들지 않는다. 이미 매일·배포 뒤에 운영 모양을 잡는 `shape_ledger_check`(ADR-0259 §5)의 캡처를 읽는다.
    계약 표 가운데 캡처된 열에 계약 열이 빠진 표가 있으면 기록과 상관없이 돌린다. 카탈로그 복원이나 드리프트는 하루 안에 다시 검증된다.
    2026-10-28 12:36 운영 캡처 대 운영 릴리스 계약: 계약 표 50개 중 48개가 캡처에 있고 빠진 열은 0개다(실측). 지금은 거짓 재실행이 없다.
- 기록은 root 의 것이다. `state_directory` 는 `/var/lib/perfectory/deploy-steps`(0700, `install -d -o root -g root -m 0700`)다.
  - 처음 판의 `/var/lib/foundation-platform` 는 작업 계정 `foundation-platform` 소유(750, 실측)다. 그 계정이 기록 디렉터리를 바꿔치기해
    마이그레이션을 건너뛰게 할 수 있었다.
  - `deploy_steps.py` 는 부모가 root(또는 호출자) 소유이고 다른 계정이 쓸 수 없는지, 기록 디렉터리가 0700 인지 확인한다. 아니면 "돌린다"로 답한다.
- 강제 실행: 손으로 돌리는 `foundation-release.sh migrate` 는 항상 돈다. 배포만 `migrate --skip-unchanged` 로 건너뛸 수 있다.
  - `deploy_steps.py forget <릴리스> <단계>` 는 기록을 지워 다음 배포가 돌게 한다.
  - 어떤 실행이든 시작 전에 기록을 지우고, 성공한 뒤에만 다시 쓴다. 실패하거나 끊긴 실행이 카탈로그 상태로 오인되지 않는다.
  - 카탈로그를 복원한 뒤에는 둘 중 하나를 한다(`docs/runbooks/iceberg-snapshot-rollback.md`).
- `always` + `budget_seconds`: 왜 매번 도는지와 걸려도 되는 시간이다. 넘으면 stderr 에 한 줄을 쓴다.
  - `postgres-chain`(77초 측정 → 150초): 런타임 이미지가 릴리스의 것이고 마이그레이션을 품는다. 사슬을 `--no-deps` 로 줄이는 것은
    2026-09-02 사고의 가드(`scripts/guard/the-deploy-does-not-restate-the-compose-chain.sh`)가 금지한다.
- 배포 뒤 한 번 작업(`started_once_after_deploy`)은 작업마다 `start-once.<작업 id>` 단계다(`inputs`).
  - 그 작업이 이 호스트에서 성공한 적이 없거나, 릴리스가 그 작업이 실행하는 것을 바꿨을 때만 시작한다.
    입력은 그 작업의 스크립트, 유닛, 그리고 그것이 돌리는 코드·계약이다.
  - 2026-10-28 측정: `legal_dong_code_changes` 의 하루 약 10번(번마다 55 MB)이 스케줄이 아니라 이 단계에서 나왔다.
    스케줄 실행은 7일에 7번이었다.
  - 입력은 publisher 전체가 아니라 그 작업 몫으로 좁게 잡는다. 놓친 변경은 그 작업의 스케줄이 하루 안에 실행한다.
  - 실패한 시작은 기록하지 않으므로 다음 배포에서 다시 시작한다.
  - 깃발(`started_once_after_deploy`)은 그대로 둔다. 계약의 시험이 "깃발이 있는 작업 = `start-once.*` 단계"를 맞춘다.

DAG 멈춤·기다림·멈춤 결정은 이 계약의 단계가 아니다. 1의 hold 목록이 정한다.

## Consequences

- hold 가 빈 릴리스(코드만 바뀐 대부분의 배포)는 어떤 작업도 건드리지 않는다. 20시간 bake 가 돌아도 병합을 멈출 이유가 없다.
- 표를 바꾸는 릴리스는 그 표를 쓰는 작업만 멈춘다. 마이그레이션 프로그램이나 백필 코드를 바꾸는 릴리스는 레이크하우스 작성 작업을 모두 멈춘다.
  Postgres 마이그레이션이 든 릴리스는 지금처럼 전부를 멈춘다. 이제 중단 가능한 작업은 기다리지 않고 멈춘다.
  - 멈춘 작업은 실행을 빚진 작업으로 기록되고 배포가 스케줄을 돌려줄 때 시작된다. bake 는 자기 핀에서 이어 굽는다(ADR-0310).
- 77198c1f 같은 릴리스는 약 4분 20초가 줄어든다(멈춤 56초 + 2b 58초 + 레이크하우스 1분 51초 + 7단계 약 50초).
  배포 뒤 한 번 작업 중 입력이 그대로인 것(2분 15초 중 대부분)도 줄어든다.
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
| 전부 비우는(`*`) 배포에서는 멈추지 않고 기다리기(리뷰의 안 a) | 5시간 상한에 걸려 Postgres 마이그레이션 배포가 bake 뒤에서 실패한다(2026-10-26 사건과 같은 모양). 기록하고 다시 시작하면 잃는 것도 기다리는 것도 없다 |
| 레이크하우스 마이그레이션 skip 을 바이트 지문만으로 | 카탈로그 복원·재지정·jar 교체 뒤에도 건너뛴다. Bazel 의 action key 가 소스만이 아니라 도구·환경을 넣는 것, Terraform 이 plan 전에 실제 상태를 refresh 하는 것과 같은 이유다 |
| 주기적 재검증용 새 타이머 | `shape_ledger_check` 가 이미 매일·배포 뒤에 운영 모양을 잡는다. 그 캡처를 읽는다 |
| 중단 가능한 작업을 grace 뒤에 멈추기(ADR-0301 의 문구) | 붙잡힌 작업은 어차피 새 계약으로 다시 돌아야 한다. grace 동안 배포와 붙잡힌 DAG 만 늦어진다. systemd 가 유닛의 `TimeoutStopSec` 만큼 정리할 시간을 준다 |

## 검증

- `platforms/foundation-platform/orchestration/tests/test_foundation_autodeploy.py` `TheWholeDeploy` 가 다음을 본다.
  - 아무것도 붙잡지 않은 레지스트리 릴리스는 DAG 를 멈추지도 켜지도 않는다. 도는 작업을 묻지도 않는다.
  - 붙잡힌 작업만 멈추고 다른 작업이 돌아도 기다리지 않는다.
  - 붙잡힌 비중단 작업은 기다린다.
  - 붙잡힌 중단 가능 작업은 한 번 멈춘다. 멈춘 작업은 기록되고 7단계에서(전환 전 실패면 EXIT trap 에서) 시작된다.
    기록하지 못하면 멈추지 않고 기다린다.
  - Airflow 가 아직 나열하지 않은 DAG 도 7단계에서 켜진다.
  - `server-build` 는 전부를 비운다.
  - 배포 뒤 한 번 작업은 처음과 입력이 바뀐 때, 그리고 실패한 다음 배포에서만 시작한다.
  - 기존 시험(알 수 없음 → 전부, 실패 시 되살리기)도 그대로 통과한다.
- `orchestration/tests/test_deploy_steps.py` 가 다음을 본다.
  - 계약의 모든 단계가 입력, 또는 이유와 예산을 갖는다. 입력 경로는 실재한다. 배포 스크립트가 단계 이름으로 묻는다.
  - 같은 바이트면 건너뛰고, 바뀌면 돈다. README 는 입력이 아니다.
  - 입력이나 계약이 없으면 절대 건너뛰지 않는다. 예산을 넘으면 그렇다고 말한다.
  - 카탈로그 설정이나 jar 가 바뀌면 다시 돈다. 자격증명 교체는 바뀜이 아니다. 값은 출력되지 않는다. 자격증명 같은 이름은 거부된다.
  - 운영 캡처에 계약 열이 빠지면 기록과 상관없이 돈다. `forget` 뒤에는 돈다.
  - 다른 계정이 쓸 수 있는 부모나 열린 기록 디렉터리는 믿지 않는다.
  - 배포만 건너뛸 수 있다. 기록은 실행 전에 지우고 성공 뒤에 쓴다.
- `orchestration/tests/test_release_changes.py` 는 마이그레이션의 다른 입력이 바뀌면 모든 표의 작성 작업을 붙잡는지 본다.
  호스트 기록이 실행 중인 릴리스와 맞을 때만 그 릴리스를 기준선으로 쓰는지도 본다.
- `orchestration/tests/test_job_specs.py` 는 모든 작업이 `commits` 를 선언하는지, `at_end` 작업이 `interruptible` 이 아닌지 본다.
