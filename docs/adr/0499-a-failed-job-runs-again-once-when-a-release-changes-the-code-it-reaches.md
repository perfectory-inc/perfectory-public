# ADR 0499: 실패한 작업은 그것이 닿는 코드를 바꾼 릴리스에서 한 번 다시 돈다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산 조정기는 사건이 아니라 상태로 판단한다),
  [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)(풀이 거절한 시작은 큐에서 다시),
  [ADR-0467](./0467-a-scheduled-slot-no-start-followed-is-started-once-whatever-kept-the-start-away.md)(시작이 없던 일정 칸은 한 번 빚진다),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)(결과 원장),
  [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(Gold 생산자의 코드 버전)

## Context

2026-11-10~11 에 세 작업이 같은 모양으로 하루를 잃었다(잰 값, ai-server 읽기 전용):

| 작업 | 실패 | 고친 PR · 배포 | 다음 시도 |
|---|---|---|---|
| `data_go_kr_file_data` | 11-10 11:42Z, `MALFORMED_CSV_RECORD`(스테이징 CSV 를 `escape='\'` 로 읽음) | #1230 · 13:42Z | 11-11 11:40Z(일정 칸) |
| `zoning_tile_bake` | 11-10 10:28Z, "already serve Gold snapshot" | #1234 · 15:51Z | 11-11 10:20Z |
| `gold_parcel_real_transaction_rebuild` | 11-10 21:58Z, 드라이버 OOM(계보 653만 행 collect) | #1262 · 11-11 02:16Z | 11-11 09:50Z |

조정기는 풀이 거절한 시작(ADR-0462)과 아무 시작이 없던 일정 칸(ADR-0467)은 다시 빚지지만, **돌았다가 실패한** 작업은 다음 칸까지
그대로 둔다. 같은 코드로 곧바로 다시 돌리면 같은 이유로 실패하니 옳은 기본값이다. 하지만 그 사이 그 작업의 코드를 고친 릴리스가
배포되면 이야기가 다르다: 고쳐진 코드로 돌 수 있는데도 최대 하루를 기다린다. 사람이 run-now 를 누르지 않으면 아무도 다시 시작하지
않았다(사장 11-07: 사람 손도 시간창도 아니다).

같은 문제를 쓰는 곳들이 푸는 방식:

- **Dagster**: 자산마다 `code_version` 이 있고, `AutomationCondition.code_version_changed()` 는 배포가 그 값을 바꾸면 자산을 다시
  요청한다. 우리 Dagster 코드 위치는 아직 관찰 단계(65 shadow · 25 없음, `proxied`·`dagster` 0)라 이 조건이 지금 결정하는 작업은 없다.
- **Kubernetes**: Deployment 는 파드 템플릿 해시(`pod-template-hash`)가 바뀔 때만 새로 굴린다. 실패한 Job 은 같은 템플릿으로는
  `backoffLimit` 뒤 멈춘다.
- **Pants·Bazel**: 대상이 어떤 파일에 기대는지를 손 목록이 아니라 소스에서 추론한다(Pants 의 의존성 추론: Python import, 셸 `source`).

작업 목록에는 작업이 어떤 파일을 실행하는지가 없다. 배포 단계의 `start-once.<job>`(ADR-0308)는 입력 경로를 손으로 적는데, 그 방식을
90 작업에 넓히면 작업·스크립트가 바뀔 때마다 목록을 고쳐야 하고, 고치지 않은 목록은 조용히 코드와 어긋난다.

## 비교

| | A. 작업의 코드 버전을 트리에서 추론, 실패 줄에 남기고 비교 | B. 실패 뒤 배포가 있으면 무조건 한 번 | C. 작업마다 입력 경로를 손으로 |
|---|---|---|---|
| 다시 도는 때 | 그 작업이 닿는 파일이 바뀐 릴리스 | 아무 릴리스(하루 10여 회) | 적은 경로가 바뀐 릴리스 |
| 헛 재시작 | 공유 파일(발행기 바이너리, 공용 셸)이 바뀔 때 한 번 | 배포마다 실패 작업 전부 | 목록이 넓으면 많고, 좁으면 놓친다 |
| 유지 | 없음(추론) | 없음 | 작업·스크립트가 바뀔 때마다 |
| Dagster 이행 | 같은 값을 `AssetSpec.code_version` 으로 | — | 별도 |

**A 를 고른다.**

## Decision

1. **작업의 코드 버전은 추론한다**(`orchestration/dags/job_code_version.py`). 작업의 유닛 파일(템플릿이면 `name@.service`)에서 시작해,
   유닛·셸·Python 이 코드로 이름 대는 트리 파일을 따라간다. 접두(`/opt/foundation-platform/current/`, `${RELEASE_ROOT}/`, Spark 의
   `/workspace/`)는 떼고, 옆 파일과 트리에 하나뿐인 파일 이름도 찾는다. Python 은 옆 모듈의 import 를 따른다. 셸 주석, Python 주석·docstring
   은 코드가 아니다. 계약 등 데이터 파일(JSON·YAML·TOML·SQL)은 포함하되 따라가지 않는다: 그 산문이 이름 대는 것은 실행되는 것이 아니다.
   코드가 이름 대는 워크스페이스 바이너리(`foundation-outbox-publisher`)는 그 크레이트와 경로 의존 크레이트의 `Cargo.toml`·`build.rs`·`src`
   (`tests.rs`·`*_tests.rs` 제외)다. 템플릿 인스턴스(`foundation-silver-refresh@<lane>`)는 그 이름을 키나 값으로 가진 계약
   (`infra/lakehouse/contracts`·`config`)과, Gold 재빌드 계약이 그 유닛의 Gold 에 주는 생산자(`gold_rebuild.load_contract`,
   `gold-panel-rebuild.sh` 가 고르는 그대로)를 더한다. 버전은 경로순으로 (경로, 바이트의 SHA-256)을 이은 SHA-256 이다. docs·tests·
   생성 트리는 따라가지 않는다.
2. **실패한 run 의 원장 줄은 그 버전을 싣는다.** `job_units.record_outcome` 은 `via=run`·`outcome=failed` 줄에만, 자기 릴리스 트리(러너가
   작업을 돌린 바로 그 트리)로 계산한 `code_version` 을 붙인다. 계산이 실패해도 줄은 남는다(버전 없이). 결과 요약(`job-outcomes.json`)은
   `ran` 에 작업마다 끝난(changed·unchanged·failed) 가장 새 run 과 그 버전을 둔다.
3. **조정기가 한 번 시작한다**(`asset_reconcile.code_changed_starts`). 조정기가 다시 시작하는 작업(`job_specs.requeued`, ADR-0462)
   가운데, 가장 새 run 이 실패했고 그 줄의 버전이 지금 릴리스의 버전과 다르면 `foundation-asset-owed <job> <key>` 를 낸다. 키는
   (실패 시각, 새 버전)이라 틱마다 같고 DAG 는 한 번만 시작한다. 같은 버전에서 또 실패하면 다음 릴리스가 바꿀 때까지 다시 빚지지 않는다
   (고침마다 한 번, 반복 없음). 큐에 있는 작업(ADR-0462), `takes_turns`(ADR-0138), 멈춘 작업(ADR-0282 §3), 도는 작업은 건드리지 않는다.
   버전이 없는 옛 줄은 판정하지 않는다.
4. **Dagster 는 같은 값을 쓴다.** 관찰 자산(`observe_assets.observed_assets`)은 쓰는 작업의 버전을 `AssetSpec.code_version` 으로 싣는다
   (여러 작업이 쓰면 (작업, 버전) 목록의 해시). 작업이 `dagster` 단계로 옮기면 `AutomationCondition.code_version_changed()` 가 같은 값을
   읽는다. 지금 결정은 3 이 한다.

## Consequences

- 11-10 을 다시 재생하면(계산): `data_go_kr_file_data` 는 #1230 배포 뒤 첫 틱(13:45Z 무렵)에 다시 돈다. 일정 칸(11-11 11:40Z)보다 약
  22 시간 빠르다. `zoning_tile_bake` 는 #1234 배포 뒤 첫 틱에(약 18.5 시간 빠름), real-transaction Gold 는 #1262 배포 뒤 첫 틱에(약 7.5 시간
  빠름) 돈다. 세 작업 모두 고친 파일(`data_go_kr_file_data.py`, `zoning-tile-bake.sh`, `real_transaction_silver_to_gold.py`)이 추론된 파일에
  들어 있다(`test_job_code_version.py::TheRealTree`).
- 추론 결과(잰 값, 노트북): 작업당 중앙값 약 600 파일(대부분 발행기 크레이트), 90 작업 62 개 서로 다른 버전, 차가운 첫 계산 1.3 s, 90 작업 전부
  11 s. 원장에는 실패 줄에만 계산하고, 조정기는 실패한 작업만 계산한다.
- 헛 재시작: 발행기 바이너리나 `admitted-writer-runtime.sh` 같은 공유 파일이 바뀌면, 그것에 닿는 **실패한** 작업이 한 번 더 돈다. 성공한
  작업은 영향이 없다. 실패 하나에 고침 하나당 한 번이다.
- 놓치는 경우: 실행 시점에 인자로 정해지는 파일 가운데 1 의 두 템플릿 규칙 밖의 것, 이미지(도커 태그)와 호스트 설정 파일, `include_str!`
  로 읽는 크레이트 밖 파일. 놓치면 지금처럼 다음 칸에 돈다(더 나빠지지 않는다).
- 범위: Spark 풀의 재시작 대상(54 작업). 조정기가 직접 다루는 Gold·굽기(`reconciles`)는 이미 실패 뒤 백오프로 다시 시작한다
  (`gold_attempt`·`lane_verdict`). default_pool 작업 14 개는 이 규칙 밖이다.
- 출처: [Dagster code versions](https://docs.dagster.io/guides/build/assets/defining-assets#asset-code-versions),
  [AutomationCondition.code_version_changed](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.code_version_changed),
  [Kubernetes Deployment pod-template-hash](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#pod-template-hash-label),
  [Pants dependency inference](https://www.pantsbuild.org/stable/docs/using-pants/key-concepts/targets-and-build-files#dependencies-and-dependency-inference).
