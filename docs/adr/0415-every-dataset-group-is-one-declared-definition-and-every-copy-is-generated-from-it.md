# ADR 0415: 데이터셋 묶음마다 선언 하나가 있고, 모든 사본은 그 선언에서 생성된다 — 정의 형식·생성기·Dagster 배치·이행 순서

- Status: Accepted (2026-11-05, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-05
- Builds on: ADR-0410(목표 구조 §2 "선언 하나, 생성물은 커밋하지 않는다", #1051),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(파티션 자산, 엔진은 Dagster),
  [ADR-0334](./0334-each-user-journey-has-an-slo-and-every-data-release-is-written-audited-and-canaried-before-it-is-served.md)(WAP),
  [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)(스키마 레지스트리),
  [ADR-0176](./0176-generated-docs-do-not-make-unrelated-prs-conflict.md)(생성 문서),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)·[ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(systemd 가 보안 경계인 실행기)
- Amends: [ADR-0123](./0123-data-contracts-are-generated-odcs-from-the-two-sources-that-already-own-the-facts.md) §1 — 3단계가 끝나면 ODCS 의 원천은 정의 파일이다(ADR-0410 §2.3 이 예고한 방향 전환)
- 관련 사건: 2026-10-24 own-producer 종류 하나를 더하며 레인 사본 일부에만 적고 호스트 열거에서 빠뜨렸다(#814).
  2026-10-28 손으로 이은 Airflow 자산 사슬에서 이벤트 3912 가 사라져 공매 굽기가 약 13시간 기다렸다(ADR-0333).
  2026-11-04 공매 사진 Silver 에 행이 있는데 발행 묶음의 사진이 비었고, 용도지역 키가 겹쳤다 — 둘 다 발행 전에 아무도 검사하지 않았다.

## Context

레인 하나를 더하면 손 편집이 약 14곳이다(#1051 인벤토리 B: 계약 줄, 작업 2개, 유닛 2개, 메모리 항목 2개, 운영 스크립트 변수와 거부 목록,
그래프 노드·엣지, 스키마 레지스트리 주제, 문서 기록, SPARK_HELD 행, 생산 작업, 시험). 사본끼리 맞추는 가드가 24개이고,
열린 PR 들이 바로 이 파일들(`jobs.v1.json`, `pipeline-graph.v1.json`, `r2-connections.contract.json`, `registry.json`,
`host-memory-budget.contract.json`)에서 충돌한다.

2026-11-05 origin/main(`bf7f9a8b`)에서 잰 값:

| 파일 | 항목 | 형식 |
|---|---:|---|
| `orchestration/jobs.v1.json` | 작업 62 | 손 정렬 JSON(바이트가 `json.dumps` 와 다름) |
| `docs/catalog/pipeline-graph.v1.json` | 노드 168·엣지 176(엣지 id 176개 모두 `<from>-to-<to>`) | 손 정렬 JSON |
| `config/r2-connections.contract.json` `by_pnu_lanes` | 레인 12 | 손 정렬 JSON |
| `tools/host-memory-budget.contract.json` `scheduled_jobs.memory` | 작업 50 | 손 정렬 JSON |
| `contracts/schema-registry/registry.json` `subjects` | 주제 5 | 손 정렬 JSON |
| `orchestration/job-dependencies.v1.json` | 작업 62 | 이미 생성물(`job_specs.py --write-dependencies`) |
| `docs/data-pipeline-map.md` | — | 이미 생성물(`render-pipeline-map.py`) |

`parcel-energy` 레인 하나의 사실은 이 중 7개 파일에 흩어져 있다(아래 `declared_definitions.py measure`).

## Decision

### 1. 정의의 단위는 데이터셋 묶음 하나, 파일 하나다

`platforms/foundation-platform/definitions/<group>.definition.json` 하나가 한 묶음의 모든 사실을 가진다.
묶음은 Dagster 의 asset group 이고, by-PNU 레인이면 레인 이름, 아니면 작업이나 노드 이름이다.
스키마는 `scripts/catalog/definition.v1.schema.json`(JSON Schema 2020-12)이다. 생성기는 같은 디렉터리의 형제 렌더러(`render-pipeline-map.py`, `pipeline_graph.py`) 곁에 있는
`scripts/catalog/declared_definitions.py` 다. 1단계에는 정의 디렉터리가 아직 없다(§4).

| 절 | 담는 것 | 생성하는 사본 | Dagster 에서 |
|---|---|---|---|
| `assets` | 그래프 노드 | `pipeline-graph.v1.json` `nodes` | 자산 키(= 노드 id) |
| `dependencies` | 노드로 들어오는 엣지(**id 없이**; id 는 `<from>-to-<to>` 로 생성) | `edges` | 자산의 `deps` |
| `jobs` | 그 자산을 만드는 작업 | `jobs.v1.json` `jobs` | 자산을 물질화하는 op, 실행기는 그 systemd 유닛 |
| `by_pnu_lanes` | 레인 줄 | `r2-connections` `by_pnu_lanes` | 서빙 자산의 메타데이터 |
| `host_memory` | 작업이 쓰는 컨테이너 | `host-memory-budget` `scheduled_jobs.memory` | 실행 자원 태그 |
| `published_schemas` | 레지스트리 주제 | `registry.json` `subjects` | 발행 검사의 기준 스키마 |

각 절의 항목은 생성하는 사본의 필드를 그대로 가진다. 필드의 뜻과 검사는 지금처럼 그 사본의 검사기
(`job_specs.py`, `pipeline_graph.py`, `schema_registry.py`, 메모리 가드)가 정본이다. 정의 형식은 위치만 바꾸고,
같은 사실을 두 번 적지 않는다. 묶음이 소유하지 않는 것(각 사본의 산문 `*_means`, 풀, 상태 디렉터리,
레인 규칙, 레지스트리 머리말)은 `platform.definition.json` 하나에 있다.

파일 문법은 JSON 이다. YAML 1.2 의 부분집합이라 Dagster 쪽 YAML 로더가 그대로 읽고, 저장소의 모든 가드처럼
표준 라이브러리만으로 검사된다(러너에 PyYAML 이 없다). 저장소의 다른 정본도 모두 JSON 이다.

### 2. 처음 묶음 나누기는 지금 파일이 이미 말하는 것에서 정한다

`declared_definitions.py split` 의 규칙(사람의 판단 없음):

1. by-PNU 레인 `L` 의 묶음은 레인 줄, 서빙 노드 `L-by-pnu-section-packs`, Gold 노드 `gold-L`, 주제 `foundation-platform.<L 의 - 를 _>_by_pnu` 를 가진다.
2. 작업은 자기가 쓰는(엣지 `to`) 첫 노드 중 묶음이 있는 노드의 묶음에, 없으면 자기 이름의 묶음에 든다.
3. 노드는 그것을 쓰는 첫 작업의 묶음에, 없으면 자기 이름의 묶음에 든다.
4. 엣지는 그것을 실행하는 첫 작업의 묶음에, 없으면 도착 노드의 묶음에 든다. 메모리 항목은 그 작업의 묶음에 든다.

결과(잰 값): 정의 137개. `parcel-energy` 정의 하나가 작업 2(`gold_parcel_energy_rebuild`, `by_pnu_parcel_energy_bake`),
자산 2, 엣지 4, 레인 줄 1, 메모리 항목 2, 주제 1 을 가진다.

### 3. 생성기는 새로 쓰지 않고 있는 렌더러를 부른다

`declared_definitions.py generate` 가 정의에서 위 다섯 사본을 만들고, 이미 생성물이던 두 파일은
**그 파일의 렌더러**로 만든다: `job_specs.rendered_dependencies()`, `render-pipeline-map.py` 의 `render()`.
두 번째 구현은 없다. systemd 유닛의 `TimeoutStartSec=`·`ReadWritePaths=` 는 지금처럼 `foundation-jobs generate`(Rust)가
`jobs.v1.json` 에서 렌더하므로, 그 파일이 생성물이 되어도 사슬은 그대로다.

### 4. 1단계의 등가 증명: 커밋된 사본 → 정의 → 사본

가드 `scripts/guard/declared-definitions-reproduce-artifacts.sh`(자기 시험 포함, `tools/ci/change-scope.json` 에 등록)가
PR 마다 커밋된 사본을 정의로 나누고, 정의를 **파일로 쓰고 다시 읽은 뒤**, 사본을 생성해 비교한다.

- 다섯 원천 사본: 항목·필드·값·**키 순서와 항목 순서까지** 같다(`json.loads` 가 파일 순서를 지킨다).
  손 정렬 공백은 비교하지 않는다. 바이트는 2단계에서 생성 형식(`json.dumps(indent=2, ensure_ascii=False)`)이 정본이 된다.
- 이미 생성물인 두 파일: 바이트 단위로 같다.
- id 정렬 생성(2단계의 형식, 순서 목록 없음)이 같은 항목 집합을 만든다.
- 거부: 이름이 `<from>-to-<to>` 가 아닌 엣지, 두 번 나오는 id(한 정의가 한 id 를 소유), 생성 파일의 손 편집.

1단계는 정의 파일을 **커밋하지 않는다**. 커밋하면 정본이 둘이 되고, 지금 `jobs.v1.json` 을 고치는 열린 PR 이 모두 정의도 함께 고쳐야 한다.
대신 모든 PR 이 자기 사본으로 등가를 증명하므로, 열린 PR 은 아무것도 바꾸지 않고 병합되고 전환(2단계)은 매 PR 에서 미리 증명된다.
이 가드가 막는 사고: #814 처럼 사본 일부에만 적힌 레인 — 전환 뒤에는 사본이 생기지 않고, 전환 전에는 정의 형식이 담지 못하는 모양을 거부한다.

### 5. 이행 순서와 되돌리기

| 단계 | 내용 | 시작 조건 | 되돌리기 |
|---|---|---|---|
| 1(이 ADR) | 정의 형식·생성기·등가 가드. 새 파일만 | 없음 | PR 되돌리기 하나. 운영 영향 없음 |
| 2 전환(PR 하나) | `split` 결과를 커밋하고, 다섯 원천 사본과 두 생성 파일·그래프 예제를 지운다(`.gitignore`). CI·배포(`foundation-release-build`)·Worker 빌드·`cargo` 빌드 스크립트가 `declared_definitions.py generate` 로 만든다. 사본 대조 가드(인벤토리 E 24개)와 손 개수 기준선을 은퇴한다. Python·셸·TS·Rust 의 레인 목록(인벤토리 B-F02, 37 파일)은 생성된 레인 색인 하나를 읽는다 | 코디네이터가 새 레인을 얼린 창, 열린 레인 PR 이 빠진 뒤(에스컬레이션) | 전환 PR 되돌리기: 사본이 다시 커밋되고, 1단계 가드가 그 사본에서 다시 돈다. 생성 결과가 사본과 같다는 것이 1단계에서 이미 증명되어 데이터 차이 없음 |
| 3 Dagster | ai-server 에 compose 로 Dagster(웹서버·데몬·코드 위치 gRPC, 저장소는 기존 Postgres 의 별도 DB). 코드 위치가 정의 디렉터리를 읽어 묶음마다 자산을 만든다. 실행기는 지금의 systemd 유닛: op 가 ADR-0122 의 SSH forced command 로 유닛을 시작하고, 결과는 Dagster Pipes 의 사용자 정의 message reader 가 유닛 저널에서 읽는다(지금의 `foundation-job-outcome` 줄을 대체). #946 airlift observe 를 다리로 DAG 하나씩 옮긴다. 발행 데이터셋마다 `@asset_check(blocking=True)`: 첫째 공매 사진(사진 Silver 에 행이 있으면 발행 묶음의 사진이 비지 않음), 용도지역 현재 키 유일성 | 2단계 병합 | DAG 하나씩: 옮긴 DAG 를 Airflow 에서 다시 켜고 Dagster 자산의 자동 실행을 끈다(airlift 의 proxy 되돌리기) |
| 3 끝 | `asset_reconcile.py`, 작업 결과 줄 프로토콜, run-now 디스패치를 은퇴(Dagster 의 Declarative Automation·Pipes·launch 가 맡음) | 모든 DAG 이전 | 은퇴 PR 되돌리기 |

2·3단계는 각각 관심사 하나의 PR 이다. 2단계 전에 코디네이터에게 레인 동결을 요청한다.

### 6. 오픈소스 선택과 근거

| 필요 | 채택 | 근거 |
|---|---|---|
| 자산·의존·자동 실행 | Dagster software-defined assets, Declarative Automation | 자산 정의 하나가 의존·파티션·자동 실행·검사를 함께 가진다(dagster.io/docs, "Assets"). ADR-0333 시험 (d) 가 Airflow 3.3.2 에서 실패 |
| 발행 전 검사 | `@asset_check(blocking=True)` | 검사가 실패하면 같은 실행의 하류 자산을 물질화하지 않는다(dagster.io/docs/guides/test/asset-checks; 1.6.4 changelog) |
| 외부 실행기 | Dagster Pipes(사용자 정의 message reader) | 오케스트레이터가 외부 프로세스를 부르고 메시지 읽기를 바꿀 수 있다(dagster.io/docs/integrations/external-pipelines/dagster-pipes-details-and-customization). systemd 는 보안 경계로 남는다(ADR-0122·0270) |
| 이행 다리 | `dagster-airlift` observe → proxy → migrate | DAG 하나씩 옮기고 되돌린다(ADR-0333 §3, #946) |
| 데이터 계약 | ODCS v3(Bitol, LF AI & Data) + `datacontract-cli` | 3단계에서 정의의 `assets` 가 ODCS 를 생성(ADR-0123 개정) |
| 생성물을 커밋하지 않기 | Bazel `genrule` 방식 | Google 모노레포는 생성 코드를 빌드가 만들고 커밋하지 않는다(ADR-0410) |
| 정의 파일 검사 | JSON Schema 2020-12, 2단계에 `check-jsonschema`(ADR-0410 §2.6 의 정책 도구) | 1단계는 생성기 자체가 모르는 절·엣지 id·중복을 거부 |

검토하고 기각한 것: dbt 의 YAML(변환 계층이지 스케줄·실행기가 아니다), Airflow Dataset/asset 확장(ADR-0333 (d) 실패),
Dagster Components YAML 을 지금 바로 정본으로 쓰기(Dagster 가 아직 배치되지 않아 1단계 증명이 Dagster 실행에 묶인다. 3단계에서 정의 파일을 Component 의 입력으로 쓴다).

### 7. 단계마다 잰다

| 지표 | 지금(잰 값) | 2단계 뒤(목표) | 재는 법 |
|---|---:|---:|---|
| 새 by-PNU 레인의 손 편집(이 ADR 이 다루는 파일) | 7 파일(`parcel-energy`) | 정의 1 | `declared_definitions.py measure` |
| 새 레인의 손 편집(전부) | 약 14곳(인벤토리) | 정의 1 + 유닛 템플릿 인스턴스 0 | 2단계 PR 이 다시 잰다 |
| 사본 대조 가드(E) | 24 | 0 | `change-scope.json` `class: copy-check` |
| 스케줄 지연 | 3단계 전에 Airflow 에서 잰다 | Dagster 센서·자동화 주기 | 3단계 PR |

## Consequences

- 1단계는 운영에 아무 영향이 없다. 커밋되는 것은 생성기, 스키마, 가드 하나와 그 자기 시험이다. 가드는 가드 수를 하나 늘리고,
  2단계에서 사본 대조 가드 24개를 지우는 근거가 된다.
- 열린 PR 은 그대로 병합된다. 등가 가드는 PR 자신의 사본에서 돌므로, 정의 형식이 담지 못하는 새 모양(예: `<from>-to-<to>` 가 아닌 엣지 id)만 거부한다.
- 정의 형식은 사본의 필드를 그대로 담으므로 필드의 중복(예: 작업 설명의 산문)은 줄이지 않는다. 필드 정리는 각 사본 검사기의 일이다.
- `platform.definition.json` 의 `order` 는 1단계에만 있다. 2단계는 id 순으로 생성하고 그 목록을 지운다(생성물을 아무도 손으로 고치지 않는다).
- 1단계가 다루지 않는 레인 사본(유닛 12개, 운영 스크립트의 레인 목록, Rust 상수, 손 개수 시험)은 2단계 PR 이 생성된 레인 색인으로 옮긴다.

## 기각한 대안

- **1단계에서 정의 파일을 커밋하고 사본과 맞추기**: 정본이 둘이 되고(R4 위반), 열린 PR 마다 두 곳을 고쳐야 한다.
- **손 정렬 바이트까지 재현하는 생성기**: 파일마다 다른 손 정렬 규칙을 흉내 내는 서식기는 2단계에서 버릴 코드다. 데이터·순서 동등이 전환의 안전 조건이다.
- **정의를 Rust 로 생성(`foundation-jobs`)**: 소비자인 Dagster 코드 위치가 Python 이고, 이미 생성물인 두 파일의 렌더러가 Python 이다. 같은 렌더를 Rust 로 다시 쓰면 두 번째 구현이 된다.
- **Airflow 위에서 계속**: ADR-0333 (d) 실패, ADR-0410 기각 대안과 같다.
