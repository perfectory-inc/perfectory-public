# ADR 0410: 저장소는 시험할 수 있는 정리 규칙 다섯 개를 지키고, 모든 산출물은 선언 하나에서 생성된다

- Status: Accepted (2026-11-04, 소유자 지시)
- Date: 2026-11-04
- Builds on: [AGENTS.md 최상위 원칙](../../AGENTS.md)(근본 해결·SSOT·재발 불가, 오픈소스 우선),
  [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md)·[ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(위생 = 기계 강제 불변식),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분 처리가 기본),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(파티션 자산, 엔진 시험 (d) 로 Dagster),
  [ADR-0334](./0334-each-user-journey-has-an-slo-and-every-data-release-is-written-audited-and-canaried-before-it-is-served.md)(WAP),
  [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)(스키마 레지스트리),
  [ADR-0176](./0176-generated-docs-do-not-make-unrelated-prs-conflict.md)(생성 문서),
  [ADR-0207](./0207-a-pull-request-runs-only-the-guards-whose-inputs-it-changes.md)(가드 목록 `tools/ci/change-scope.json`)
- Amends: [ADR-0123](./0123-data-contracts-are-generated-odcs-from-the-two-sources-that-already-own-the-facts.md) §1·§2 — 이행 3단계가 끝나면 생성 방향이 거꾸로 된다(아래 §2)
- 전수 목록: [하이엔드 정리 인벤토리](../roadmap/high-end-cleanup-inventory.md)
- 관련 사건: 2026-10-28 자산 이벤트 3912 가 손으로 만든 Airflow 자산 사슬에서 사라져 공매 굽기가 약 13시간 기다렸다(ADR-0333).
  2026-10-26 사진 100% 실패는 같은 객체 저장소 읽기를 형제 둘이 서로 다르게 처리해서 났다(ADR-0299).
  2026-11-04 전수 조사에서 `docs/technology-stack.md` 가 SIGSEGV 로 금지된 Iceberg 1.6.1 을 현행으로 적고 있었다.
  런북 `lakehouse-registry.md` 는 파서가 거부하는 `run-national-data-collection --execute` 를 안내하고 있었다.
  가드 두 개(`xtask-path-coverage`, `area-ci-input-coverage`)는 ADR-0207 뒤로 아무것도 검사하지 않고 통과해 왔다.

## Context

2026-11-04 소유자 지시: 저장소 전체를 대기업 수준의 정제된 프로젝트로 만든다. 성숙한 오픈소스와 표준 도구를 쓰고
바퀴를 다시 만들지 않는다. 우리가 만드는 것은 도메인 논리(한국 공공데이터, PNU, 법정동 코드, by-PNU 묶음 형식)뿐이다.
죽은 코드·일회성 명령·땜질 위 땜질 가드·중복 구현을 지우고, 흩어진 것은 코드와 문서 모두 SSOT 로 모은다.

같은 날 origin/main(`5ec54b93`)을 표본이 아니라 전수로 조사했다. 줄 수는 `git grep -c '' origin/main -- <path>` 로 쟀다.

| 범주 | 항목 | 줄 수(잰 값) | 대표 |
|---|---:|---:|---|
| A 재발명 | 68 | 약 84,000 | 손 Iceberg 스캔, 아웃박스 중계 5벌, JWKS 검증기 4벌, 손 MVT 해독 4벌, 손 CLI 파서(하위 명령 177개) |
| B 코드 중복 | 55 | 레인 하나 = 손 편집 약 14곳 | by-PNU 레인 목록 사본 30+ 파일, 작업 목록 3벌, 시도 코드 3벌, PNU 규칙 2가지 |
| C 문서 중복·낡음 | 24 | md 704개·68,567줄 중 낡음 218·고아 92 | 틀린 버전 숫자, 대체된 ADR 을 현행으로 인용 22건 |
| D 죽은 코드 | 25 | 약 70,600 | 막힌 API 전국 수집 계열 29,114, 필지 앵커 7,408, 아무도 안 부르는 하위 명령 23개 |
| 가드 100개 | 76 정리 대상 | 구현 24,439 + 자기 시험 12,039 | 사본 대조(E) 24, 도구 규칙으로(R) 38, 땜질(P) 10, 죽음 4 |

원인은 세 가지로 모인다.

1. **선언이 여럿이다.** 한 데이터셋의 사실이 `orchestration/jobs.v1.json`, `docs/catalog/pipeline-graph.v1.json`,
   `config/r2-connections.contract.json`, Rust 표 상수, 원천 계약, systemd 유닛, 시험에 나뉘어 손으로 적힌다.
   그래서 사본을 맞추는 가드가 생기고(E 24개), 가드 사이 구멍을 메우는 가드가 또 생긴다(P 10개).
2. **영역 간 공유 수단이 없다.** ADR-0001 이 영역 사이 Cargo 경로 의존을 막아서, 같은 재시도·검증기·아웃박스·R2 읽기를
   작업 공간마다 다시 쓴다. `products/gongzzang/crates/gongzzang-r2-object-storage/src/lib.rs` 첫 주석이 그렇게 말한다.
3. **고르기 전에 만들었다.** `docs/technology-stack.md` §1.1 의 채택 목록이 생기기 전에 만든 기제가 그대로 남았다.
   이미 Cargo.lock 에 든 크레이트(`shapefile`/`dbase`, `geo`, `tower` limit, `sqlx` test)를 두고 손으로 다시 짠 것도 있다.

대기업·오픈소스 사례:

- **Google 모노레포**: "버전은 하나" 규칙, 생성 코드는 빌드가 만들고 커밋하지 않는다(Bazel `genrule`).
- **Airbnb Minerva**: 지표는 한 번 정의하고 모든 소비처가 그 정의에서 받는다.
- **Dagster software-defined assets**: 자산 정의 하나가 의존·파티션·자동 실행·검사·코드 버전을 함께 가진다.
- **ODCS (Bitol, Linux Foundation AI & Data)**: 데이터 계약 표준. `datacontract-cli` 가 DDL·스키마·품질 시험을 계약에서 생성한다.
- **Netflix Maestro**: 신호(파티션 준비)가 워크플로를 시작한다.
- **Write-Audit-Publish (Iceberg 브랜치, Netflix 발표)**: 발행 전에 감사한다.
- **정책 도구**: OPA/Conftest, Semgrep, zizmor, actionlint 는 손으로 짠 정책 파서 대신 쓰는 업계 표준 정책 엔진이다.

## Decision

### 1. 다섯 기준은 시험할 수 있는 규칙이다

| # | 규칙 | 시험(통과 조건) |
|---|---|---|
| R1 재발명 금지 | `docs/technology-stack.md` §1.1 에 있는 필요를 직접 구현하지 않는다. 직접 구현하는 기제는 검토한 후보와 기각 이유를 ADR 에 남긴 것만 있다 | 인벤토리 A 의 남은 행 = "남김(이유 ADR)" 행뿐. 금지 모양(손 hex·손 HMAC·`reqwest::Client::builder()` 를 공유 HTTP 크레이트 밖에서·손 Prometheus 텍스트·`FOR UPDATE SKIP LOCKED` 를 큐 크레이트 밖에서·손 Avro manifest 읽기)은 Semgrep 규칙이 거부한다 |
| R2 도메인만 만든다 | 직접 만드는 범위는 한국 공공데이터 어댑터, PNU, 법정동 코드 이력과 변경 추적(ADR-0329 §3 `views.changed_codes`), by-PNU 묶음 형식과 내용 일치 관문이다. 그 밖은 R1 을 따른다 | 인벤토리 "도메인이라 남기는 것" 목록 밖의 직접 구현이 0 |
| R3 찌꺼기 0 | 저장소의 모든 명령·모듈·스크립트·설정에는 시험 아닌 호출자(작업·유닛·CI·배포)나 런북의 운영자 절이 있다. 일회성 코드는 `one-off: <끝나는 조건>` 머리말을 달고 조건이 끝나는 PR 에서 지운다 | 호출자 없는 하위 명령 0, `cargo-machete`·`knip`·`vulture` 의 미사용 0, 끝난 `one-off:` 0 |
| R4 SSOT(코드·문서) | 한 사실은 한 선언에만 있다. 사본은 생성물이고 커밋하지 않는다(빌드·CI·배포가 만든다). 시험은 계약 값을 다시 적지 않고 계약에서 읽는다. 손으로 센 개수(`[Self; 20]`, `len()==12`)를 단정하지 않는다 | 인벤토리 B·E 의 남은 행 0, 사본 대조 가드 0 |
| R5 문서 정리 | 문서는 사실을 링크하거나 생성한다. 버전·호스트·경로·목록을 다시 적지 않는다. `status: current` 문서는 대체된 ADR 을 현행 근거로 들지 않는다. README 아닌 문서에는 들어오는 링크가 있다. 측정 일지·사고 서술은 비공개 `perfectory-ops` 로 간다 | 인벤토리 C 의 남은 행 0, 아래 §4 문서 검사 통과 |

### 2. 목표 구조 — 선언 하나, 생성물은 커밋하지 않는다

1. **데이터셋의 정의는 하나다.** 데이터셋마다 ODCS 계약 하나와 그 계약에서 생성되는 Dagster 자산 정의 하나가 정본이다.
   ODCS 는 칸·타입·필수·허용 값·품질 규칙·담당·신선도 예산·파티션·처리 범위(ADR-0314 `scope`)·법정동 추종을 가진다.
   Dagster 자산은 자산 키(= 지금 그래프 노드 id), 상류, 파티션, `AutomationCondition`, `code_version`, 실행기를 가진다.
   실행기는 `dagster-pipes` 로 부르는 지금의 systemd 유닛이다.
2. **생성물.** 아래는 모두 그 선언에서 빌드·CI·배포 시점에 만들고 저장소에 두지 않는다.
   - Iceberg DDL 과 스키마 이주
   - Rust·Python 칸 상수
   - systemd 유닛(템플릿 인스턴스)
   - 파이프라인 그래프, by-PNU 레인 색인
   - 작업 목록과 의존 파일(`orchestration/job-dependencies.v1.json`)
   - dbt sources, 스키마 레지스트리 주제
   - DataHub 등록과 계보(OpenLineage)
   - 데이터 지도 문서와 수집 카탈로그 문서

   생성기가 돌 수 없는 소비처(예: Worker 설정)는 그 빌드 단계에서 생성한다. 외부 소비자에게 내보내는 공개
   계약(OpenAPI·이벤트 스키마)만 커밋하며, 이것은 생성과 일치 검사를 함께 둔다(ADR-0176 의 방식).
3. **이행 순서(strangler).** 지금 정본인 `orchestration/jobs.v1.json`·`docs/catalog/pipeline-graph.v1.json`·
   `infra/lakehouse/contracts/*`·`config/*.contract.json` 은 이행 동안 정본으로 남는다. 자산 하나씩 ODCS + Dagster
   정의로 옮기고, 옮긴 자산의 손 사본을 같은 PR 에서 지운다. ADR-0333 의 엔진 판정((d) 실패 → Dagster)을 따른다.
   - 1단계: 생성기와 첫 자산.
   - 2단계: by-PNU 레인 전부.
   - 3단계: Silver 레인 전부와 Rust 칸 상수의 생성 전환.
   - 3단계가 끝나면 ADR-0123 §1·§2 의 "ODCS 는 생성물, Rust 계약과 지도가 정본"이 거꾸로 된다.
4. **발행 때 검사는 모든 발행 데이터셋에 있다.** 발행은 WAP(ADR-0334)다: 브랜치에 쓰고, 감사하고, 통과해야 main 으로 빨리 감기한다.
   - 감사는 계약의 `quality` 를 Dagster 자산 검사 또는 `datacontract test` 로 돈다.
   - 검사 항목: 스키마, 현재 키 유일성, 행 수 변화 한계, 신선도, 법정동 `changed_codes` 반영.
   - 품질 블록이 없는 계약은 거부한다. 예외 없이 모든 발행 표·서빙 레인에 적용한다.
5. **영역 간 공유.** 영역 사이 결합은 지금처럼 공개 계약(HTTP·이벤트)만이다. 도메인 없는 공용 부품(재시도·검증기·아웃박스
   중계·객체 저장소·관측)은 루트 공용 라이브러리 작업 공간 하나에만 둔다. 이것은 ADR-0001 의 "영역 간 Cargo 경로 의존 금지"를
   좁히는 결정이라 예약 번호 ADR-0411 로 따로 결정한다. 그 전까지 공용 부품 정리는 영역 안 중복부터 지운다.
6. **오픈소스 선택.** 기본값이며, 교체할 때 그 PR 이 현행 크레이트 판으로 기능 범위를 확인한다(인벤토리의 "확인 못 함" 항목).

| 필요 | 채택 | 대체되는 것(인벤토리 id) |
|---|---|---|
| 일정·자산·자동 실행 | Dagster(+`dagster-pipes`, `dagster-openlineage`) | A-F01~05, A-F07, A-F12 |
| 데이터 계약·DDL·품질 | ODCS + `datacontract-cli`, Dagster asset checks | A-F13, A-F14, A-F16 |
| Iceberg 읽기 | `iceberg`(iceberg-rust) `TableScan`, `iceberg-catalog-rest`, `pyiceberg` | A-F08~10 |
| 바뀐 행 | Iceberg `create_changelog_view`·`MERGE INTO` | A-F11 |
| 객체 저장소 | `object_store` | A-F23, A-O07 |
| 재시도·차단기·속도 | `reqwest-middleware`+`reqwest-retry`, `backon`, `failsafe`/`tower`, `governor` | A-F22, A-F24, A-F25, A-O01, A-O06 |
| 큐·아웃박스 | Debezium Outbox Event Router(이미 쓰는 Kafka), `pgmq` | A-F18, A-F21, A-O02, A-O18 |
| JWT·OIDC | `jsonwebtoken`+`jwtk` 원격 JWKS, `openidconnect`, `tower-sessions` | A-F29, A-O03, A-O04 |
| 지도 | `geozero`/`mvt-reader`, `pmtiles`, `geo`, `wkb`, tippecanoe | A-F32~35, A-O09 |
| CLI·설정 | `clap` derive(`#[arg(env)]`), `figment` | A-F39, A-F40 |
| 관측 | `metrics`+`metrics-exporter-prometheus`/`prometheus-client`, OpenLineage, blackbox_exporter | A-F06, A-F26, A-F27, A-F43 |
| API 클라이언트 | `progenitor`, `openapi-typescript`+`openapi-fetch` | A-O10, A-O15, B-O01, B-O09, B-O11 |
| 정책 가드 | Semgrep/ast-grep, Conftest(OPA), zizmor+actionlint, check-jsonschema, hadolint, ls-lint, cargo-deny, clippy `disallowed-*`, ESLint `no-restricted-*` | 가드 R-OSS 38개 |
| 미사용 검출 | `cargo-machete`, `knip`, `vulture` | D 재발 방지 |
| 의존성·취약점 | Renovate, `osv-scanner` 설정의 `IgnoredVulns` | A-F50, G068 |

### 3. 정리 순서

인벤토리의 실행 순서가 정본이다.

- (a) 지금 병렬로 할 수 있는 것: 진행 중 PR 과 겹치지 않고 구조 전환에 기대지 않는 것.
- (b) 진행 중 PR(행 버전 사슬, ADR-0336 이전, 수집 레인 초안)이 병합된 뒤.
- (c) 구조 전환(§2.3).

가드는 정본을 고친 PR 에서 함께 지운다. 정본을 먼저 고치지 않고 가드만 지우지 않는다.

### 4. 범주마다 0 을 지키는 가드(ADR-0001)

가드 목록의 정본은 지금처럼 `tools/ci/change-scope.json` `guards.run` 하나다. 각 항목에 `class` 를 더한다.
값은 `invariant`(도메인 불변식), `tool-rule`(OSS 도구 규칙의 얇은 실행), `ratchet`(줄기만 하는 기준선), `copy-check`(사본 대조) 중 하나다.

| 범주 | 0 을 지키는 장치 | 실패가 막는 실제 사고 |
|---|---|---|
| A 재발명 | Semgrep 규칙 묶음(§1 R1 의 금지 모양) + cargo-deny `[bans]` 의 중복 크레이트 금지. 규칙마다 허용 위치는 그 OSS 를 감싼 어댑터 하나 | 2026-10-14 #559·10-15 #594: R2 읽기 경로가 여러 벌이라 재시도 기본값이 달랐다 |
| B 코드 중복 | 생성물 디렉터리는 `.gitignore` 에 있고 커밋되면 거부. 계약에서 생성되는 값(레인 이름·호스트·데이터셋 id)을 생성기 밖에서 문자열로 적으면 Semgrep 이 거부. 시험의 손 개수 단정 금지 | 2026-10-04 지역 코드 쌍 사본 불일치(가드 `region-code-pairs-have-one-home` 이 생긴 계기), 2026-09-02 Silver 넘김 접두사 세 벌 |
| C 문서 | `scripts/catalog/documentation_ratchets.py` 확장 4개: 대체된 ADR 의 현행 인용 거부, 디렉터리·크레이트 경로의 죽은 참조, README 아닌 고아 문서, 계약 키가 가진 버전 숫자의 문서 재진술 | 2026-11-04 `technology-stack.md` 가 금지된 Iceberg 1.6.1 을 현행으로 안내 |
| D 죽은 코드 | clap `Subcommand` 이름마다 시험 아닌 호출자나 런북 운영자 절이 있어야 한다(목록은 clap 이 생성). `cargo-machete`·`knip`·`vulture` 를 CI 에서 돌린다. 끝난 `one-off:` 머리말 거부 | 2026-11-04 런북이 파서가 거부하는 명령을 안내, 막힌 API 수집 29,114줄이 도달 불가인 채 유지 |
| E 사본 대조 가드 | `class: copy-check` 와 `ratchet` 항목 수는 줄기만 한다. 대상은 지금 각각 24·10, 0 이 되면 그 값을 허용하지 않는다. 새 가드는 `invariant` 나 `tool-rule` 이고 사고 한 줄을 적는다 | 2026-11-04 확인: `xtask-path-coverage`·`area-ci-input-coverage` 가 ADR-0207(워크플로 `paths:` 제거) 뒤로 검사 대상 0 인 채 초록이었다 |

이 가드들은 각 범주의 첫 정리 PR 에서 착지하고, AGENTS.md `위생 불변식` 절에 한 줄씩 들어간다(ADR-0299 §5).

## Consequences

- 지금 정본 네 개(작업 목록·그래프·레이크하우스 계약·설정 계약)는 이행이 끝나면 ODCS + Dagster 정의 하나로 합쳐진다.
  끝날 때까지 새 데이터셋은 기존 정본에 적되, 손 사본을 늘리는 PR 은 R4 에 걸린다.
- 저장소에서 지울 줄 수의 상한 추정(인벤토리 합계, 계산): 죽은 코드 약 70.6k, 대체될 재발명 약 79k(남김 약 4.7k 제외),
  사본 대조·땜질·죽은 가드 구현 약 8.3k + 자기 시험 약 3.8k. 겹치는 파일이 있어 실제 합은 이보다 작다.
- Dagster 를 더하면 호스트 메모리가 약 1.5~2.5 GiB 늘어난다(ADR-0333 계산). 전환 동안 스케줄러가 둘이다(airlift).
- 생성물을 커밋하지 않으므로 리뷰어는 생성 결과를 CI 산출물로 본다. 공개 계약만 커밋과 일치 검사를 유지한다.
- 공용 라이브러리 작업 공간(ADR-0411)이 결정되기 전까지 영역 간 중복(A-O·B-O 의 다수)은 영역 안 정리에 그친다.
- 소유자 결정이 필요한 것: 브론즈 카탈로그 복구 레인(D09)을 재해 복구 도구로 남길지, 공짱 AWS 쪽 앵커 가져오기(D22)·Pulumi 두 벌(B-O07)은
  AWS 공짱 동결이 풀릴 때까지 손대지 않는다.

## 기각한 대안

- **표본 감사를 계속하며 눈에 띄는 것부터 지우기**: 같은 원인(선언 여럿)이 새 사본을 계속 만든다. 11-04 표본 조사의
  추정이 전수 결과와 달랐다. 가드는 "약 40 교체 가능"이었으나 실제는 62 다. MVT 해독기 "TS 3벌"은 main 에 없었다.
- **사본마다 대조 가드를 더 두기**: 지금 방식이다. 가드가 100개, 구현 2.4만 줄이 됐고 가드가 가드를 지키는 상태가 됐다.
- **생성물을 커밋하고 `--check` 로 맞추기**: 사본을 커밋하는 한 같은 파일을 여러 PR 이 고쳐 충돌한다(ADR-0176 이 겪음).
  리뷰 편의는 CI 산출물로 대신한다. 공개 계약만 예외다.
- **Airflow 위에서 계속 진화**: ADR-0333 시험 (d)(파티션 검사가 하류를 막음)가 Airflow 3.3.2 에서 되지 않았다.
