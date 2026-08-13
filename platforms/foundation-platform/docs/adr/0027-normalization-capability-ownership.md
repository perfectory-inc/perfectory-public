# ADR 0027: Normalization 기능이 의미 결정과 제안 거버넌스를 소유

- Status: Accepted
- Date: 2026-07-17
- Supersedes: Normalization ownership implied by the Catalog umbrella
- Related: Foundation ADR 0021, Foundation ADR 0026, Gongzzang ADR 0048

## 배경

Foundation Platform은 deterministic normalization, semantic metadata, entity-impact detection,
AI proposal intake, human review, apply, rollback을 이미 소유했다. 이 결정 당시 Catalog의
영구 책임은 canonical entity와 command인데도 해당 동작은 `catalog-domain`,
`catalog-application`, `catalog-infrastructure` 안에 구현되어 있었다.

현재 배치에는 명확한 결함이 있다.

- Lakehouse materialization이 `catalog-domain`에서 normalization rule을 import한다.
- Normalization lifecycle error를 `CatalogError`로 표현한다.
- proposal command와 unit-of-work port가 Catalog port에 섞여 있다.
- 하나의 Catalog infrastructure file이 proposal persistence, review persistence, target별
  validation, canonical mutation SQL, outbox write, rollback을 모두 소유한다.
- package name에서 semantic metadata와 entity-impact ownership이 보이지 않는다.
- 향후 worker나 Kafka adapter가 사용할 단일 Normalization application boundary가 없다.

## 결정

### Capability packages

Normalization behavior moves to these packages:

```text
crates/normalization/normalization-domain
crates/normalization/normalization-application
crates/normalization/normalization-infrastructure
```

`normalization-domain`은 deterministic floor/unit rule, entity-context resolution, semantic
metadata, entity-impact mapping, proposal identity·lifecycle type, `NormalizationError`를 소유한다.

`normalization-application`은 proposal submit/review/apply/rollback use case, command, receipt,
`NormalizationUnitOfWork` port를 소유한다.

`normalization-infrastructure`는 PostgreSQL proposal/review/application ledger persistence와
승인된 canonical 변경을 조정하는 transaction을 소유한다.

### Dependency direction

```text
normalization-domain
        ^
        |
normalization-application
        ^
        |
normalization-infrastructure ---> catalog-application
        |                         catalog-infrastructure
        |
Foundation service composition roots

lakehouse-application ----------> normalization-domain
```

Catalog와 Collection package는 Normalization package에 의존하지 않는다. Lakehouse는 Silver
row를 materialize할 때 Normalization의 pure domain rule에 의존할 수 있다. canonical Catalog
SQL이 Catalog 소유로 남으므로 Normalization infrastructure는 transaction-scoped Catalog
infrastructure collaborator를 호출할 수 있다. 경계를 넘는 모든 request/result는 Catalog 소유
(`ComplexId`, `ComplexMutation`, Catalog mutation receipt)다. Catalog는 Normalization command나
type을 직접 import하지 않는다.

### Canonical apply transaction

AI는 proposal producer로 남고 human review는 필수다. Apply와 rollback은 하나의 Foundation
PostgreSQL transaction으로 계속 실행한다.

1. proposal 또는 이전 application을 lock한다.
2. lifecycle state와 예상 canonical version을 검증한다.
3. canonical mutation을 위해 Catalog 소유 transaction collaborator를 호출한다.
4. canonical state가 바뀌면 Catalog outbox event를 쓴다.
5. Normalization application/rollback ledger를 쓴다.
6. proposal status를 갱신한다.
7. 한 번 commit한다.

Catalog collaborator는 기존 SQLx transaction을 받고 직접 commit하지 않는다. Catalog SQL,
row mapping, optimistic-version check, Catalog event construction은 `catalog-infrastructure`에
남는다. Normalization infrastructure는 canonical Catalog mutation SQL을 복제하지 않는다.

Catalog collaborator가 canonical row를 바꾸고 outbox event를 insert한 뒤 실패하도록 atomicity
test를 만든다. Normalization application ledger insert나 proposal status update 실패가 canonical
row·outbox event·Normalization ledger·proposal status를 함께 rollback해야 한다.

Building-register unit overrides remain Normalization ledger records because they
do not directly mutate a canonical Catalog aggregate in the current slice.

Building-register-unit application은 target별 rooted·acyclic·unbranched predecessor chain 하나를
이룬다. immutable lineage는 historical tail을 따르고 active override state는 독립적으로
추적한다. active state는 rollback되지 않은 chain의 가장 깊은 application이다. 더 깊은
descendant가 active인 동안 ancestor를 rollback해도 오래된 값이 부활하지 않는다.
transaction 시작 timestamp와 UUID 순서는 audit metadata이며 state를 선택하지 않는다.
reader와 writer는 graph query 하나를 공유하고 missing link·extra root·branch·malformed
snapshot envelope·cycle은 즉시 실패한다.

Industrial-complex rollback은 compensating inverse patch다. 선택된 application이 바꾼 field만
복원한다. application과 compensating row는 version 순서의 LIFO stack을 이룬다. 선택된
application은 active stack top이어야 하며 version gap 없는 인접 row는 동일한 canonical
snapshot을 넘겨야 한다.
 B가 보상된 뒤 A는 최신 검증 ledger head를 기준으로 보상할 수 있다.
외부 Catalog mutation·non-LIFO request·malformed ledger handoff·ABA change는 canonical
mutation 전에 state conflict를 반환한다. lock된 canonical state와 같은 proposed patch는
version·outbox event·ledger row·status transition을 만들기 전에 거부한다.

### Error boundary

Normalization package는 `NormalizationError`를 사용하며 `CatalogError`를 만들지 않는다.
transaction collaborator가 반환한 Catalog error는 현재 HTTP status와 response 동작을 유지하며
Normalization infrastructure 경계에서 매핑한다. 의도적인 강화는 internal submit failure가
기존 HTTP status와 error code는 유지하되 opaque message를 반환하는 것이다. DB와 provider
detail을 caller에게 반환하지 않는다.

### Physical database namespace

이 capability 추출은 기존 PostgreSQL 테이블 이름을 바꾸지 않는다. 다음까지
separately approved physical-schema migration, Normalization infrastructure is
authorized to write only these legacy-namespace records:

- `catalog.normalization_proposal`
- `catalog.normalization_proposal_review`
- `catalog.normalization_application`

이는 Catalog가 소유한 `catalog.industrial_complex`와 `catalog.outbox_event` 변경을
Catalog transaction collaborator를 통해서만 수행한다는 뜻이다.
물리적으로 함께 배치해도 Catalog 기능 소유권이 생기지 않는다.

Active building-register unit override read도 Normalization 소유다. service worker는
Normalization infrastructure가 구현한 application read port로 application id와 opaque
`after_snapshot`을 얻으며 `catalog.normalization_application`을 직접 query하지 않는다.

### Compatibility

추출은 기존 HTTP path, request/response JSON 형태, 기존 Catalog OpenAPI, proposal key, status
wire value, PostgreSQL row, event byte, Silver/Parquet output, authorization requirement을
유지한다. 기존 결과의 validation·conflict message는 바꾸지 않는다. 새로 감지한 stale-state
conflict는 기존 HTTP 409 error shape를 사용하고 새 no-op mutation은 기존 invalid-input shape를
사용한다. internal submit failure는 status와 error code를 유지하되 message를 의도적으로
redact한다. Normalization route는 public transport DTO 기반의 별도 generated OpenAPI 문서를
가지며 static Catalog 문서에는 원래 표현되지 않았다. cutover 후 `catalog-*` 아래에
compatibility re-export를 남기지 않는다.

## 영향

### 긍정 효과

- package name에서 normalization behavior의 실제 owner가 드러난다.
- Catalog가 canonical entity와 command ownership으로 돌아간다.
- Lakehouse가 실제 owner의 deterministic rule을 소비한다.
- proposal governance가 Intelligence·HTTP·향후 event adapter를 위한 안정된 경계 하나를 가진다.
- Catalog persistence logic을 복제하지 않고 canonical apply가 atomic으로 남는다.

### 비용

- Normalization infrastructure가 잠시 legacy `catalog` PostgreSQL schema table에 쓴다.
- Foundation API composition root가 Catalog와 Normalization adapter를 주입한다.
- Rust ownership이 바뀌는 동안 정확한 compatibility test가 필요하다.

## 명시적 범위 밖

이 결정은 다음을 하지 않는다.

- PostgreSQL schema·table 이름을 바꾸거나 이동한다.
- normalization rule·accepted value·entity-resolution policy를 바꾼다.
- Qwen을 실행하거나 proposal을 자동 승인한다.
- Bronze·Silver·Gold·Parquet·Iceberg·R2·dbt contract를 바꾼다.
- Kafka·Kubernetes·Temporal·Dagster·다른 orchestrator를 추가한다.
- Spatial capability를 추출하거나 service deployable을 나눈다.
- public HTTP path·JSON field·permission·event name을 바꾼다.

## Verification

완료에는 다음이 필요하다.

1. no Normalization implementation or forwarding alias remains under `catalog-*`;
2. Catalog and Collection packages have no dependency on `normalization-*`;
3. Lakehouse imports deterministic rules only from `normalization-domain`;
4. apply and rollback failure-injection tests prove one-transaction behavior;
5. concurrency tests prove chain-based active state and conflict-safe compensation;
6. exact HTTP, generated Normalization OpenAPI, proposal-key, status, and Silver-output
   tests remain green;
7. focused, workspace, clippy, formatting, and supply-chain gates pass.

---

> 2026-07-20 개정 각주: crate rename 반영 — 본문의 `normalization-{domain,application,infrastructure}` 는
> 현재 `crates/normalization/foundation-normalization-{domain,application,infrastructure}` 이다
> (전역 유일 패키지명 규칙, 루트 ADR-0001). 결정 내용 자체는 변경 없음.
