# ADR-0048: 수평 플랫폼 재정의

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Decision owner | perfectoryinc |
| Supersedes | ADR-0030/0031 core-centered platform naming, where it conflicts with this ADR |
| Related | ADR-0030, ADR-0031, ADR-0032, ADR-0034, ADR-0045, foundation implementation ADR-0021 |

## 배경

이전 저장소 간 모델은 하나의 shared core를 내부 hub로 사용해 Catalog,
Workforce/Authz, lakehouse, collection, governance를 묶었다. 이 모델은 product
service에서 공통 data를 추출하는 데 도움이 되었지만, 공통 capability가 점점 넓어지는
하나의 core service로 몰리는 수직 중력 우물을 만들었다.

목표 구조는 이제 horizontal platform architecture다. 공유 capability를 하나의
generic core 아래 묶지 않고, 오래 유지되는 platform 책임별로 묶는다.

## 결정

다음 top-level platform 이름과 소유 경계를 채택한다.

```text
foundation-platform
identity-platform
intelligence-platform
```

이전 core 이름은 플랫폼 이름으로 폐기한다. 역사적 migration evidence를 명시적으로
가리키는 경우를 제외하고 새 외부 resource·runtime label·contract·service·package·
architecture 문서에는 사용하지 않는다.

### foundation-platform 책임

`foundation-platform`은 canonical shared data와 data infrastructure를 소유한다:

- public/canonical Catalog data
- industrial complex, parcel, building, manufacturer, spatial, map anchor 사실
- Bronze/Silver/Gold lakehouse data
- R2/Iceberg/Trino/Spark lakehouse integration
- source catalog, raw lineage, collection ledger, Bronze commit protocol
- 소유 data의 canonical normalization proposal inbox
- 소유 data의 승인된 canonical apply command
- data governance, retention, lineage, promotion policy

현재 Catalog·lakehouse·collection·pipeline 책임은 foundation-platform으로 이동한다.

### identity-platform 책임

`identity-platform`은 shared identity, authorization, principal policy를 소유한다:

- staff identity
- service identity and service tokens
- session verification
- role/permission/policy model
- cross-service authorization contracts
- audit principal resolution
- identity-related outbox/events

현재 Workforce/Authz 책임은 identity-platform으로 이동한다.

Product-user identity는 명시적으로 이동하기 전까지 product가 소유한다. 예를 들어
Gongzzang B2C user는 계속 `gongzzang` 소유이고 staff/admin identity는
identity-platform 소유다.

### intelligence-platform 책임

`intelligence-platform`은 AI 실행과 proposal 생성을 소유한다:

- model calls and model routing
- embeddings/vector indexing and retrieval
- prompt/model/policy profiles
- normalization proposal generation
- validation/evaluation of AI-generated candidates
- AI retry/outbox state
- developer UI integrations such as Open WebUI, when used for development

`intelligence-platform`은 canonical data를 소유하지 않는다. 승인된 API를 통해
foundation-platform이나 다른 owner service에 proposal을 제출할 수 있다. owner DB,
Silver/Gold table, canonical record에 직접 쓰면 안 된다.

## 이름 규칙

새 platform-level resource는 최종 platform 이름을 사용한다:

```text
foundation-platform-*
identity-platform-*
intelligence-platform-*
```

Examples:

```text
foundation-platform-lakehouse-prod
foundation-platform-r2
foundation-platform.catalog.*
source_system = foundation-platform-r2
```

새 resource에는 legacy core prefix를 사용하지 않는다. 기존 resource는 migration input으로
필요한 동안만 남길 수 있고 legacy로 표시하며, 검증된 migration 후 폐기한다.

## 경계 규칙

데이터를 소유한 플랫폼이 해당 데이터의 승인 게이트를 소유한다.

```text
AI proposes.
소유 플랫폼이 관리한다.
필요하면 사람이 승인한다.
소유 플랫폼 명령이 정본 상태에 쓴다.
```

따라서:

- foundation-platform owns Catalog normalization proposal inboxes for foundation
  canonical data.
- identity-platform owns identity-policy approval gates.
- product services own product-specific gates such as listing moderation or
  site presentation approval.

이 규모에는 하나의 범용 승인 서비스가 필요하지 않다.

## 전환 전략

1. 이 ADR을 cross-repo decision source로 기록한다.
2. 영향을 받는 repo에 얇은 pointer ADR을 추가한다.
3. 현재 core repository/path는 이름을 바꾸거나 교체하기 전까지만 legacy 구현 위치로
   취급한다.
4. 문서, resource prefix, environment variable, runtime label을 건드릴 때 최종 이름으로
   바꾼다.
5. R2 bucket처럼 provider가 rename을 지원하지 않으면 기존 resource를 바꾸지 말고 최종
   이름으로 새 external resource를 만든다.
6. repository를 물리적으로 나누기 전에 identity 책임을 identity-platform contract 뒤로
   이동한다.
7. contract, CI, deployment 이름, data migration이 안정화된 뒤에만 물리 repository를
   이동하거나 이름을 바꾼다.

## 범위 밖

- 이 ADR에서 즉시 repository를 강제로 이동하지 않는다.
- platform 간 직접 database 공유를 하지 않는다.
- 이 naming 결정으로 Kafka/Kubernetes 의무를 추가하지 않는다.
- AI service에 canonical write permission을 주지 않는다.
- product-specific semantics를 foundation-platform으로 옮기지 않는다.

## 영향

- 긍정적 효과: shared capability가 하나의 수직 core umbrella 아래 쌓이지 않는다.
- 긍정적 효과: data, identity, AI 책임을 platform 수준에서 분리한다.
- 긍정적 효과: 향후 service가 과부하된 core service가 아니라 horizontal contract에
  의존할 수 있다.
- 비용: 기존 문서와 resource 이름을 신중하게 migration해야 한다.
- 비용: legacy 물리 path가 잠시 남을 수 있지만 유효한 platform 이름은 아니다.

## 재평가 조건

- repository 수나 platform 수가 늘어 Gongzzang이 cross-repo governance 위치로 더 이상
  적절하지 않으면 ADR-0045에 따라 전용 architecture/governance repository를 만든다.
- identity-platform을 독립 배포할 수 있게 되면 물리 추출 계획을 위한 repo-local ADR을
  만든다.
- foundation-platform의 물리 이름을 바꾸면 최종 path/resource migration evidence로 이
  ADR을 대체한다.
