# ADR 0021 - 수평 플랫폼 아키텍처 채택

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Governing ADR | `../../../../products/gongzzang/docs/adr/0048-horizontal-platform-redefinition.md` |

## 배경

shared data, identity, intelligence는 보안 경계·scaling pattern·release lifecycle이 서로
다르다. 하나의 platform에 두면 소유권이 흐려지고 직접 database coupling을 부른다.

## 결정

최종 아키텍처는 세 개의 horizontal platform으로 구성한다.

```text
foundation-platform
identity-platform
intelligence-platform
```

- `foundation-platform`은 Catalog, collection, lakehouse, canonical public/reference data,
  lineage, quality, normalization proposal governance를 소유한다.
- `identity-platform`은 staff identity, service identity, authentication policy,
  authorization을 소유한다.
- `intelligence-platform`은 model execution, proposal generation, vector/RAG processing을
  소유한다.

Gongzzang과 향후 product는 발행된 API·event·immutable artifact를 소비한다. platform 간
직접 database 접근과 compatibility alias는 금지한다.

## 이름 규칙

- system slug은 `foundation-platform`, `identity-platform`, `intelligence-platform`을 사용한다.
- brand 표기는 `Foundation Platform`, `Identity Platform`, `Intelligence Platform`을 사용한다.
- 새 database·bucket·service·package·environment variable·event에는 최종 이름만 사용한다.
- `/v1`, `.v1`, `schema_version: 1` 같은 contract version은 실제 public API·event·schema
  호환성 경계를 표현하는 경우 유지한다.

## 영향

- 각 platform을 독립적으로 deploy하고 scale할 수 있다.
- Identity data는 더 이상 Foundation에 저장하거나 foreign key로 연결하지 않는다.
- AI는 proposal producer로 남고 Foundation은 canonical decision authority로 남는다.
- 출시 전 migration helper와 compatibility 이름은 계속 운반하지 않고 제거한다.
