# Intelligence Platform Architecture

## Ownership

Intelligence Platform owns model execution, normalization proposal generation, knowledge retrieval,
vector/RAG processing, and durable AI workflow delivery. It does not own canonical property data and
does not write directly to Foundation databases.

> Knowledge retrieval과 vector/RAG processing은 별도 설계와 ADR 승인 없이 도입하지 않습니다.
> 이 문서의 소유권 선언은 구현 완료를 뜻하지 않으며, 지원 capability의 SSOT는 코드와 공개
> API 계약입니다. ADR-0001 Consequences를 따릅니다.

## Rust Module Boundaries

```text
./
|-- crates/normalization/
|   |-- intelligence-normalization-domain
|   |-- intelligence-normalization-application
|   `-- intelligence-normalization-infrastructure
|-- crates/knowledge/
|   |-- knowledge-domain
|   |-- knowledge-application
|   `-- knowledge-infrastructure
|-- crates/messaging/
|   `-- messaging-infrastructure
|-- crates/intelligence-contracts
|-- services/intelligence-api
`-- services/intelligence-worker
```

- Domain crates contain business rules and value types.
- Application crates contain use cases and ports.
- Infrastructure crates implement HTTP, PostgreSQL, Kafka, and model adapters
  (vector-store adapters are planned, not yet implemented).
- `messaging-infrastructure` is a technical delivery adapter; message contracts live in
  `intelligence-contracts` and `schemas/`, so it intentionally has no domain/application pair.
- Services compose modules and expose runtime boundaries.

## Cross-Platform Contract

1. Foundation publishes immutable raw/canonical references through a versioned contract.
2. Intelligence creates a normalization proposal with evidence, confidence, lineage, and an
   idempotency key.
3. Intelligence submits the proposal to Foundation through the Foundation API.
4. Foundation stores and reviews the proposal. Intelligence cannot approve or apply it.
5. Only a Foundation command may apply an approved proposal to canonical data.

Avro files named `*.v1.avsc` are retained because they are real event compatibility contracts, not
implementation iteration labels. A future incompatible event shape receives a new schema version.
