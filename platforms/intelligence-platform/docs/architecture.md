---
status: current
owner: intelligence-platform
doc_type: documentation
last_reviewed: 2026-07-29
---

# Intelligence Platform 아키텍처

## 소유권

Intelligence Platform은 모델 실행, 정규화 제안 생성, 지식 검색, vector/RAG 처리, 영속 AI workflow
전달을 소유한다. 정본 부동산 데이터를 소유하지 않으며 Foundation DB에 직접 쓰지 않는다.

> Knowledge retrieval과 vector/RAG processing은 별도 설계와 ADR 승인 없이 도입하지 않습니다.
> 이 문서의 소유권 선언은 구현 완료를 뜻하지 않으며, 지원 capability의 SSOT는 코드와 공개
> API 계약입니다. ADR-0001 Consequences를 따릅니다.

## Rust 모듈 경계

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

- Domain crate는 business rule과 value type을 담는다.
- Application crate는 use case와 port를 담는다.
- Infrastructure crate는 HTTP·PostgreSQL·Kafka·model adapter를 구현한다(vector-store adapter는
  계획만 있고 아직 구현하지 않았다).
- `messaging-infrastructure`는 기술 전달 adapter다. message 계약은 `intelligence-contracts`와
  `schemas/`에 있으므로 domain/application 쌍을 의도적으로 두지 않는다.
- service는 모듈을 조합하고 runtime 경계를 노출한다.

## 플랫폼 간 계약

1. Foundation은 versioned contract로 불변 raw/canonical reference를 공개한다.
2. Intelligence는 evidence·confidence·lineage·idempotency key가 있는 정규화 제안을 만든다.
3. Intelligence는 Foundation API로 제안을 제출한다.
4. Foundation이 제안을 저장·검토하며 Intelligence는 승인·적용할 수 없다.
5. 승인된 제안을 정본 데이터에 적용하는 명령은 Foundation만 실행한다.

`*.v1.avsc` 이름의 Avro 파일은 구현 반복 번호가 아니라 실제 이벤트 호환성 계약이므로 유지한다.
호환되지 않는 이벤트 형태가 생기면 새 schema version을 부여한다.
