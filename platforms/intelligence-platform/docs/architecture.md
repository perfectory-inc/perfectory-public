---
status: current
owner: intelligence-platform
doc_type: documentation
last_reviewed: 2026-11-05
---

# Intelligence Platform 아키텍처

## 소유권

Intelligence Platform은 모델 실행, 정규화 제안 생성, 지식 검색, vector/RAG 처리, 영속 AI workflow
전달을 소유한다. 정본 부동산 데이터를 소유하지 않으며 Foundation DB에 직접 쓰지 않는다.

> 지식 검색은 Postgres 전문검색과 한국어 형태소 색인, 신호별 리트리버의 RRF 융합으로 구현되어
> 있다([ADR 0003](./adr/0003-korean-morphology-in-rust.md)·[0004](./adr/0004-retriever-fusion-over-a-single-scorer.md)·[0005](./adr/0005-result-assembly-diversity-and-context.md)).
> embedding provider·vector index와 운영 RAG endpoint는 [ADR 0002](./adr/0002-canonical-release-rag-design.md)의
> 승인 조건과 별도 ADR 없이 도입하지 않는다. 이 문서의 소유권 선언은 구현 완료를 뜻하지 않으며,
> 지원 capability의 SSOT는 코드와 공개 API 계약이다([ADR 0001](./adr/0001-canonical-implementation-rust.md) §영향).

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
|-- crates/intelligence-workspace-hack
|-- services/intelligence-api
`-- services/intelligence-worker
```

- Domain crate는 business rule과 value type을 담는다.
- Application crate는 use case와 port를 담는다.
- Infrastructure crate는 HTTP·PostgreSQL·Kafka·model adapter를 구현한다. 검색은 Postgres
  전문검색 어댑터(와 시험용 메모리 어댑터)뿐이며 vector-store adapter는 아직 구현하지 않았다.
- `messaging-infrastructure`는 기술 전달 adapter다. message 계약은 `intelligence-contracts`와
  `schemas/`에 있으므로 domain/application 쌍을 의도적으로 두지 않는다.
- `intelligence-workspace-hack`은 빌드 기능 통합용 crate이며 코드를 담지 않는다.
- service는 모듈을 조합하고 runtime 경계를 노출한다.

## 플랫폼 간 계약

1. Foundation은 versioned contract로 불변 raw/canonical reference를 공개한다.
2. Intelligence는 evidence·confidence·lineage·idempotency key가 있는 정규화 제안을 만든다.
3. Intelligence는 Foundation API로 제안을 제출한다.
4. Foundation이 제안을 저장·검토하며 Intelligence는 승인·적용할 수 없다.
5. 승인된 제안을 정본 데이터에 적용하는 명령은 Foundation만 실행한다.

`*.v1.avsc` 이름의 Avro 파일은 구현 반복 번호가 아니라 실제 이벤트 호환성 계약이므로 유지한다.
호환되지 않는 이벤트 형태가 생기면 새 schema version을 부여한다.
